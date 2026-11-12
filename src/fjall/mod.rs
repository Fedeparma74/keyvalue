//! [fjall](https://docs.rs/fjall)-backed key-value store.
//!
//! `fjall` is an LSM-tree-based embedded database for Rust. This module
//! provides [`FjallDB`], which maps the `keyvalue` table concept onto fjall's
//! *keyspaces*. Because fjall does not support dropping keyspaces at runtime,
//! a soft-deletion strategy is used: deleted keyspaces have their data erased
//! and their name recorded in an internal `_meta_deleted` keyspace.

use std::{
    collections::HashSet,
    io,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use fjall::{
    AbstractTree, CompressionType as FjallCompressionUpstream, KeyspaceCreateOptions, Readable,
    SingleWriterTxDatabase,
};

use crate::KeyValueDB;

fn lock_poisoned() -> io::Error {
    io::Error::other("RwLock poisoned")
}

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

#[cfg(feature = "stats")]
mod stats;

#[cfg(feature = "stats")]
pub use self::stats::{FjallStats, KeyspaceStats};

#[cfg(feature = "tokio")]
crate::impl_async_kvdb_via_spawn_blocking!(FjallDB);

/// Marker keyspace that tracks which user keyspaces have been logically deleted.
const META_DELETED_KEYSPACE: &str = "_meta_deleted";

/// Compression algorithm for fjall journal entries.
///
/// Journal entries larger than ~4 KiB are compressed with the chosen codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FjallCompression {
    /// No compression.
    None,
    /// LZ4 — fast with moderate compression ratio (default).
    Lz4,
}

impl From<FjallCompression> for FjallCompressionUpstream {
    fn from(c: FjallCompression) -> Self {
        match c {
            FjallCompression::None => FjallCompressionUpstream::None,
            FjallCompression::Lz4 => FjallCompressionUpstream::Lz4,
        }
    }
}

/// Configuration for a [`FjallDB`] instance.
///
/// Use [`Default::default()`] for sensible defaults tuned for multi-keyspace
/// workloads.  All sizes are in **bytes**.
///
/// # Examples
///
/// ```no_run
/// use keyvalue::fjall::{FjallDB, FjallConfig, FjallCompression};
/// use std::path::Path;
///
/// let config = FjallConfig::default()
///     .max_memtable_size(8 * 1024 * 1024)   // 8 MiB per keyspace
///     .cache_size(64 * 1024 * 1024)          // 64 MiB block cache
///     .journal_compression(FjallCompression::Lz4);
///
/// let db = FjallDB::open_with_config(Path::new("/tmp/mydb"), config).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct FjallConfig {
    // ── Per-keyspace options ─────────────────────────────────────
    /// Maximum memtable size **per keyspace** before rotation triggers a
    /// flush.  Lower values reduce peak memory at the cost of more frequent
    /// I/O.
    ///
    /// Default: **64 MiB** (fjall upstream default).
    pub max_memtable_size: u64,

    // ── Database-level options ───────────────────────────────────
    /// Block-cache capacity shared across all keyspaces.
    /// Recommended to set to 20–25 % of available RAM.
    ///
    /// Default: **32 MiB** (fjall upstream default).
    pub cache_size: u64,

    /// Maximum total size of all write-ahead journals before the oldest ones
    /// are evicted.  Similar to RocksDB `max_total_wal_size`.
    ///
    /// Must be ≥ 64 MiB.  Default: **512 MiB**.
    pub max_journaling_size: u64,

    /// Global cap on all active (unsealed) memtables across every keyspace.
    /// `None` disables the limit.
    ///
    /// Default: **None** (disabled).
    pub max_write_buffer_size: Option<u64>,

    /// Number of background worker threads for flushes and compaction.
    /// `None` uses upstream default: `min(available CPUs, 4)`.
    ///
    /// Default: **None** (auto).
    pub worker_threads: Option<usize>,

    /// Compression codec for large journal entries.
    ///
    /// Default: [`FjallCompression::Lz4`].
    pub journal_compression: FjallCompression,

    /// When `true`, the journal is **not** automatically flushed to disk
    /// after each write batch.  The caller must manage persistence
    /// (`Database::persist`) manually for batching.  When `false` (default),
    /// every committed write batch is flushed to at least OS page-cache
    /// buffers.
    ///
    /// **Durability:** Setting this to `true` trades durability for
    /// throughput — committed data may be lost on application crash unless
    /// the caller explicitly persists.
    ///
    /// Default: **false** (auto-persist after each write batch).
    pub manual_journal_persist: bool,

    /// Maximum number of file descriptors cached for open SST files.
    /// `None` uses the platform default (900 Linux, 400 Windows, 150 macOS).
    ///
    /// Must be ≥ 10 if set.  Default: **None** (platform default).
    pub max_cached_files: Option<usize>,

    /// When `true`, the database directory is deleted when the `FjallDB`
    /// handle is dropped.  Useful for temporary / test databases.
    ///
    /// Default: **false**.
    pub temporary: bool,
}

impl Default for FjallConfig {
    fn default() -> Self {
        Self {
            max_memtable_size: 64 * 1024 * 1024,
            cache_size: 32 * 1024 * 1024,
            max_journaling_size: 512 * 1024 * 1024,
            max_write_buffer_size: None,
            worker_threads: None,
            journal_compression: FjallCompression::Lz4,
            manual_journal_persist: false,
            max_cached_files: None,
            temporary: false,
        }
    }
}

impl FjallConfig {
    /// Sets the per-keyspace memtable size.
    #[must_use]
    pub fn max_memtable_size(mut self, bytes: u64) -> Self {
        self.max_memtable_size = bytes;
        self
    }

    /// Sets the shared block-cache capacity.
    #[must_use]
    pub fn cache_size(mut self, bytes: u64) -> Self {
        self.cache_size = bytes;
        self
    }

    /// Sets the maximum total journal size.
    #[must_use]
    pub fn max_journaling_size(mut self, bytes: u64) -> Self {
        self.max_journaling_size = bytes;
        self
    }

    /// Sets the global write-buffer cap.
    #[must_use]
    pub fn max_write_buffer_size(mut self, bytes: Option<u64>) -> Self {
        self.max_write_buffer_size = bytes;
        self
    }

    /// Sets the number of background worker threads.
    #[must_use]
    pub fn worker_threads(mut self, n: usize) -> Self {
        self.worker_threads = Some(n);
        self
    }

    /// Sets the journal compression codec.
    #[must_use]
    pub fn journal_compression(mut self, c: FjallCompression) -> Self {
        self.journal_compression = c;
        self
    }

    /// Enables or disables manual journal persistence.
    #[must_use]
    pub fn manual_journal_persist(mut self, flag: bool) -> Self {
        self.manual_journal_persist = flag;
        self
    }

    /// Sets the maximum cached file descriptors.
    #[must_use]
    pub fn max_cached_files(mut self, n: usize) -> Self {
        self.max_cached_files = Some(n);
        self
    }

    /// Marks the database as temporary (deleted on drop).
    #[must_use]
    pub fn temporary(mut self, flag: bool) -> Self {
        self.temporary = flag;
        self
    }
}

/// Whether fjall runs a database on one worker thread, from the count the
/// config asks for and the CPUs the process may use when the database is
/// opened (`None` when they cannot be read). fjall runs the count asked for;
/// with none, it runs one worker for each CPU up to a cap above one, and one
/// worker when the CPUs cannot be read.
fn runs_one_worker_thread(configured: Option<usize>, cpus: Option<NonZeroUsize>) -> bool {
    match configured {
        Some(workers) => workers == 1,
        None => cpus.is_none_or(|cpus| cpus.get() == 1),
    }
}

/// Key-value database backed by [fjall](https://docs.rs/fjall).
///
/// Each table maps to a fjall *keyspace*. Deletion is soft: all entries are
/// removed and the keyspace name is recorded in `_meta_deleted`. Re-inserting
/// into a deleted keyspace transparently un-deletes it.
#[derive(Clone)]
pub struct FjallDB {
    inner: Arc<RwLock<Option<Arc<SingleWriterTxDatabase>>>>,
    path: Arc<PathBuf>,
    config: Arc<FjallConfig>,
    deleted_tables: Arc<RwLock<HashSet<String>>>,
    max_memtable_size: u64,
    /// Whether fjall runs the open database on one worker thread, taken
    /// when the database is opened and again when it is re-opened.
    one_worker_thread: Arc<AtomicBool>,
    /// The live transactions, reported by [`FjallDB::stats`].
    #[cfg(all(feature = "stats", feature = "transactional"))]
    transactions: stats::TransactionRegistry,
}

/// RAII guard returned by [`FjallDB::inner`].
///
/// Derefs to `SingleWriterTxDatabase` so all call-sites remain unchanged.
/// Constructed only via [`FjallDB::inner`], which ensures the `Option` is
/// `Some` before handing out the guard.
pub(super) struct FjallDBGuard<'a>(
    std::sync::RwLockReadGuard<'a, Option<Arc<SingleWriterTxDatabase>>>,
);

impl FjallDBGuard<'_> {
    /// Clones the shared handle out of the guard.
    ///
    /// Every clone keeps fjall's directory lock held, so a transaction or a
    /// compaction that holds one blocks [`FjallDB::try_recover_from_poison`]
    /// from re-opening the path. That is why recovery counts the
    /// outstanding clones instead of assuming the store is free.
    pub(super) fn handle(&self) -> Arc<SingleWriterTxDatabase> {
        Arc::clone(
            self.0
                .as_ref()
                .expect("FjallDB inner is None during recovery"),
        )
    }
}

impl<'a> std::ops::Deref for FjallDBGuard<'a> {
    type Target = SingleWriterTxDatabase;
    fn deref(&self) -> &Self::Target {
        // SAFETY: `inner()` only constructs this guard after verifying `is_some()`.
        self.0
            .as_ref()
            .expect("FjallDB inner is None during recovery")
    }
}

impl FjallDB {
    /// Opens (or creates) a fjall database at the given filesystem `path`
    /// using [`FjallConfig::default()`].
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_config(path, FjallConfig::default())
    }

    /// Opens (or creates) a fjall database at `path` with custom
    /// [`FjallConfig`].
    pub fn open_with_config(path: &Path, config: FjallConfig) -> io::Result<Self> {
        let (database, one_worker_thread) = Self::build_database(path, &config)?;
        let inner = Arc::new(database);
        let deleted = Arc::new(RwLock::new(HashSet::new()));

        // Load persisted deleted tables
        Self::load_deleted_tables(&inner, &deleted)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(Some(inner))),
            path: Arc::new(path.to_path_buf()),
            config: Arc::new(config.clone()),
            deleted_tables: deleted,
            max_memtable_size: config.max_memtable_size,
            one_worker_thread: Arc::new(AtomicBool::new(one_worker_thread)),
            #[cfg(all(feature = "stats", feature = "transactional"))]
            transactions: stats::TransactionRegistry::default(),
        })
    }

    /// Builds a new `SingleWriterTxDatabase` from the given path and config,
    /// with whether fjall runs it on one worker thread. The CPUs are read
    /// where fjall reads them for its count: when the builder is made.
    fn build_database(
        path: &Path,
        config: &FjallConfig,
    ) -> io::Result<(SingleWriterTxDatabase, bool)> {
        let builder = SingleWriterTxDatabase::builder(path);
        let one_worker_thread = runs_one_worker_thread(
            config.worker_threads,
            std::thread::available_parallelism().ok(),
        );
        let mut builder = builder
            .cache_size(config.cache_size)
            .max_journaling_size(config.max_journaling_size)
            .journal_compression(config.journal_compression.into())
            .manual_journal_persist(config.manual_journal_persist);

        #[allow(deprecated)]
        {
            builder = builder.max_write_buffer_size(config.max_write_buffer_size);
        }

        let mut builder = builder.temporary(config.temporary);

        if let Some(n) = config.worker_threads {
            builder = builder.worker_threads(n);
        }
        if let Some(n) = config.max_cached_files {
            builder = builder.max_cached_files(Some(n));
        }

        let database = builder.open().map_err(io::Error::other)?;
        Ok((database, one_worker_thread))
    }

    /// Loads persisted deleted-table markers from the internal `_meta_deleted`
    /// keyspace into an already-locked `HashSet`.
    ///
    /// Used both at open-time and during `try_recover_from_poison`.
    fn load_deleted_tables_into(
        inner: &SingleWriterTxDatabase,
        deleted: &mut HashSet<String>,
    ) -> io::Result<()> {
        if inner.keyspace_exists(META_DELETED_KEYSPACE) {
            let meta_ks = inner
                .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
                .map_err(io::Error::other)?;

            let snapshot = inner.read_tx();
            for item in snapshot.iter(&meta_ks) {
                let (key_bytes, _) = item.into_inner().map_err(io::Error::other)?;
                let table_name = String::from_utf8(key_bytes.to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                deleted.insert(table_name);
            }
        }
        Ok(())
    }

    /// Loads persisted deleted-table markers from the internal `_meta_deleted` keyspace.
    fn load_deleted_tables(
        inner: &SingleWriterTxDatabase,
        deleted: &Arc<RwLock<HashSet<String>>>,
    ) -> io::Result<()> {
        let mut guard = deleted.write().map_err(|_| lock_poisoned())?;
        Self::load_deleted_tables_into(inner, &mut guard)
    }

    /// Attempts to recover from a fjall database-level error state by
    /// re-opening the underlying database.
    ///
    /// When fjall encounters a hardware-level I/O failure (e.g. disk full)
    /// during a flush or commit, it marks the database instance as *poisoned*
    /// and refuses all future writes.  Re-opening the path is the only way
    /// back, and re-opening requires this process to be holding no other
    /// handle: fjall guards the directory with an advisory file lock owned by
    /// an `Arc`, so the lock is released only when the LAST handle drops, and
    /// a second `open()` against a path this process still holds fails with
    /// `Error::Locked`.
    ///
    /// Every live transaction owns a handle clone, and so does a compaction
    /// while it runs, so recovery is only possible while none is
    /// outstanding.  This method therefore checks first and **refuses
    /// without touching the store** when it cannot succeed — dropping the
    /// handle to find out would leave the database absent for the rest of
    /// the process's life, since the dropped handle cannot be rebuilt
    /// without the very `open()` that is failing.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::WouldBlock`] when transactions or a compaction are
    /// still live: the store is untouched and the caller may try again once
    /// they are over.
    /// Any other error means the re-open itself failed (e.g. the underlying
    /// hardware issue has not been resolved yet).
    pub fn try_recover_from_poison(&self) -> io::Result<()> {
        // Acquire write lock; also recover from any RwLock poison caused by
        // a panicking thread that held the lock.
        let mut inner_guard = match self.inner.write() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        let mut deleted_guard = match self.deleted_tables.write() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };

        // Step 1: refuse while any transaction or compaction still holds a
        // handle clone. The write lock above stops new ones being handed
        // out, so this count cannot grow while the check and the teardown
        // run.
        if let Some(db) = inner_guard.as_ref() {
            let outstanding = Arc::strong_count(db) - 1;
            if outstanding > 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "cannot re-open the fjall database while {outstanding} transaction(s) \
                         or compaction(s) still hold it; retry once they are over"
                    ),
                ));
            }
        }

        // Step 2: drop the old (possibly poisoned) instance so fjall releases
        // its exclusive file lock on the database directory.
        *inner_guard = None;

        // Step 3: re-open the database.  This will fail if the underlying
        // issue (e.g. disk full) has not been resolved.
        let (new_db, one_worker_thread) = Self::build_database(&self.path, &self.config)?;
        let new_db = Arc::new(new_db);

        // Step 4: reload deleted-table metadata from the fresh database.
        deleted_guard.clear();
        Self::load_deleted_tables_into(&new_db, &mut deleted_guard)?;

        // Step 5: install the new database, with whether it runs one worker
        // thread.
        self.one_worker_thread
            .store(one_worker_thread, Ordering::Release);
        *inner_guard = Some(new_db);

        Ok(())
    }

    /// Collects runtime statistics of the database and each of its keyspaces.
    ///
    /// Purely observational: it opens no snapshot or transaction, creates no
    /// keyspace and triggers no flush, compaction or garbage collection, so
    /// reading the figures does not move them.
    #[cfg(feature = "stats")]
    pub fn stats(&self) -> io::Result<FjallStats> {
        stats::collect(self)
    }

    /// Acquires a read lock on the inner database.
    ///
    /// Returns an error if the database is currently being recovered
    /// (i.e. a `try_recover_from_poison` call is in progress).
    fn inner(&self) -> io::Result<FjallDBGuard<'_>> {
        let guard = self.inner.read().map_err(|_| lock_poisoned())?;
        if guard.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "fjall database is being recovered; retry after try_recover_from_poison completes",
            ));
        }
        Ok(FjallDBGuard(guard))
    }

    /// Returns [`KeyspaceCreateOptions`] using the configured memtable size.
    fn ks_options(&self) -> KeyspaceCreateOptions {
        KeyspaceCreateOptions::default().max_memtable_size(self.max_memtable_size)
    }
}

impl KeyValueDB for FjallDB {
    fn insert(&self, table: &str, key: &str, value: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot insert into meta deleted keyspace",
            ));
        }

        let inner = self.inner()?;

        // Fast-path: if the table is not soft-deleted, no cross-cutting
        // topology change is involved and the fjall single-writer lock is
        // sufficient to serialize the mutation.
        let was_deleted = self
            .deleted_tables
            .read()
            .map_err(|_| lock_poisoned())?
            .contains(table);

        if !was_deleted {
            let mut write_tx = inner.write_tx();
            let ks = inner
                .keyspace(table, || self.ks_options())
                .map_err(io::Error::other)?;
            let key_bytes = key.as_bytes();
            let old = write_tx
                .get(&ks, key_bytes)
                .map_err(io::Error::other)?
                .map(|v| v.to_vec());
            write_tx.insert(&ks, key_bytes, value);
            write_tx.commit().map_err(io::Error::other)?;
            return Ok(old);
        }

        // Slow-path: the table is soft-deleted and must be transparently
        // un-deleted.  Hold the `deleted_tables` write lock from the disk
        // commit through the in-memory update so that concurrent readers
        // never observe the intermediate state (on-disk un-deleted but
        // in-memory still marked deleted).
        let mut deleted_guard = self.deleted_tables.write().map_err(|_| lock_poisoned())?;
        // Re-check: another writer may have undeleted it between the read
        // lock release and the write lock acquire.
        if !deleted_guard.contains(table) {
            drop(deleted_guard);
            return self.insert(table, key, value);
        }

        let mut write_tx = inner.write_tx();
        let meta_ks = inner
            .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;
        write_tx.remove(&meta_ks, table.as_bytes());

        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;
        let key_bytes = key.as_bytes();
        let old = write_tx
            .get(&ks, key_bytes)
            .map_err(io::Error::other)?
            .map(|v| v.to_vec());
        write_tx.insert(&ks, key_bytes, value);
        write_tx.commit().map_err(io::Error::other)?;

        deleted_guard.remove(table);
        Ok(old)
    }
    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(None);
        }

        {
            let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
            if deleted_tables.contains(table) {
                return Ok(None);
            }
        }

        let inner = self.inner()?;

        if !inner.keyspace_exists(table) {
            return Ok(None);
        }

        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;

        Ok(ks
            .get(key.as_bytes())
            .map_err(io::Error::other)?
            .map(|v| v.to_vec()))
    }

    fn remove(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(None);
        }

        {
            let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
            if deleted_tables.contains(table) {
                return Ok(None);
            }
        }

        let inner = self.inner()?;

        if !inner.keyspace_exists(table) {
            return Ok(None);
        }

        let mut write_tx = inner.write_tx();

        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;

        let key_bytes = key.as_bytes();
        let old = write_tx
            .get(&ks, key_bytes)
            .map_err(io::Error::other)?
            .map(|v| v.to_vec());
        write_tx.remove(&ks, key_bytes);
        write_tx.commit().map_err(io::Error::other)?;

        Ok(old)
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }

        {
            let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
            if deleted_tables.contains(table) {
                return Ok(Vec::new());
            }
        }

        let inner = self.inner()?;

        if !inner.keyspace_exists(table) {
            return Ok(Vec::new());
        }

        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;

        let mut result = Vec::new();
        let snapshot = inner.read_tx();
        for item in snapshot.iter(&ks) {
            let (k, v) = item.into_inner().map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let inner = self.inner()?;
        // Exclude internal meta and deleted tables
        let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
        let names = inner
            .list_keyspace_names()
            .iter()
            .filter_map(|n| {
                if n.as_ref() != META_DELETED_KEYSPACE && !deleted_tables.contains(n.as_ref()) {
                    Some(n.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        Ok(names)
    }

    fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }

        {
            let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
            if deleted_tables.contains(table) {
                return Ok(Vec::new());
            }
        }

        let inner = self.inner()?;

        if !inner.keyspace_exists(table) {
            return Ok(Vec::new());
        }

        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;

        let mut result = Vec::new();
        let snapshot = inner.read_tx();
        for item in snapshot.prefix(&ks, prefix.as_bytes()) {
            let (k, v) = item.into_inner().map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn iter_range(
        &self,
        table: &str,
        range: crate::KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        use core::ops::Bound as OB;
        use fjall::Readable;

        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }
        {
            let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
            if deleted_tables.contains(table) {
                return Ok(Vec::new());
            }
        }
        let inner = self.inner()?;
        if !inner.keyspace_exists(table) {
            return Ok(Vec::new());
        }
        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;

        // Translate our Bound -> std::ops::Bound<Vec<u8>>.
        let lower = match &range.lower {
            crate::Bound::Unbounded => OB::Unbounded,
            crate::Bound::Included(k) => OB::Included(k.as_bytes().to_vec()),
            crate::Bound::Excluded(k) => OB::Excluded(k.as_bytes().to_vec()),
        };
        let upper = match &range.upper {
            crate::Bound::Unbounded => OB::Unbounded,
            crate::Bound::Included(k) => OB::Included(k.as_bytes().to_vec()),
            crate::Bound::Excluded(k) => OB::Excluded(k.as_bytes().to_vec()),
        };

        let snapshot = inner.read_tx();
        let iter = snapshot.range(&ks, (lower, upper));
        let limit = range.limit.unwrap_or(usize::MAX);
        let mut result = Vec::new();

        macro_rules! process {
            ($it:expr) => {
                for item in $it {
                    let (k, v) = item.into_inner().map_err(io::Error::other)?;
                    let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
                    if range.is_beyond_far_end(&k_str) {
                        break;
                    }
                    if !range.matches_prefix(&k_str) {
                        continue;
                    }
                    result.push((k_str, v.to_vec()));
                    if result.len() >= limit {
                        break;
                    }
                }
            };
        }

        match range.direction {
            crate::Direction::Forward => process!(iter),
            crate::Direction::Reverse => process!(iter.rev()),
        }

        Ok(result)
    }

    fn keys_range(&self, table: &str, range: crate::KeyRange) -> Result<Vec<String>, io::Error> {
        use core::ops::Bound as OB;
        use fjall::Readable;

        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }
        {
            let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
            if deleted_tables.contains(table) {
                return Ok(Vec::new());
            }
        }
        let inner = self.inner()?;
        if !inner.keyspace_exists(table) {
            return Ok(Vec::new());
        }
        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;

        let lower = match &range.lower {
            crate::Bound::Unbounded => OB::Unbounded,
            crate::Bound::Included(k) => OB::Included(k.as_bytes().to_vec()),
            crate::Bound::Excluded(k) => OB::Excluded(k.as_bytes().to_vec()),
        };
        let upper = match &range.upper {
            crate::Bound::Unbounded => OB::Unbounded,
            crate::Bound::Included(k) => OB::Included(k.as_bytes().to_vec()),
            crate::Bound::Excluded(k) => OB::Excluded(k.as_bytes().to_vec()),
        };

        let snapshot = inner.read_tx();
        let iter = snapshot.range(&ks, (lower, upper));
        let limit = range.limit.unwrap_or(usize::MAX);
        let mut result: Vec<String> = Vec::new();

        macro_rules! process {
            ($it:expr) => {
                for item in $it {
                    // `Guard::key` skips the value blob under key-value
                    // separation.
                    let k = item.key().map_err(io::Error::other)?;
                    let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
                    if range.is_beyond_far_end(&k_str) {
                        break;
                    }
                    if !range.matches_prefix(&k_str) {
                        continue;
                    }
                    result.push(k_str);
                    if result.len() >= limit {
                        break;
                    }
                }
            };
        }

        match range.direction {
            crate::Direction::Forward => process!(iter),
            crate::Direction::Reverse => process!(iter.rev()),
        }

        Ok(result)
    }

    fn contains_table(&self, table: &str) -> Result<bool, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(false);
        }

        let deleted_tables = self.deleted_tables.read().map_err(|_| lock_poisoned())?;
        if deleted_tables.contains(table) {
            return Ok(false);
        }
        let inner = self.inner()?;
        Ok(inner.keyspace_exists(table))
    }

    fn delete_table(&self, table: &str) -> Result<(), io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete meta deleted keyspace",
            ));
        }

        let inner = self.inner()?;

        // Hold the `deleted_tables` write lock from the persistent commit
        // through the in-memory update so readers never observe an
        // inconsistent "on-disk empty, in-memory not yet deleted" state.
        let mut deleted_guard = self.deleted_tables.write().map_err(|_| lock_poisoned())?;
        if deleted_guard.contains(table) {
            return Ok(());
        }
        if !inner.keyspace_exists(table) {
            return Ok(());
        }

        let mut write_tx = inner.write_tx();
        let ks = inner
            .keyspace(table, || self.ks_options())
            .map_err(io::Error::other)?;
        for item in write_tx.iter(&ks) {
            let (key_bytes, _) = item.into_inner().map_err(io::Error::other)?;
            write_tx.remove(&ks, key_bytes);
        }

        let meta_ks = inner
            .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;
        write_tx.insert(&meta_ks, table.as_bytes(), []);
        write_tx.commit().map_err(io::Error::other)?;

        deleted_guard.insert(table.to_string());
        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        let inner = self.inner()?;

        // Hold the `deleted_tables` write lock for the full critical section.
        let mut deleted_guard = self.deleted_tables.write().map_err(|_| lock_poisoned())?;

        let current_tables: Vec<String> = inner
            .list_keyspace_names()
            .iter()
            .filter_map(|n| {
                if n.as_ref() != META_DELETED_KEYSPACE && !deleted_guard.contains(n.as_ref()) {
                    Some(n.to_string())
                } else {
                    None
                }
            })
            .collect();

        let meta_ks = inner
            .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;
        let mut write_tx = inner.write_tx();

        for table_name in &current_tables {
            let ks = inner
                .keyspace(table_name, || self.ks_options())
                .map_err(io::Error::other)?;
            for item in write_tx.iter(&ks) {
                let (key_bytes, _) = item.into_inner().map_err(io::Error::other)?;
                write_tx.remove(&ks, key_bytes);
            }
            write_tx.insert(&meta_ks, table_name.as_bytes(), []);
        }

        write_tx.commit().map_err(io::Error::other)?;

        for table_name in current_tables {
            deleted_guard.insert(table_name);
        }
        Ok(())
    }

    /// Seals every keyspace's active memtable, waits until fjall has written
    /// every sealed memtable to disk, then seals the journal and syncs it.
    /// Recovery replays the active journal in full but skips the entries of a
    /// sealed journal that each keyspace has persisted, so a reopen loads
    /// nothing back into memory.
    ///
    /// fjall reports no completion of a flush, so this checks the sealed
    /// memtable counts every 10 ms, as fjall's own `rotate_memtable_and_wait`
    /// does. A flush that fails poisons the database and never completes, so
    /// the wait returns an error once the database is poisoned.
    fn flush(&self) -> Result<(), io::Error> {
        let inner = self.inner()?;
        let mut keyspaces = Vec::new();
        for name in inner.list_keyspace_names() {
            let keyspace = inner
                .keyspace(name.as_ref(), || self.ks_options())
                .map_err(io::Error::other)?;
            keyspace
                .inner()
                .rotate_memtable()
                .map_err(io::Error::other)?;
            keyspaces.push(keyspace);
        }
        while keyspaces
            .iter()
            .any(|keyspace| keyspace.inner().sealed_memtable_count() > 0)
        {
            if inner.inner().is_poisoned() {
                return Err(io::Error::other(fjall::Error::Poisoned));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        inner.inner().seal_journal().map_err(io::Error::other)?;
        inner
            .persist(fjall::PersistMode::SyncAll)
            .map_err(io::Error::other)
    }

    /// Runs a major compaction on every keyspace, which merges its tables
    /// into its last level and leaves out the removed entries and replaced
    /// values they hold, then seals every memtable. It does not wait for
    /// fjall to write the sealed memtables out: what they hold reaches
    /// tables in the background, and a later call merges it.
    ///
    /// Sealing a memtable is also what makes fjall delete the files a
    /// compaction replaced: at least those of every compaction but the one
    /// that finished last. So the keyspaces are compacted from the largest
    /// on disk to the smallest, and the files left for the next call are
    /// the smallest keyspace's.
    ///
    /// When no memtable holds anything there is nothing to seal, and the
    /// files a compaction replaced would stay, one more set at every call.
    /// The call then does nothing.
    ///
    /// The handle is cloned for the duration, as a transaction's is, so
    /// [`FjallDB::try_recover_from_poison`] refuses instead of closing a
    /// database that is being compacted.
    ///
    /// On a database fjall runs with one worker thread, as taken when the
    /// database was opened, the call does nothing. When a memtable of the
    /// keyspace being merged is written out, fjall queues a compaction of
    /// that keyspace, which waits for the merge to end. With one worker that
    /// wait holds the only thread that rotates full memtables and writes
    /// them out, so no memtable of the database would be written out until
    /// the merge ended, and writes would pile up in memory. With more, fjall
    /// keeps one worker for that. The answer is read once the handle is
    /// held, when recovery can no longer re-open the database under it.
    fn compact(&self) -> Result<(), io::Error> {
        let db = self.inner()?.handle();
        if self.one_worker_thread.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut keyspaces = Vec::new();
        for name in db.list_keyspace_names() {
            let keyspace = db
                .keyspace(name.as_ref(), || self.ks_options())
                .map_err(io::Error::other)?;
            keyspaces.push(keyspace);
        }
        if keyspaces
            .iter()
            .all(|keyspace| keyspace.inner().tree.active_memtable().is_empty())
        {
            return Ok(());
        }
        keyspaces.sort_by_key(|keyspace| std::cmp::Reverse(keyspace.inner().disk_space()));
        for keyspace in &keyspaces {
            keyspace.inner().major_compact().map_err(io::Error::other)?;
        }
        for keyspace in &keyspaces {
            keyspace
                .inner()
                .rotate_memtable()
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod flush_tests {
    use super::*;

    const TABLES: [&str; 3] = ["alpha", "beta", "gamma"];
    const ROWS: usize = 200;

    fn write_rows(db: &FjallDB) {
        for table in TABLES {
            for i in 0..ROWS {
                db.insert(table, &format!("key-{i:04}"), &[7u8; 64])
                    .expect("insert");
            }
        }
    }

    /// Bytes a fresh open of `path` holds in memtables before any write:
    /// what recovery loaded back from the journal.
    fn replayed_bytes(path: &Path) -> u64 {
        let db = FjallDB::open(path).expect("reopen");
        db.inner().expect("inner").write_buffer_size()
    }

    #[test]
    fn a_reopen_without_flush_replays_the_journal() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let db = FjallDB::open(dir.path()).expect("open");
            write_rows(&db);
        }
        assert!(replayed_bytes(dir.path()) > 0);
    }

    #[test]
    fn a_reopen_after_flush_replays_nothing_and_keeps_every_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let db = FjallDB::open(dir.path()).expect("open");
            write_rows(&db);
            KeyValueDB::flush(&db).expect("flush");
        }
        assert_eq!(replayed_bytes(dir.path()), 0);

        let db = FjallDB::open(dir.path()).expect("reopen");
        for table in TABLES {
            assert_eq!(db.keys(table).expect("keys").len(), ROWS);
        }
    }

    #[test]
    fn flushing_an_empty_database_succeeds_and_repeats() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = FjallDB::open(dir.path()).expect("open");
        KeyValueDB::flush(&db).expect("flush");
        KeyValueDB::flush(&db).expect("second flush");
    }
}

#[cfg(test)]
mod compact_tests {
    use std::sync::{Mutex, OnceLock, mpsc};
    use std::thread::ThreadId;

    use fjall::compaction::filter::{
        CompactionFilter, CompactionFilterResult, Context, Factory, ItemAccessor, Verdict,
    };

    use super::*;

    const TABLE: &str = "alpha";
    const ROWS: usize = 100;

    fn key(i: usize) -> String {
        format!("key-{i:04}")
    }

    /// Writes [`ROWS`] rows to [`TABLE`] and to disk, then removes one,
    /// which the table's memtable holds.
    fn store_rows_and_remove_one(db: &FjallDB) {
        for i in 0..ROWS {
            KeyValueDB::insert(db, TABLE, &key(i), &[7u8; 64]).expect("insert");
        }
        KeyValueDB::flush(db).expect("flush");
        KeyValueDB::remove(db, TABLE, &key(0)).expect("remove");
    }

    /// Whether the memtable of [`TABLE`] holds nothing, as after a seal.
    fn memtable_is_empty(db: &FjallDB) -> bool {
        let inner = db.inner().expect("inner");
        let keyspace = inner.keyspace(TABLE, || db.ks_options()).expect("keyspace");
        keyspace.inner().tree.active_memtable().is_empty()
    }

    /// `compact` on a database opened with `config` and holding a removal in
    /// memory: whether the call sealed the memtable.
    fn compact_seals(config: FjallConfig) -> bool {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = FjallDB::open_with_config(dir.path(), config).expect("open");
        store_rows_and_remove_one(&db);
        KeyValueDB::compact(&db).expect("compact");
        assert_eq!(KeyValueDB::keys(&db, TABLE).expect("keys").len(), ROWS - 1);
        memtable_is_empty(&db)
    }

    /// With one worker thread configured the call does nothing: the removal
    /// stays in memory. With two it compacts and seals the memtable.
    #[test]
    fn compact_does_nothing_on_a_database_run_with_one_worker_thread() {
        assert!(!compact_seals(FjallConfig::default().worker_threads(1)));
        assert!(compact_seals(FjallConfig::default().worker_threads(2)));
    }

    /// With the count left to fjall, the call does nothing exactly when the
    /// process may use one CPU, where fjall runs one worker. Under
    /// `taskset -c 0` this is the one-CPU case.
    #[test]
    fn compact_with_the_worker_count_left_to_fjall_follows_the_cpus_it_may_use() {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        assert_eq!(
            compact_seals(FjallConfig::default()),
            cpus > 1,
            "{cpus} CPUs"
        );
    }

    /// One worker thread when the config asks for one, or leaves the count
    /// to fjall where the process may use one CPU or its CPUs cannot be read;
    /// more than one for any other configured count, and for a count left
    /// to fjall where the process may use more than one CPU.
    #[test]
    fn one_worker_thread_follows_the_configured_count_or_else_the_cpus() {
        let cpus = NonZeroUsize::new;
        assert!(runs_one_worker_thread(Some(1), cpus(32)));
        assert!(runs_one_worker_thread(Some(1), None));
        assert!(!runs_one_worker_thread(Some(2), cpus(1)));
        assert!(!runs_one_worker_thread(Some(4), None));
        assert!(runs_one_worker_thread(None, cpus(1)));
        assert!(runs_one_worker_thread(None, None));
        assert!(!runs_one_worker_thread(None, cpus(2)));
        assert!(!runs_one_worker_thread(None, cpus(32)));
    }

    /// `try_recover_from_poison` takes the answer again for the database it
    /// opens, in place of the one recorded before.
    #[test]
    fn recovery_takes_again_whether_the_database_runs_one_worker_thread() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = FjallDB::open_with_config(dir.path(), FjallConfig::default().worker_threads(2))
            .expect("open");
        db.one_worker_thread.store(true, Ordering::SeqCst);

        db.try_recover_from_poison().expect("recovery");

        store_rows_and_remove_one(&db);
        KeyValueDB::compact(&db).expect("compact");
        assert!(
            memtable_is_empty(&db),
            "a database run with two workers was not compacted after recovery"
        );
    }

    /// What the compaction under test reports to the test.
    enum Event {
        /// It filters its first item, and waits to be released.
        Holding,
        /// It returned.
        Returned(io::Result<()>),
    }

    /// Holds the first item that a compaction running on the thread
    /// `holds_on` filters, until the test releases it.
    struct HoldFirstItem {
        holds_on: OnceLock<ThreadId>,
        held: AtomicBool,
        events: Mutex<mpsc::Sender<Event>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    struct HoldingFactory(Arc<HoldFirstItem>);

    impl Factory for HoldingFactory {
        fn name(&self) -> &str {
            "hold-first-item"
        }

        fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
            Box::new(HoldingFilter(self.0.clone()))
        }
    }

    struct HoldingFilter(Arc<HoldFirstItem>);

    impl CompactionFilter for HoldingFilter {
        fn filter_item(
            &mut self,
            _item: ItemAccessor<'_>,
            _ctx: &Context,
        ) -> CompactionFilterResult {
            let hold = &self.0;
            if hold.holds_on.get() == Some(&std::thread::current().id())
                && !hold.held.swap(true, Ordering::SeqCst)
            {
                hold.events.lock().unwrap().send(Event::Holding).unwrap();
                hold.release.lock().unwrap().recv().unwrap();
            }
            Ok(Verdict::Keep)
        }
    }

    /// Opens a database at `path`, with two workers, whose [`TABLE`] is
    /// compacted through `hold`.
    fn open_holding(path: &Path, hold: &Arc<HoldFirstItem>) -> FjallDB {
        let config = FjallConfig::default().worker_threads(2);
        let factory: Arc<dyn Factory> = Arc::new(HoldingFactory(hold.clone()));
        let database = SingleWriterTxDatabase::builder(path)
            .worker_threads(2)
            .with_compaction_filter_factories(Arc::new(move |table: &str| {
                (table == TABLE).then(|| factory.clone())
            }))
            .open()
            .expect("open");
        FjallDB {
            inner: Arc::new(RwLock::new(Some(Arc::new(database)))),
            path: Arc::new(path.to_path_buf()),
            max_memtable_size: config.max_memtable_size,
            config: Arc::new(config),
            deleted_tables: Arc::new(RwLock::new(HashSet::new())),
            one_worker_thread: Arc::new(AtomicBool::new(false)),
            #[cfg(all(feature = "stats", feature = "transactional"))]
            transactions: stats::TransactionRegistry::default(),
        }
    }

    /// `try_recover_from_poison` called while a compaction runs refuses with
    /// `WouldBlock` at once. The compaction holds a handle of the database,
    /// not its lock, so recovery neither waits for it nor closes the
    /// database under it; once it has returned, recovery goes through.
    #[test]
    fn recovery_during_a_compaction_refuses_at_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (events, events_received) = mpsc::channel();
        let (release, release_received) = mpsc::channel();
        let hold = Arc::new(HoldFirstItem {
            holds_on: OnceLock::new(),
            held: AtomicBool::new(false),
            events: Mutex::new(events.clone()),
            release: Mutex::new(release_received),
        });
        let db = open_holding(dir.path(), &hold);
        store_rows_and_remove_one(&db);

        let (lock_free, recovery) = std::thread::scope(|scope| {
            scope.spawn(|| {
                hold.holds_on.set(std::thread::current().id()).unwrap();
                let result = KeyValueDB::compact(&db);
                events.send(Event::Returned(result)).unwrap();
            });
            match events_received.recv().unwrap() {
                Event::Holding => {}
                Event::Returned(result) => {
                    panic!("the compaction returned {result:?} without filtering an item")
                }
            }
            // Recovery takes the store's lock first: a compaction holding it
            // would make recovery wait for it.
            let lock_free = db.inner.try_write().is_ok();
            let recovery = lock_free.then(|| db.try_recover_from_poison());
            release.send(()).unwrap();
            (lock_free, recovery)
        });

        assert!(
            lock_free,
            "the compaction holds the store's lock: recovery would wait for it"
        );
        let error = recovery
            .unwrap()
            .expect_err("recovery re-opened the database during a compaction");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        match events_received.recv().unwrap() {
            Event::Returned(result) => result.expect("compact"),
            Event::Holding => panic!("the compaction held a second item"),
        }
        db.try_recover_from_poison()
            .expect("recovery once the compaction has returned");
        assert_eq!(KeyValueDB::keys(&db, TABLE).expect("keys").len(), ROWS - 1);
    }
}
