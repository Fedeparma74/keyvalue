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
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use fjall::{
    CompressionType as FjallCompressionUpstream, KeyspaceCreateOptions, Readable,
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

/// Key-value database backed by [fjall](https://docs.rs/fjall).
///
/// Each table maps to a fjall *keyspace*. Deletion is soft: all entries are
/// removed and the keyspace name is recorded in `_meta_deleted`. Re-inserting
/// into a deleted keyspace transparently un-deletes it.
#[derive(Clone)]
pub struct FjallDB {
    inner: Arc<RwLock<Option<SingleWriterTxDatabase>>>,
    path: Arc<PathBuf>,
    config: Arc<FjallConfig>,
    deleted_tables: Arc<RwLock<HashSet<String>>>,
    max_memtable_size: u64,
}

/// RAII guard returned by [`FjallDB::inner`].
///
/// Derefs to `SingleWriterTxDatabase` so all call-sites remain unchanged.
/// Constructed only via [`FjallDB::inner`], which ensures the `Option` is
/// `Some` before handing out the guard.
pub(super) struct FjallDBGuard<'a>(std::sync::RwLockReadGuard<'a, Option<SingleWriterTxDatabase>>);

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
        let inner = Self::build_database(path, &config)?;
        let deleted = Arc::new(RwLock::new(HashSet::new()));

        // Load persisted deleted tables
        Self::load_deleted_tables(&inner, &deleted)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(Some(inner))),
            path: Arc::new(path.to_path_buf()),
            config: Arc::new(config.clone()),
            deleted_tables: deleted,
            max_memtable_size: config.max_memtable_size,
        })
    }

    /// Builds a new `SingleWriterTxDatabase` from the given path and config.
    fn build_database(path: &Path, config: &FjallConfig) -> io::Result<SingleWriterTxDatabase> {
        let mut builder = SingleWriterTxDatabase::builder(path)
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

        builder.open().map_err(io::Error::other)
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
    /// and refuses all future writes.  This method:
    ///
    /// 1. Acquires exclusive write access (no readers or writers in flight).
    /// 2. Drops the old (poisoned) database instance, releasing the file lock.
    /// 3. Re-opens the database from the same path and config.
    /// 4. Reloads the in-memory deleted-table metadata from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be re-opened (e.g. the
    /// underlying hardware issue has not been resolved yet).
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

        // Step 1: drop the old (possibly poisoned) instance so fjall releases
        // its exclusive file lock on the database directory.
        *inner_guard = None;

        // Step 2: re-open the database.  This will fail if the underlying
        // issue (e.g. disk full) has not been resolved.
        let new_db = Self::build_database(&self.path, &self.config)?;

        // Step 3: reload deleted-table metadata from the fresh database.
        deleted_guard.clear();
        Self::load_deleted_tables_into(&new_db, &mut deleted_guard)?;

        // Step 4: install the new database.
        *inner_guard = Some(new_db);

        Ok(())
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
}
