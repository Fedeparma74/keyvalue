//! [redb](https://docs.rs/redb)-backed key-value store.
//!
//! `redb` is a pure-Rust, ACID-compliant, embedded database. This module
//! provides [`RedbDB`], which implements [`KeyValueDB`](crate::KeyValueDB)
//! (and, when the `tokio` feature is enabled, [`AsyncKeyValueDB`](crate::AsyncKeyValueDB)
//! via `spawn_blocking`). The optional `transactional` feature adds
//! [`TransactionalKVDB`](crate::TransactionalKVDB) support.

use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use ::redb::{CommitError, Database, DatabaseError, StorageError, TableError, TransactionError};
use redb::{Durability, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};

use crate::KeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

/// Durability guarantee for redb write transactions.
///
/// Re-exported from `redb` for convenience so callers do not need to depend on
/// the upstream crate directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedbDurability {
    /// Do **not** flush to disk on commit.  Data may be lost on crash, but
    /// throughput is much higher.
    None,
    /// Flush to disk on every commit.  Every committed transaction is
    /// guaranteed to survive a crash.
    Immediate,
}

impl From<RedbDurability> for Durability {
    fn from(d: RedbDurability) -> Self {
        match d {
            RedbDurability::None => Durability::None,
            RedbDurability::Immediate => Durability::Immediate,
        }
    }
}

/// Configuration for a [`RedbDB`] instance.
///
/// Use [`Default::default()`] for sensible defaults.  All sizes are in
/// **bytes**.
#[derive(Debug, Clone)]
pub struct RedbConfig {
    /// Total cache capacity (read + write).
    ///
    /// Default: **1 GiB** (redb upstream default).
    pub cache_size: usize,

    /// Durability guarantee for write transactions.
    ///
    /// **Durability:** [`RedbDurability::Immediate`] flushes every commit to
    /// disk; [`RedbDurability::None`] defers flushing for higher throughput
    /// at the cost of losing the most recent commits on crash.
    ///
    /// Default: [`RedbDurability::None`].
    pub durability: RedbDurability,

    /// Enable two-phase commit protocol.  Required for cross-database
    /// consistency guarantees.
    ///
    /// Default: **false**.
    pub two_phase_commit: bool,

    /// Enable the quick-repair optimisation.  When `true`, redb can repair
    /// a partially-committed transaction faster at the cost of slightly
    /// increased write amplification.
    ///
    /// Default: **false**.
    pub quick_repair: bool,
}

impl Default for RedbConfig {
    fn default() -> Self {
        Self {
            cache_size: 1024 * 1024 * 1024,
            durability: RedbDurability::None,
            two_phase_commit: false,
            quick_repair: false,
        }
    }
}

impl RedbConfig {
    /// Sets the total cache capacity.
    #[must_use]
    pub fn cache_size(mut self, bytes: usize) -> Self {
        self.cache_size = bytes;
        self
    }

    /// Sets the durability guarantee for write transactions.
    #[must_use]
    pub fn durability(mut self, d: RedbDurability) -> Self {
        self.durability = d;
        self
    }

    /// Enables or disables two-phase commit.
    #[must_use]
    pub fn two_phase_commit(mut self, flag: bool) -> Self {
        self.two_phase_commit = flag;
        self
    }

    /// Enables or disables quick-repair.
    #[must_use]
    pub fn quick_repair(mut self, flag: bool) -> Self {
        self.quick_repair = flag;
        self
    }
}

/// Key-value database backed by [redb](https://docs.rs/redb).
///
/// Internally wraps a `redb::Database` behind an `Arc` so that cloning is
/// cheap and the handle can be shared across threads.
#[derive(Debug, Clone)]
pub struct RedbDB {
    inner: Arc<RwLock<Option<Database>>>,
    path: Arc<PathBuf>,
    config: Arc<RedbConfig>,
}

/// RAII guard returned by [`RedbDB::inner`].
///
/// Derefs to `redb::Database` so all call-sites remain unchanged.
/// Constructed only via [`RedbDB::inner`], which ensures the `Option` is
/// `Some` before handing out the guard.
pub(super) struct RedbDBGuard<'a>(std::sync::RwLockReadGuard<'a, Option<Database>>);

impl<'a> std::ops::Deref for RedbDBGuard<'a> {
    type Target = Database;
    fn deref(&self) -> &Self::Target {
        // SAFETY: `inner()` only constructs this guard after verifying `is_some()`.
        self.0
            .as_ref()
            .expect("RedbDB inner is None during recovery")
    }
}

fn lock_poisoned() -> io::Error {
    io::Error::other("RwLock poisoned")
}

impl RedbDB {
    /// Opens (or creates) a redb database at the given filesystem `path`
    /// using [`RedbConfig::default()`].
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_config(path, RedbConfig::default())
    }

    /// Opens (or creates) a redb database at `path` with custom
    /// [`RedbConfig`].
    pub fn open_with_config(path: &Path, config: RedbConfig) -> io::Result<Self> {
        let inner = Self::build_database(path, &config)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(Some(inner))),
            path: Arc::new(path.to_path_buf()),
            config: Arc::new(config),
        })
    }

    /// Builds a new `Database` from the given path and config.
    fn build_database(path: &Path, config: &RedbConfig) -> io::Result<Database> {
        let mut builder = Database::builder();
        builder.set_cache_size(config.cache_size);
        builder.create(path).map_err(database_error_to_io_error)
    }

    /// Attempts to recover from a redb error state by re-opening the
    /// underlying database.
    ///
    /// When a critical I/O error is encountered (e.g. disk full) the database
    /// handle becomes unusable.  This method:
    ///
    /// 1. Acquires exclusive write access (no readers or writers in flight).
    /// 2. Drops the old (possibly broken) database instance, releasing the
    ///    file lock held by redb.
    /// 3. Re-opens the database from the same path and config.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be re-opened (e.g. the
    /// underlying hardware issue has not been resolved yet).
    pub fn try_recover_from_error(&self) -> io::Result<()> {
        // Acquire write lock; also recover from any RwLock poison caused by
        // a panicking thread that held the lock.
        let mut inner_guard = match self.inner.write() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };

        // Step 1: drop the old (possibly broken) instance so redb releases
        // its exclusive file lock.
        *inner_guard = None;

        // Step 2: re-open the database.
        let new_db = Self::build_database(&self.path, &self.config)?;

        // Step 3: install the new database.
        *inner_guard = Some(new_db);

        Ok(())
    }

    /// Acquires a read lock on the inner database.
    ///
    /// Returns an error if the database is currently being recovered
    /// (i.e. a `try_recover_from_error` call is in progress).
    fn inner(&self) -> io::Result<RedbDBGuard<'_>> {
        let guard = self.inner.read().map_err(|_| lock_poisoned())?;
        if guard.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "redb database is being recovered; retry after try_recover_from_error completes",
            ));
        }
        Ok(RedbDBGuard(guard))
    }
}

#[cfg(feature = "tokio")]
crate::impl_async_kvdb_via_spawn_blocking!(RedbDB);

impl RedbDB {
    /// Begins a write transaction configured with the stored durability,
    /// two-phase-commit, and quick-repair settings.
    fn begin_configured_write(&self) -> io::Result<redb::WriteTransaction> {
        let inner = self.inner()?;
        let mut tx = inner.begin_write().map_err(transaction_error_to_io_error)?;
        tx.set_durability(self.config.durability.into())
            .map_err(io::Error::other)?;
        tx.set_two_phase_commit(self.config.two_phase_commit);
        tx.set_quick_repair(self.config.quick_repair);
        Ok(tx)
    }
}

impl KeyValueDB for RedbDB {
    fn insert(&self, table_name: &str, key: &str, value: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let write_transaction = self.begin_configured_write()?;
        let old_value = {
            let mut table = write_transaction
                .open_table(TableDefinition::<&str, &[u8]>::new(table_name))
                .map_err(table_error_to_io_error)?;

            table
                .insert(key, value)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value().to_vec())
        };
        write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        Ok(old_value)
    }

    fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let read_transaction = self
            .inner()?
            .begin_read()
            .map_err(transaction_error_to_io_error)?;
        let value = {
            let table_res =
                read_transaction.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
            let table = match table_res {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => {
                    return Ok(None);
                }
                Err(e) => return Err(table_error_to_io_error(e)),
            };

            table
                .get(key)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value().to_vec())
        };

        Ok(value)
    }

    fn remove(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let write_transaction = self.begin_configured_write()?;
        let old_value = {
            let table_res =
                write_transaction.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
            let mut table = match table_res {
                Ok(table) => Some(table),
                Err(TableError::TableDoesNotExist(_)) => None,
                Err(e) => return Err(table_error_to_io_error(e)),
            };

            if let Some(table) = table.as_mut() {
                table
                    .remove(key)
                    .map_err(storage_error_to_io_error)?
                    .map(|v| v.value().to_vec())
            } else {
                None
            }
        };

        if old_value.is_none() {
            write_transaction
                .abort()
                .map_err(storage_error_to_io_error)?;
        } else {
            write_transaction
                .commit()
                .map_err(commit_error_to_io_error)?;
        }

        Ok(old_value)
    }

    fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let read_transaction = self
            .inner()?
            .begin_read()
            .map_err(transaction_error_to_io_error)?;
        let table_res =
            read_transaction.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
        let table = match table_res {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(table_error_to_io_error(e)),
        };
        let mut result = Vec::new();
        for item in table.iter().map_err(storage_error_to_io_error)? {
            let (key, value) = item.map_err(storage_error_to_io_error)?;
            result.push((key.value().to_string(), value.value().to_vec()));
        }
        Ok(result)
    }

    fn iter_range(
        &self,
        table_name: &str,
        range: crate::KeyRange,
    ) -> io::Result<Vec<(String, Vec<u8>)>> {
        let read_transaction = self
            .inner()?
            .begin_read()
            .map_err(transaction_error_to_io_error)?;
        let table_res =
            read_transaction.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
        let table = match table_res {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(table_error_to_io_error(e)),
        };
        redb_collect_range(&table, &range)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let read_transaction = self
            .inner()?
            .begin_read()
            .map_err(transaction_error_to_io_error)?;
        let mut result = Vec::new();
        let tables_res = read_transaction.list_tables();
        match tables_res {
            Ok(tables) => {
                for table_name in tables {
                    result.push(table_name.name().to_string());
                }
            }
            Err(StorageError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(storage_error_to_io_error(e));
            }
        }
        Ok(result)
    }

    fn delete_table(&self, table_name: &str) -> io::Result<()> {
        let write_transaction = self.begin_configured_write()?;
        match write_transaction.delete_table(TableDefinition::<&str, &[u8]>::new(table_name)) {
            Ok(_) => {}
            Err(TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(e) => return Err(table_error_to_io_error(e)),
        }
        write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        let write_transaction = self.begin_configured_write()?;

        for table_name in write_transaction
            .list_tables()
            .map_err(storage_error_to_io_error)?
        {
            write_transaction
                .delete_table(TableDefinition::<&str, &[u8]>::new(table_name.name()))
                .map_err(table_error_to_io_error)?;
        }
        write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        Ok(())
    }
}

fn storage_error_to_io_error(e: StorageError) -> io::Error {
    match e {
        StorageError::Io(e) => e,
        StorageError::ValueTooLarge(size) => io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Value is too large: {}", size),
        ),
        StorageError::Corrupted(e) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Database is corrupted: {}", e),
        ),
        StorageError::LockPoisoned(location) => {
            io::Error::other(format!("Database lock is poisoned: {}", location))
        }
        e => io::Error::other(e),
    }
}

fn database_error_to_io_error(e: DatabaseError) -> io::Error {
    match e {
        DatabaseError::Storage(e) => storage_error_to_io_error(e),
        DatabaseError::DatabaseAlreadyOpen => io::Error::other("Database is already open"),
        DatabaseError::RepairAborted => io::Error::other("Database repair was aborted"),
        DatabaseError::UpgradeRequired(version) => {
            io::Error::other(format!("Database upgrade required to version {}", version))
        }
        e => io::Error::other(e),
    }
}

fn transaction_error_to_io_error(e: TransactionError) -> io::Error {
    match e {
        TransactionError::Storage(e) => storage_error_to_io_error(e),
        e => io::Error::other(e),
    }
}

fn table_error_to_io_error(e: TableError) -> io::Error {
    match e {
        TableError::Storage(e) => storage_error_to_io_error(e),
        TableError::TableAlreadyOpen(name, location) => {
            io::Error::other(format!("Table {} is already open: {}", name, location))
        }
        TableError::TableDoesNotExist(name) => io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table {} does not exist", name),
        ),
        TableError::TableIsMultimap(name) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Table {} is a multimap", name),
        ),
        TableError::TableIsNotMultimap(name) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Table {} is not a multimap", name),
        ),
        TableError::TableTypeMismatch { table, key, value } => io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Table {} is not a table of type <{:?}, {:?}>",
                table, key, value
            ),
        ),
        TableError::TypeDefinitionChanged {
            name,
            alignment,
            width,
        } => io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Type definition for {:?} changed. Expected alignment {}, width {:?}",
                name, alignment, width
            ),
        ),
        e => io::Error::other(e),
    }
}

fn commit_error_to_io_error(e: CommitError) -> io::Error {
    match e {
        CommitError::Storage(e) => storage_error_to_io_error(e),
        e => io::Error::other(e),
    }
}

/// Shared helper: apply a [`KeyRange`] over an open redb [`ReadableTable`]
/// using the backend-native range scan.
///
/// Used by all three `iter_range` impls ([`RedbDB`],
/// [`ReadTransaction`](transactional::ReadTransaction),
/// [`WriteTransaction`](transactional::WriteTransaction)).
pub(super) fn redb_collect_range<T>(
    table: &T,
    range: &crate::KeyRange,
) -> io::Result<Vec<(String, Vec<u8>)>>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    use std::ops::Bound as OB;

    let lower: OB<&str> = match &range.lower {
        crate::Bound::Unbounded => OB::Unbounded,
        crate::Bound::Included(k) => OB::Included(k.as_str()),
        crate::Bound::Excluded(k) => OB::Excluded(k.as_str()),
    };
    let upper: OB<&str> = match &range.upper {
        crate::Bound::Unbounded => OB::Unbounded,
        crate::Bound::Included(k) => OB::Included(k.as_str()),
        crate::Bound::Excluded(k) => OB::Excluded(k.as_str()),
    };

    let iter = table
        .range::<&str>((lower, upper))
        .map_err(storage_error_to_io_error)?;

    let limit = range.limit.unwrap_or(usize::MAX);
    let mut result = Vec::new();

    macro_rules! process {
        ($it:expr) => {
            for item in $it {
                let (k, v) = item.map_err(storage_error_to_io_error)?;
                let k_str = k.value().to_string();
                if range.is_beyond_far_end(&k_str) {
                    break;
                }
                if !range.matches_prefix(&k_str) {
                    continue;
                }
                result.push((k_str, v.value().to_vec()));
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
