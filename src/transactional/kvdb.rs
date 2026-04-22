use crate::{Direction, KeyRange, MaybeSendSync, apply_range_in_memory, io};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use super::WriteOp;

/// A key-value database that supports explicit read and write transactions.
///
/// Transactions provide snapshot isolation for reads and atomic commit/abort
/// semantics for writes. The associated types define the concrete transaction
/// handles returned by [`begin_read`](TransactionalKVDB::begin_read) and
/// [`begin_write`](TransactionalKVDB::begin_write).
pub trait TransactionalKVDB: MaybeSendSync + 'static {
    /// The read transaction handle type.
    type ReadTransaction<'a>: KVReadTransaction<'a>;
    /// The write transaction handle type.
    type WriteTransaction<'a>: KVWriteTransaction<'a>;

    /// Begins a new read-only transaction (snapshot).
    fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error>;
    /// Begins a new read-write transaction.
    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error>;

    /// Attempts to recover the database from an unrecoverable internal state.
    ///
    /// The default implementation is a no-op. Backends that support runtime
    /// recovery should override this.
    fn try_recover(&self) -> Result<(), io::Error> {
        Ok(())
    }
}

/// Read-only transaction handle.
///
/// Provides a consistent snapshot of the database at the time the transaction
/// was opened. The snapshot remains valid until the handle is dropped.
pub trait KVReadTransaction<'a>: MaybeSendSync {
    /// Retrieves the value for `key` in `table_name` as of this snapshot.
    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    /// Iterates all entries in `table_name` as of this snapshot.
    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;
    /// Lists all table names as of this snapshot.
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    #[allow(clippy::type_complexity)]
    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name)? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }

    /// Transactional counterpart of [`crate::KeyValueDB::iter_range`].
    ///
    /// See the non-transactional trait for semantics.  The default
    /// implementation is an in-memory fallback; every shipped backend
    /// overrides this with a native range-scan that honours the transaction
    /// snapshot and any pending writes (read-your-own-writes).
    #[allow(clippy::type_complexity)]
    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let items = match &range.prefix {
            Some(p) => self.iter_from_prefix(table_name, p.as_str())?,
            None => self.iter(table_name)?,
        };
        Ok(apply_range_in_memory(items, &range))
    }

    /// Cursor-based pagination over the full table.
    #[allow(clippy::type_complexity)]
    fn iter_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut range = KeyRange::all().with_direction(direction).with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        self.iter_range(table_name, range)
    }

    /// Cursor-based pagination restricted to a prefix.
    #[allow(clippy::type_complexity)]
    fn iter_from_prefix_paginated(
        &self,
        table_name: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut range = KeyRange::prefix(prefix)
            .with_direction(direction)
            .with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        self.iter_range(table_name, range)
    }

    /// Cursor-based pagination returning only keys.
    fn keys_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<String>, io::Error> {
        Ok(self
            .iter_paginated(table_name, start_after, limit, direction)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    /// Cursor-based pagination returning only values.
    fn values_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .iter_paginated(table_name, start_after, limit, direction)?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names()?.contains(&table_name.to_string()))
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key)?.is_some())
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self.iter(table_name)?.into_iter().map(|(k, _)| k).collect())
    }
    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self.iter(table_name)?.into_iter().map(|(_, v)| v).collect())
    }
}

/// Read-write transaction handle.
///
/// Extends [`KVReadTransaction`] with mutating operations. Changes are
/// buffered locally and only become visible to other transactions after
/// [`commit()`](KVWriteTransaction::commit) succeeds. Calling
/// [`abort()`](KVWriteTransaction::abort) discards all pending changes.
pub trait KVWriteTransaction<'a>: KVReadTransaction<'a> {
    /// Inserts a key-value pair. Returns the previous value, if any.
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;
    /// Removes the entry for `key`. Returns the previous value, if any.
    fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    /// Atomically persists all changes made in this transaction.
    fn commit(self) -> Result<(), io::Error>;
    /// Discards all changes made in this transaction.
    fn abort(self) -> Result<(), io::Error>;

    fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name)? {
            self.remove(table_name, &key)?;
        }
        Ok(())
    }
    fn clear(&mut self) -> Result<(), io::Error> {
        for table_name in self.table_names()? {
            self.delete_table(&table_name)?;
        }
        Ok(())
    }

    /// Applies a batch of [`WriteOp`]s and commits in a single step.
    ///
    /// The default implementation applies each operation individually and then
    /// calls [`commit`](Self::commit).
    fn batch_commit(mut self, ops: Vec<WriteOp>) -> Result<(), io::Error>
    where
        Self: Sized,
    {
        for op in ops {
            match op {
                WriteOp::Insert {
                    table_name,
                    key,
                    value,
                } => {
                    self.insert(&table_name, &key, &value)?;
                }
                WriteOp::Remove { table_name, key } => {
                    self.remove(&table_name, &key)?;
                }
                WriteOp::DeleteTable { table_name } => {
                    self.delete_table(&table_name)?;
                }
                WriteOp::Clear => {
                    self.clear()?;
                }
            }
        }
        self.commit()
    }
}
