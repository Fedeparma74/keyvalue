use crate::{MaybeSendSync, io};
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
    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names()?.contains(&table_name.to_string()))
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key)?.is_some())
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let mut keys = Vec::new();
        for (key, _) in self.iter(table_name)? {
            keys.push(key);
        }
        Ok(keys)
    }
    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name)? {
            values.push(value);
        }
        Ok(values)
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
