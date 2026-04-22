use crate::{
    Direction, KVReadTransaction, KVWriteTransaction, KeyRange, MaybeSendSync, TransactionalKVDB,
    apply_range_in_memory, decode, encode, io, versioned::VersionedObject,
};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

/// Synchronous transactional key-value store with per-entry versioning.
///
/// Blanket-implemented for every `T: TransactionalKVDB`.
pub trait VersionedTransactionalKVDB: MaybeSendSync + 'static {
    type ReadTransaction<'a>: KVReadVersionedTransaction<'a>;
    type WriteTransaction<'a>: KVWriteVersionedTransaction<'a>;

    fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error>;
    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error>;
}

/// Read-only versioned transaction.  Blanket-implemented for `T: KVReadTransaction`.
pub trait KVReadVersionedTransaction<'a>: MaybeSendSync {
    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error>;
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    #[allow(clippy::type_complexity)]
    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name)? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }

    /// Versioned transactional counterpart of
    /// [`crate::KeyValueDB::iter_range`].
    ///
    /// Default implementation filters the full `iter()` output; the
    /// blanket impl over `T: KVReadTransaction` overrides this to delegate
    /// to the backend's native range scan and decode only the returned
    /// entries.
    #[allow(clippy::type_complexity)]
    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let items = self.iter(table_name)?;
        Ok(apply_range_in_memory(items, &range))
    }

    /// Cursor-based pagination.
    #[allow(clippy::type_complexity)]
    fn iter_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
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
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut range = KeyRange::prefix(prefix)
            .with_direction(direction)
            .with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        self.iter_range(table_name, range)
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
    fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name)? {
            values.push(value);
        }
        Ok(values)
    }
}

/// Read-write versioned transaction.  Extends [`KVReadVersionedTransaction`]
/// with mutations, auto-increment [`update`](Self::update), and
/// soft-delete / prune on [`delete_table`](Self::delete_table) and
/// [`clear`](Self::clear).  Blanket-implemented for `T: KVWriteTransaction`.
pub trait KVWriteVersionedTransaction<'a>: KVReadVersionedTransaction<'a> {
    /// Inserts or updates the value of the key in the table with the specified version.
    /// If value is `None`, the entry is marked as deleted by setting its value to `None` and the specified version.
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    /// Permanently removes the entry from the table.
    fn remove(&mut self, table_name: &str, key: &str)
    -> Result<Option<VersionedObject>, io::Error>;
    fn commit(self) -> Result<(), io::Error>;
    fn abort(self) -> Result<(), io::Error>;

    /// Updates the value of the key in the table and increases the version by 1.
    /// If the key does not exist, it will be inserted with version 1.
    fn update(
        &mut self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let current_object = KVReadVersionedTransaction::get(self, table_name, key)?;
        let new_version = match current_object {
            Some(ref obj) => obj.version.checked_add(1).ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "Version overflow",
            ))?,
            None => 1,
        };
        KVWriteVersionedTransaction::insert(self, table_name, key, value, new_version)
    }

    /// Deletes all the entries in the specified table. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    fn delete_table(&mut self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        for key in self.keys(table_name)? {
            if prune {
                KVWriteVersionedTransaction::remove(self, table_name, &key)?;
            } else {
                KVWriteVersionedTransaction::update(self, table_name, &key, None)?;
            }
        }
        Ok(())
    }
    /// Clears all the tables in the database. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    fn clear(&mut self, prune: bool) -> Result<(), io::Error> {
        for table_name in self.table_names()? {
            KVWriteVersionedTransaction::delete_table(self, &table_name, prune)?;
        }
        Ok(())
    }
}

impl<T> VersionedTransactionalKVDB for T
where
    T: TransactionalKVDB,
{
    type ReadTransaction<'a> = T::ReadTransaction<'a>;
    type WriteTransaction<'a> = T::WriteTransaction<'a>;

    fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        TransactionalKVDB::begin_read(self)
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        TransactionalKVDB::begin_write(self)
    }
}

impl<'a, T> KVReadVersionedTransaction<'a> for T
where
    T: KVReadTransaction<'a>,
{
    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = KVReadTransaction::get(self, table_name, key)?;
        if let Some(value) = value {
            Ok(Some(decode(&value)?))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in KVReadTransaction::iter(self, table_name)? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KVReadTransaction::table_names(self)
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in KVReadTransaction::iter_from_prefix(self, table_name, prefix)? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in KVReadTransaction::iter_range(self, table_name, range)? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        KVReadTransaction::contains_table(self, table_name)
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KVReadTransaction::contains_key(self, table_name, key)
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KVReadTransaction::keys(self, table_name)
    }

    fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in KVReadTransaction::iter(self, table_name)? {
            values.push(decode(&value)?);
        }
        Ok(values)
    }
}

impl<'a, T> KVWriteVersionedTransaction<'a> for T
where
    T: KVWriteTransaction<'a>,
{
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let obj = VersionedObject {
            value: value.map(|v| v.to_vec()),
            version,
        };

        let old_value = KVWriteTransaction::insert(self, table_name, key, &encode(&obj)?)?;
        if let Some(old_value) = old_value {
            Ok(Some(decode(&old_value)?))
        } else {
            Ok(None)
        }
    }

    fn remove(
        &mut self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error> {
        KVWriteTransaction::remove(self, table_name, key)?
            .map(|v| decode(&v))
            .transpose()
    }

    fn delete_table(&mut self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        if prune {
            KVWriteTransaction::delete_table(self, table_name)?;
        } else {
            for key in self.keys(table_name)? {
                KVWriteVersionedTransaction::update(self, table_name, &key, None)?;
            }
        }

        Ok(())
    }

    fn clear(&mut self, prune: bool) -> Result<(), io::Error> {
        if prune {
            KVWriteTransaction::clear(self)?;
        } else {
            for table_name in self.table_names()? {
                KVWriteVersionedTransaction::delete_table(self, &table_name, false)?;
            }
        }

        Ok(())
    }

    fn commit(self) -> Result<(), io::Error> {
        KVWriteTransaction::commit(self)
    }

    fn abort(self) -> Result<(), io::Error> {
        KVWriteTransaction::abort(self)
    }
}
