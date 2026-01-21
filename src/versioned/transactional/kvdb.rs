use crate::{
    KVReadTransaction, KVWriteTransaction, MaybeSendSync, TransactionalKVDB, decode, encode, io,
    versioned::VersionedObject,
};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

pub trait VersionedTransactionalKVDB: MaybeSendSync + 'static {
    type ReadTransaction: KVReadVersionedTransaction;
    type WriteTransaction: KVWriteVersionedTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error>;
    fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error>;
}

pub trait KVReadVersionedTransaction: MaybeSendSync {
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

pub trait KVWriteVersionedTransaction: KVReadVersionedTransaction {
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    fn remove(
        &mut self,
        table_name: &str,
        key: &str,
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
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
        match value {
            Some(v) => self.insert(table_name, key, v, new_version),
            None => self.remove(table_name, key, new_version),
        }
    }

    fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name)? {
            self.update(table_name, &key, None)?;
        }
        Ok(())
    }
    fn clear(&mut self) -> Result<(), io::Error> {
        for table_name in self.table_names()? {
            self.delete_table(&table_name)?;
        }
        Ok(())
    }
}

impl<T> VersionedTransactionalKVDB for T
where
    T: TransactionalKVDB,
{
    type ReadTransaction = T::ReadTransaction;
    type WriteTransaction = T::WriteTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error> {
        TransactionalKVDB::begin_read(self)
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error> {
        TransactionalKVDB::begin_write(self)
    }
}

impl<T> KVReadVersionedTransaction for T
where
    T: KVReadTransaction,
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

impl<T> KVWriteVersionedTransaction for T
where
    T: KVWriteTransaction,
{
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let obj = VersionedObject {
            value: Some(value.to_vec()),
            version,
        };

        let old_value = KVWriteTransaction::insert(self, table_name, key, &encode(&obj))?;
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
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = KVWriteTransaction::remove(self, table_name, key)?;
        if let Some(old_value) = old_value {
            let obj = decode(&old_value)?;

            let new_obj = VersionedObject {
                value: None,
                version,
            };

            KVWriteTransaction::insert(self, table_name, key, &encode(&new_obj))?;

            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        KVWriteTransaction::delete_table(self, table_name)
    }

    fn clear(&mut self) -> Result<(), io::Error> {
        KVWriteTransaction::clear(self)
    }

    fn commit(self) -> Result<(), io::Error> {
        KVWriteTransaction::commit(self)
    }

    fn abort(self) -> Result<(), io::Error> {
        KVWriteTransaction::abort(self)
    }
}
