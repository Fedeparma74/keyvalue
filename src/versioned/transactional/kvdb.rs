use crate::{
    KVReadTransaction, KVWriteTransaction, MaybeSendSync, TransactionalKVDB, io,
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

pub trait KVReadVersionedTransaction: MaybeSendSync + 'static {
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
        value: &[u8],
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
}

impl<T> VersionedTransactionalKVDB for T
where
    T: TransactionalKVDB,
{
    type ReadTransaction = T::ReadTransaction;
    type WriteTransaction = T::WriteTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error> {
        self.begin_read()
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error> {
        self.begin_write()
    }
}

impl<T> KVReadVersionedTransaction for T
where
    T: KVReadTransaction,
{
    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = self.get(table_name, key)?;
        if let Some(value) = value {
            let (obj, _): (VersionedObject, _) =
                bincode::decode_from_slice(&value, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name)? {
            let (obj, _): (VersionedObject, _) =
                bincode::decode_from_slice(&value, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj));
        }
        Ok(result)
    }
    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        self.table_names()
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter_from_prefix(table_name, prefix)? {
            let (obj, _): (VersionedObject, _) =
                bincode::decode_from_slice(&value, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj));
        }
        Ok(result)
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        self.contains_table(table_name)
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        self.contains_key(table_name, key)
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        self.keys(table_name)
    }
    fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name)? {
            let (obj, _): (VersionedObject, _) =
                bincode::decode_from_slice(&value, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            values.push(obj);
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
        let encoded = bincode::encode_to_vec(&obj, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let old_value = self.insert(table_name, key, &encoded)?;
        if let Some(old_value) = old_value {
            let old_object = bincode::decode_from_slice(&old_value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(old_object.0))
        } else {
            Ok(None)
        }
    }

    fn remove(
        &mut self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = self.remove(table_name, key)?;
        if let Some(old_value) = old_value {
            let (obj, _): (VersionedObject, _) =
                bincode::decode_from_slice(&old_value, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let new_obj = VersionedObject {
                value: None,
                version: obj.version.checked_add(1).ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Version overflow",
                ))?,
            };
            let encoded = bincode::encode_to_vec(&new_obj, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.insert(table_name, key, &encoded)?;

            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }
    fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        self.delete_table(table_name)
    }
    fn clear(&mut self) -> Result<(), io::Error> {
        self.clear()
    }

    fn commit(self) -> Result<(), io::Error> {
        self.commit()
    }
    fn abort(self) -> Result<(), io::Error> {
        self.abort()
    }
}
