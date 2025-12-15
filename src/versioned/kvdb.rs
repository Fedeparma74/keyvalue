use crate::{KeyValueDB, MaybeSendSync, io};
#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use super::VersionedObject;

pub trait VersionedKeyValueDB: MaybeSendSync + 'static {
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    fn remove(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    #[allow(clippy::type_complexity)]
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error>;
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    /// Updates the value of the key in the table and increases the version by 1.
    /// If the key does not exist, it will be inserted with version 1.
    fn update(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<VersionedObject>, io::Error> {
        let current_value = VersionedKeyValueDB::get(self, table_name, key)?;
        let new_version = match current_value {
            Some(ref obj) => obj.version.checked_add(1).ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "Version overflow",
            ))?,
            None => 1,
        };
        VersionedKeyValueDB::insert(self, table_name, key, value, new_version)
    }

    #[allow(clippy::type_complexity)]
    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in VersionedKeyValueDB::iter(self, table_name)? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }
    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(VersionedKeyValueDB::table_names(self)?.contains(&table_name.to_string()))
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(VersionedKeyValueDB::get(self, table_name, key)?.is_some())
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let mut keys = Vec::new();
        for (key, _) in VersionedKeyValueDB::iter(self, table_name)? {
            keys.push(key);
        }
        Ok(keys)
    }
    fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in VersionedKeyValueDB::iter(self, table_name)? {
            values.push(value);
        }
        Ok(values)
    }
    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        for key in VersionedKeyValueDB::keys(self, table_name)? {
            VersionedKeyValueDB::remove(self, table_name, &key)?;
        }
        Ok(())
    }
    fn clear(&self) -> Result<(), io::Error> {
        for table_name in VersionedKeyValueDB::table_names(self)? {
            VersionedKeyValueDB::delete_table(self, &table_name)?;
        }
        Ok(())
    }
}

impl VersionedKeyValueDB for dyn KeyValueDB {
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let obj = VersionedObject {
            value: value.to_vec(),
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

    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = self.get(table_name, key)?;
        if let Some(value) = value {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    fn remove(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = self.remove(table_name, key)?;
        if let Some(old_value) = old_value {
            let obj = bincode::decode_from_slice(&old_value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name)? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
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
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
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
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            values.push(obj.0);
        }
        Ok(values)
    }
    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.delete_table(table_name)
    }
    fn clear(&self) -> Result<(), io::Error> {
        self.clear()
    }
}

impl<T> VersionedKeyValueDB for T
where
    T: KeyValueDB,
{
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let obj = VersionedObject {
            value: value.to_vec(),
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

    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = self.get(table_name, key)?;
        if let Some(value) = value {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    fn remove(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = self.remove(table_name, key)?;
        if let Some(old_value) = old_value {
            let obj = bincode::decode_from_slice(&old_value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name)? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
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
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
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
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            values.push(obj.0);
        }
        Ok(values)
    }
    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.delete_table(table_name)
    }
    fn clear(&self) -> Result<(), io::Error> {
        self.clear()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn VersionedKeyValueDB>> = None;
    }
}
