use crate::{KeyValueDB, MaybeSendSync, decode, encode, io};
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
    /// Removes the entry from the table. If `version` is provided, the entry is marked as deleted
    /// by setting its value to `None` and updating its version. If `version` is `None`, the entry is
    /// permanently removed.
    fn remove(
        &self,
        table_name: &str,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<VersionedObject>, io::Error>;
    #[allow(clippy::type_complexity)]
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error>;
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    /// Updates the value of the key in the table and increases the version by 1.
    /// If the key does not exist, it will be inserted with version 1.
    fn update(
        &self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let current_object = VersionedKeyValueDB::get(self, table_name, key)?;
        let new_version = match current_object {
            Some(ref obj) => obj.version.checked_add(1).ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "Version overflow",
            ))?,
            None => 1,
        };
        match value {
            Some(v) => VersionedKeyValueDB::insert(self, table_name, key, v, new_version),
            None => VersionedKeyValueDB::remove(self, table_name, key, Some(new_version)),
        }
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
    /// Deletes all the entries in the specified table. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    fn delete_table(&self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        for key in VersionedKeyValueDB::keys(self, table_name)? {
            if prune {
                VersionedKeyValueDB::remove(self, table_name, &key, None)?;
            } else {
                VersionedKeyValueDB::update(self, table_name, &key, None)?;
            }
        }
        Ok(())
    }
    /// Clears all the tables in the database. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    fn clear(&self, prune: bool) -> Result<(), io::Error> {
        for table_name in VersionedKeyValueDB::table_names(self)? {
            VersionedKeyValueDB::delete_table(self, &table_name, prune)?;
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
            value: Some(value.to_vec()),
            version,
        };

        let old_value = KeyValueDB::insert(self, table_name, key, &encode(&obj))?;
        if let Some(old_value) = old_value {
            Ok(Some(decode(&old_value)?))
        } else {
            Ok(None)
        }
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = KeyValueDB::get(self, table_name, key)?;
        if let Some(value) = value {
            Ok(Some(decode(&value)?))
        } else {
            Ok(None)
        }
    }
    fn remove(
        &self,
        table_name: &str,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = KeyValueDB::remove(self, table_name, key)?;
        if let Some(version) = version {
            if let Some(old_value) = old_value {
                let obj = decode(&old_value)?;

                let new_obj = VersionedObject {
                    value: None,
                    version,
                };

                KeyValueDB::insert(self, table_name, key, &encode(&new_obj))?;

                Ok(Some(obj))
            } else {
                Ok(None)
            }
        } else {
            Ok(old_value.map(|v| decode(&v)).transpose()?)
        }
    }
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in KeyValueDB::iter(self, table_name)? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }
    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KeyValueDB::table_names(self)
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in KeyValueDB::iter_from_prefix(self, table_name, prefix)? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_table(self, table_name)
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_key(self, table_name, key)
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KeyValueDB::keys(self, table_name)
    }
    fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in KeyValueDB::iter(self, table_name)? {
            values.push(decode(&value)?);
        }
        Ok(values)
    }
    fn delete_table(&self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        if prune {
            KeyValueDB::delete_table(self, table_name)?;
        } else {
            for key in self.keys(table_name)? {
                VersionedKeyValueDB::update(self, table_name, &key, None)?;
            }
        }
        Ok(())
    }
    fn clear(&self, prune: bool) -> Result<(), io::Error> {
        if prune {
            KeyValueDB::clear(self)?;
        } else {
            for table_name in self.table_names()? {
                VersionedKeyValueDB::delete_table(self, &table_name, false)?;
            }
        }
        Ok(())
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
            value: Some(value.to_vec()),
            version,
        };

        let old_value = KeyValueDB::insert(self, table_name, key, &encode(&obj))?;
        if let Some(old_value) = old_value {
            Ok(Some(decode(&old_value)?))
        } else {
            Ok(None)
        }
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = KeyValueDB::get(self, table_name, key)?;
        if let Some(value) = value {
            Ok(Some(decode(&value)?))
        } else {
            Ok(None)
        }
    }
    fn remove(
        &self,
        table_name: &str,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = KeyValueDB::remove(self, table_name, key)?;
        if let Some(version) = version {
            if let Some(old_value) = old_value {
                let obj = decode(&old_value)?;

                let new_obj = VersionedObject {
                    value: None,
                    version,
                };

                KeyValueDB::insert(self, table_name, key, &encode(&new_obj))?;

                Ok(Some(obj))
            } else {
                Ok(None)
            }
        } else {
            Ok(old_value.map(|v| decode(&v)).transpose()?)
        }
    }
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in KeyValueDB::iter(self, table_name)? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }
    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KeyValueDB::table_names(self)
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in KeyValueDB::iter_from_prefix(self, table_name, prefix)? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_table(self, table_name)
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_key(self, table_name, key)
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KeyValueDB::keys(self, table_name)
    }
    fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in KeyValueDB::iter(self, table_name)? {
            values.push(decode(&value)?);
        }
        Ok(values)
    }
    fn delete_table(&self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        if prune {
            KeyValueDB::delete_table(self, table_name)?;
        } else {
            for key in self.keys(table_name)? {
                VersionedKeyValueDB::update(self, table_name, &key, None)?;
            }
        }
        Ok(())
    }
    fn clear(&self, prune: bool) -> Result<(), io::Error> {
        if prune {
            KeyValueDB::clear(self)?;
        } else {
            for table_name in self.table_names()? {
                VersionedKeyValueDB::delete_table(self, &table_name, false)?;
            }
        }
        Ok(())
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
