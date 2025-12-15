use crate::{AsyncKeyValueDB, MaybeSendSync, io};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

use async_trait::async_trait;

use super::VersionedObject;

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncVersionedKeyValueDB: MaybeSendSync + 'static {
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    async fn remove(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error>;
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error>;
    async fn table_names(&self) -> Result<Vec<String>, io::Error>;

    /// Updates the value of the key in the table and increases the version by 1.
    /// If the key does not exist, it will be inserted with version 1.
    async fn update(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<VersionedObject>, io::Error> {
        let current_value = AsyncVersionedKeyValueDB::get(self, table_name, key).await?;
        let new_version = match current_value {
            Some(ref obj) => obj.version.checked_add(1).ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "Version overflow",
            ))?,
            None => 1,
        };
        AsyncVersionedKeyValueDB::insert(self, table_name, key, value, new_version).await
    }

    #[allow(clippy::type_complexity)]
    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in AsyncVersionedKeyValueDB::iter(self, table_name).await? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }
    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(AsyncVersionedKeyValueDB::table_names(self)
            .await?
            .contains(&table_name.to_string()))
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(AsyncVersionedKeyValueDB::get(self, table_name, key)
            .await?
            .is_some())
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let mut keys = Vec::new();
        for (key, _) in AsyncVersionedKeyValueDB::iter(self, table_name).await? {
            keys.push(key);
        }
        Ok(keys)
    }
    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in AsyncVersionedKeyValueDB::iter(self, table_name).await? {
            values.push(value);
        }
        Ok(values)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        for key in AsyncVersionedKeyValueDB::keys(self, table_name).await? {
            AsyncVersionedKeyValueDB::remove(self, table_name, &key).await?;
        }
        Ok(())
    }
    async fn clear(&self) -> Result<(), io::Error> {
        for table_name in AsyncVersionedKeyValueDB::table_names(self).await? {
            AsyncVersionedKeyValueDB::delete_table(self, &table_name).await?;
        }
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncVersionedKeyValueDB for dyn AsyncKeyValueDB {
    async fn insert(
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

        let old_value = self.insert(table_name, key, &encoded).await?;
        if let Some(old_value) = old_value {
            let old_object = bincode::decode_from_slice(&old_value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(old_object.0))
        } else {
            Ok(None)
        }
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = self.get(table_name, key).await?;
        if let Some(value) = value {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    async fn remove(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = self.remove(table_name, key).await?;
        if let Some(old_value) = old_value {
            let obj = bincode::decode_from_slice(&old_value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name).await? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        self.table_names().await
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter_from_prefix(table_name, prefix).await? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
        }
        Ok(result)
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        self.contains_table(table_name).await
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        self.contains_key(table_name, key).await
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        self.keys(table_name).await
    }
    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name).await? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            values.push(obj.0);
        }
        Ok(values)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.delete_table(table_name).await
    }
    async fn clear(&self) -> Result<(), io::Error> {
        self.clear().await
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T> AsyncVersionedKeyValueDB for T
where
    T: AsyncKeyValueDB,
{
    async fn insert(
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

        let old_value = self.insert(table_name, key, &encoded).await?;
        if let Some(old_value) = old_value {
            let old_object = bincode::decode_from_slice(&old_value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(old_object.0))
        } else {
            Ok(None)
        }
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = self.get(table_name, key).await?;
        if let Some(value) = value {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    async fn remove(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = self.remove(table_name, key).await?;
        if let Some(old_value) = old_value {
            let obj = bincode::decode_from_slice(&old_value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(obj.0))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name).await? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        self.table_names().await
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter_from_prefix(table_name, prefix).await? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.push((key, obj.0));
        }
        Ok(result)
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        self.contains_table(table_name).await
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        self.contains_key(table_name, key).await
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        self.keys(table_name).await
    }
    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name).await? {
            let obj = bincode::decode_from_slice(&value, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            values.push(obj.0);
        }
        Ok(values)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.delete_table(table_name).await
    }
    async fn clear(&self) -> Result<(), io::Error> {
        self.clear().await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn AsyncVersionedKeyValueDB>> = None;
    }
}
