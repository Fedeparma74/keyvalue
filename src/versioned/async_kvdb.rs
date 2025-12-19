use crate::{AsyncKeyValueDB, MaybeSendSync, decode, encode, io};
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
        let current_object = AsyncVersionedKeyValueDB::get(self, table_name, key).await?;
        let new_version = match current_object {
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
            value: Some(value.to_vec()),
            version,
        };

        let old_value = AsyncKeyValueDB::insert(self, table_name, key, &encode(&obj)).await?;
        if let Some(old_value) = old_value {
            Ok(Some(decode(&old_value)?))
        } else {
            Ok(None)
        }
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = AsyncKeyValueDB::get(self, table_name, key).await?;
        if let Some(value) = value {
            Ok(Some(decode(&value)?))
        } else {
            Ok(None)
        }
    }
    async fn remove(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = AsyncKeyValueDB::remove(self, table_name, key).await?;
        if let Some(old_value) = old_value {
            let obj = decode(&old_value)?;

            let new_obj = VersionedObject {
                value: None,
                version: obj.version.checked_add(1).ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Version overflow",
                ))?,
            };

            AsyncKeyValueDB::insert(self, table_name, key, &encode(&new_obj)).await?;

            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in AsyncKeyValueDB::iter(self, table_name).await? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        AsyncKeyValueDB::table_names(self).await
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in AsyncKeyValueDB::iter_from_prefix(self, table_name, prefix).await? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        AsyncKeyValueDB::contains_table(self, table_name).await
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        AsyncKeyValueDB::contains_key(self, table_name, key).await
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        AsyncKeyValueDB::keys(self, table_name).await
    }
    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in AsyncKeyValueDB::iter(self, table_name).await? {
            values.push(decode(&value)?);
        }
        Ok(values)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        AsyncKeyValueDB::delete_table(self, table_name).await
    }
    async fn clear(&self) -> Result<(), io::Error> {
        AsyncKeyValueDB::clear(self).await
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
            value: Some(value.to_vec()),
            version,
        };

        let old_value = AsyncKeyValueDB::insert(self, table_name, key, &encode(&obj)).await?;
        if let Some(old_value) = old_value {
            Ok(Some(decode(&old_value)?))
        } else {
            Ok(None)
        }
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = AsyncKeyValueDB::get(self, table_name, key).await?;
        if let Some(value) = value {
            Ok(Some(decode(&value)?))
        } else {
            Ok(None)
        }
    }
    async fn remove(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = AsyncKeyValueDB::remove(self, table_name, key).await?;
        if let Some(old_value) = old_value {
            let obj = decode(&old_value)?;

            let new_obj = VersionedObject {
                value: None,
                version: obj.version.checked_add(1).ok_or(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Version overflow",
                ))?,
            };

            AsyncKeyValueDB::insert(self, table_name, key, &encode(&new_obj)).await?;

            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in AsyncKeyValueDB::iter(self, table_name).await? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        AsyncKeyValueDB::table_names(self).await
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in AsyncKeyValueDB::iter_from_prefix(self, table_name, prefix).await? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        AsyncKeyValueDB::contains_table(self, table_name).await
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        AsyncKeyValueDB::contains_key(self, table_name, key).await
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        AsyncKeyValueDB::keys(self, table_name).await
    }
    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in AsyncKeyValueDB::iter(self, table_name).await? {
            values.push(decode(&value)?);
        }
        Ok(values)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        AsyncKeyValueDB::delete_table(self, table_name).await
    }
    async fn clear(&self) -> Result<(), io::Error> {
        AsyncKeyValueDB::clear(self).await
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
