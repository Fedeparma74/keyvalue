use crate::{
    AsyncKVReadTransaction, AsyncKVWriteTransaction, AsyncTransactionalKVDB, MaybeSendSync, decode,
    encode, io, versioned::VersionedObject,
};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use async_trait::async_trait;

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncVersionedTransactionalKVDB: MaybeSendSync + 'static {
    type ReadTransaction<'a>: AsyncKVReadVersionedTransaction<'a>;
    type WriteTransaction<'a>: AsyncKVWriteVersionedTransaction<'a>;

    async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error>;
    async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error>;
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVReadVersionedTransaction<'a>: MaybeSendSync {
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error>;
    async fn table_names(&self) -> Result<Vec<String>, io::Error>;

    #[allow(clippy::type_complexity)]
    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name).await? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }
    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names().await?.contains(&table_name.to_string()))
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key).await?.is_some())
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let mut keys = Vec::new();
        for (key, _) in self.iter(table_name).await? {
            keys.push(key);
        }
        Ok(keys)
    }
    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name).await? {
            values.push(value);
        }
        Ok(values)
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVWriteVersionedTransaction<'a>: AsyncKVReadVersionedTransaction<'a> {
    async fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    /// Removes the entry from the table. If `version` is provided, the entry is marked as deleted
    /// by setting its value to `None` and updating its version. If `version` is `None`, the entry is
    /// permanently removed.
    async fn remove(
        &mut self,
        table_name: &str,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<VersionedObject>, io::Error>;
    async fn commit(self) -> Result<(), io::Error>;
    async fn abort(self) -> Result<(), io::Error>;

    /// Updates the value of the key in the table and increases the version by 1.
    /// If the key does not exist, it will be inserted with version 1.
    async fn update(
        &mut self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let current_object = AsyncKVReadVersionedTransaction::get(self, table_name, key).await?;
        let new_version = match current_object {
            Some(ref obj) => obj.version.checked_add(1).ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "Version overflow",
            ))?,
            None => 1,
        };
        match value {
            Some(v) => self.insert(table_name, key, v, new_version).await,
            None => self.remove(table_name, key, Some(new_version)).await,
        }
    }

    /// Deletes all the entries in the specified table. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    async fn delete_table(&mut self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        for key in self.keys(table_name).await? {
            if prune {
                self.remove(table_name, &key, None).await?;
            } else {
                self.update(table_name, &key, None).await?;
            }
        }
        Ok(())
    }
    /// Clears all the tables in the database. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    async fn clear(&mut self, prune: bool) -> Result<(), io::Error> {
        for table_name in self.table_names().await? {
            self.delete_table(&table_name, prune).await?;
        }
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T: AsyncTransactionalKVDB> AsyncVersionedTransactionalKVDB for T {
    type ReadTransaction<'a> = T::ReadTransaction<'a>;
    type WriteTransaction<'a> = T::WriteTransaction<'a>;
    async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        AsyncTransactionalKVDB::begin_read(self).await
    }

    async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        AsyncTransactionalKVDB::begin_write(self).await
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a, T> AsyncKVReadVersionedTransaction<'a> for T
where
    T: AsyncKVReadTransaction<'a>,
{
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        let value = AsyncKVReadTransaction::get(self, table_name, key).await?;
        if let Some(value) = value {
            Ok(Some(decode(&value)?))
        } else {
            Ok(None)
        }
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in AsyncKVReadTransaction::iter(self, table_name).await? {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        AsyncKVReadTransaction::table_names(self).await
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in
            AsyncKVReadTransaction::iter_from_prefix(self, table_name, prefix).await?
        {
            result.push((key, decode(&value)?));
        }
        Ok(result)
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        AsyncKVReadTransaction::contains_table(self, table_name).await
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        AsyncKVReadTransaction::contains_key(self, table_name, key).await
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        AsyncKVReadTransaction::keys(self, table_name).await
    }

    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in AsyncKVReadTransaction::iter(self, table_name).await? {
            values.push(decode(&value)?);
        }
        Ok(values)
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a, T> AsyncKVWriteVersionedTransaction<'a> for T
where
    T: AsyncKVWriteTransaction<'a>,
{
    async fn insert(
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

        let old_value =
            AsyncKVWriteTransaction::insert(self, table_name, key, &encode(&obj)).await?;
        if let Some(old_value) = old_value {
            Ok(Some(decode(&old_value)?))
        } else {
            Ok(None)
        }
    }

    async fn remove(
        &mut self,
        table_name: &str,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = AsyncKVWriteTransaction::remove(self, table_name, key).await?;
        if let Some(version) = version {
            if let Some(old_value) = old_value {
                let obj = decode(&old_value)?;

                let new_obj = VersionedObject {
                    value: None,
                    version,
                };

                AsyncKVWriteTransaction::insert(self, table_name, key, &encode(&new_obj)).await?;

                Ok(Some(obj))
            } else {
                Ok(None)
            }
        } else {
            Ok(old_value.map(|v| decode(&v)).transpose()?)
        }
    }

    async fn delete_table(&mut self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        if prune {
            AsyncKVWriteTransaction::delete_table(self, table_name).await?;
        } else {
            for key in self.keys(table_name).await? {
                AsyncKVWriteVersionedTransaction::update(self, table_name, &key, None).await?;
            }
        }

        Ok(())
    }

    async fn clear(&mut self, prune: bool) -> Result<(), io::Error> {
        if prune {
            AsyncKVWriteTransaction::clear(self).await?;
        } else {
            for table_name in self.table_names().await? {
                AsyncKVWriteVersionedTransaction::delete_table(self, &table_name, false).await?;
            }
        }

        Ok(())
    }

    async fn commit(self) -> Result<(), io::Error> {
        AsyncKVWriteTransaction::commit(self).await
    }

    async fn abort(self) -> Result<(), io::Error> {
        AsyncKVWriteTransaction::abort(self).await
    }
}
