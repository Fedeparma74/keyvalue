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
    type ReadTransaction: AsyncKVReadVersionedTransaction;
    type WriteTransaction: AsyncKVWriteVersionedTransaction;

    async fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error>;
    async fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error>;
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVReadVersionedTransaction: MaybeSendSync {
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
pub trait AsyncKVWriteVersionedTransaction: AsyncKVReadVersionedTransaction {
    async fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    async fn remove(
        &mut self,
        table_name: &str,
        key: &str,
        version: u64,
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
            None => self.remove(table_name, key, new_version).await,
        }
    }

    async fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name).await? {
            self.update(table_name, &key, None).await?;
        }
        Ok(())
    }
    async fn clear(&mut self) -> Result<(), io::Error> {
        for table_name in self.table_names().await? {
            self.delete_table(&table_name).await?;
        }
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T: AsyncTransactionalKVDB> AsyncVersionedTransactionalKVDB for T {
    type ReadTransaction = T::ReadTransaction;
    type WriteTransaction = T::WriteTransaction;
    async fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error> {
        AsyncTransactionalKVDB::begin_read(self).await
    }

    async fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error> {
        AsyncTransactionalKVDB::begin_write(self).await
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T> AsyncKVReadVersionedTransaction for T
where
    T: AsyncKVReadTransaction,
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
impl<T> AsyncKVWriteVersionedTransaction for T
where
    T: AsyncKVWriteTransaction,
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
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let old_value = AsyncKVWriteTransaction::remove(self, table_name, key).await?;
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
    }

    async fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        AsyncKVWriteTransaction::delete_table(self, table_name).await
    }

    async fn clear(&mut self) -> Result<(), io::Error> {
        AsyncKVWriteTransaction::clear(self).await
    }

    async fn commit(self) -> Result<(), io::Error> {
        AsyncKVWriteTransaction::commit(self).await
    }

    async fn abort(self) -> Result<(), io::Error> {
        AsyncKVWriteTransaction::abort(self).await
    }
}
