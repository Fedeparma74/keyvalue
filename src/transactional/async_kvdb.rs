use crate::{io, AsyncKeyValueDB};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use async_trait::async_trait;

use super::{KVReadTransaction, KVWriteTransaction, TransactionalKVDB};

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncTransactionalKVDB: Send + Sync + 'static {
    type ReadTransaction: AsyncKVReadTransaction;
    type WriteTransaction: AsyncKVWriteTransaction;

    async fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error>;
    async fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error>;
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVReadTransaction: Send + Sync + 'static {
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;
    async fn table_names(&self) -> Result<Vec<String>, io::Error>;

    #[allow(clippy::type_complexity)]
    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
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
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name).await? {
            values.push(value);
        }
        Ok(values)
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVWriteTransaction: AsyncKVReadTransaction {
    async fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;
    async fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn commit(self) -> Result<(), io::Error>;
    async fn abort(self) -> Result<(), io::Error>;

    async fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name).await? {
            self.remove(table_name, &key).await?;
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
impl<T: AsyncTransactionalKVDB> AsyncKeyValueDB for T {
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let mut write_transaction = self.begin_write().await?;
        match write_transaction.insert(table_name, key, value).await {
            Ok(old_value) => {
                write_transaction.commit().await?;
                Ok(old_value)
            }
            Err(e) => {
                write_transaction.abort().await?;
                Err(e)
            }
        }
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.get(table_name, key).await
    }

    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let mut write_transaction = self.begin_write().await?;
        match write_transaction.remove(table_name, key).await {
            // Ok(Some(old_value)) => {
            //     write_transaction.commit().await?;
            //     Ok(Some(old_value))
            // }
            // Ok(None) => {
            //     write_transaction.abort().await?;
            //     Ok(None)
            // }
            Ok(old_value) => {
                write_transaction.commit().await?;
                Ok(old_value)
            }
            Err(e) => {
                write_transaction.abort().await?;
                Err(e)
            }
        }
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.iter(table_name).await
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.table_names().await
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.iter_from_prefix(table_name, prefix).await
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.contains_table(table_name).await
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.contains_key(table_name, key).await
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.keys(table_name).await
    }

    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let read_transaction = self.begin_read().await?;
        read_transaction.values(table_name).await
    }

    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        let mut write_transaction = self.begin_write().await?;
        match write_transaction.delete_table(table_name).await {
            Ok(()) => {
                write_transaction.commit().await?;
                Ok(())
            }
            Err(e) => {
                write_transaction.abort().await?;
                Err(e)
            }
        }
    }

    async fn clear(&self) -> Result<(), io::Error> {
        let mut write_transaction = self.begin_write().await?;
        match write_transaction.clear().await {
            Ok(()) => {
                write_transaction.commit().await?;
                Ok(())
            }
            Err(e) => {
                write_transaction.abort().await?;
                Err(e)
            }
        }
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T> AsyncKVReadTransaction for T
where
    T: KVReadTransaction,
{
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KVReadTransaction::get(self, table_name, key)
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KVReadTransaction::iter(self, table_name)
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KVReadTransaction::table_names(self)
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KVReadTransaction::iter_from_prefix(self, table_name, prefix)
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        KVReadTransaction::contains_table(self, table_name)
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KVReadTransaction::contains_key(self, table_name, key)
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KVReadTransaction::keys(self, table_name)
    }

    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        KVReadTransaction::values(self, table_name)
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T> AsyncKVWriteTransaction for T
where
    T: KVWriteTransaction,
{
    async fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        KVWriteTransaction::insert(self, table_name, key, value)
    }

    async fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KVWriteTransaction::remove(self, table_name, key)
    }

    async fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        KVWriteTransaction::delete_table(self, table_name)
    }

    async fn clear(&mut self) -> Result<(), io::Error> {
        KVWriteTransaction::clear(self)
    }

    async fn commit(self) -> Result<(), io::Error> {
        KVWriteTransaction::commit(self)
    }

    async fn abort(self) -> Result<(), io::Error> {
        KVWriteTransaction::abort(self)
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T: TransactionalKVDB> AsyncTransactionalKVDB for T {
    type ReadTransaction = T::ReadTransaction;
    type WriteTransaction = T::WriteTransaction;

    async fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error> {
        TransactionalKVDB::begin_read(self)
    }

    async fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error> {
        TransactionalKVDB::begin_write(self)
    }
}
