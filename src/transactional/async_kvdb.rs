use crate::{MaybeSendSync, io};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use async_trait::async_trait;

use super::{KVReadTransaction, KVWriteTransaction, TransactionalKVDB};

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncTransactionalKVDB: MaybeSendSync + 'static {
    type ReadTransaction<'a>: AsyncKVReadTransaction<'a>;
    type WriteTransaction<'a>: AsyncKVWriteTransaction<'a>;

    async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error>;
    async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error>;
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVReadTransaction<'a>: MaybeSendSync {
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
pub trait AsyncKVWriteTransaction<'a>: AsyncKVReadTransaction<'a> {
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
        for table in self.table_names().await? {
            self.delete_table(&table).await?;
        }
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a, T> AsyncKVReadTransaction<'a> for T
where
    T: KVReadTransaction<'a>,
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
impl<'a, T> AsyncKVWriteTransaction<'a> for T
where
    T: KVWriteTransaction<'a>,
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
    type ReadTransaction<'a> = T::ReadTransaction<'a>;
    type WriteTransaction<'a> = T::WriteTransaction<'a>;

    async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        TransactionalKVDB::begin_read(self)
    }

    async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        TransactionalKVDB::begin_write(self)
    }
}
