use crate::{KeyValueDB, MaybeSendSync, io};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

use async_trait::async_trait;

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKeyValueDB: MaybeSendSync + 'static {
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;
    async fn table_names(&self) -> Result<Vec<String>, io::Error>;

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
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name).await? {
            self.remove(table_name, &key).await?;
        }
        Ok(())
    }
    async fn clear(&self) -> Result<(), io::Error> {
        for table_name in self.table_names().await? {
            self.delete_table(&table_name).await?;
        }
        Ok(())
    }
}

#[cfg(feature = "tokio")]
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T: KeyValueDB> AsyncKeyValueDB for T {
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::insert(self, table_name, key, value))
    }
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::get(self, table_name, key))
    }
    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::remove(self, table_name, key))
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::iter(self, table_name))
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::table_names(self))
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::iter_from_prefix(self, table_name, prefix))
    }
    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::contains_table(self, table_name))
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::contains_key(self, table_name, key))
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::keys(self, table_name))
    }
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::values(self, table_name))
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::delete_table(self, table_name))
    }
    async fn clear(&self) -> Result<(), io::Error> {
        tokio::task::block_in_place(|| KeyValueDB::clear(self))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn AsyncKeyValueDB>> = None;
    }
}
