use crate::{MaybeSendSync, io};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

use async_trait::async_trait;

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
/// Asynchronous counterpart of [`crate::KeyValueDB`].
///
/// Provides the same table-oriented key-value semantics but with `async` methods.
/// On native targets with `std` the trait requires `Send`; on `wasm32` or
/// `no_std` builds the `Send` bound is relaxed.
///
/// Backends that are natively synchronous (redb, fjall, RocksDB) get an async
/// implementation for free via the `impl_async_kvdb_via_spawn_blocking!` macro,
/// which delegates to [`tokio::task::spawn_blocking`]. Natively async backends
/// (SQLite/Turso, AWS S3, IndexedDB) implement this trait directly.
pub trait AsyncKeyValueDB: MaybeSendSync + 'static {
    /// Inserts a key-value pair. Returns the previous value, if any.
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
        Ok(self
            .iter(table_name)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .iter(table_name)
            .await?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn AsyncKeyValueDB>> = None;
    }
}
