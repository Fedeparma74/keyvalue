use crate::{Direction, KeyRange, MaybeSendSync, apply_range_in_memory, io};
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

    /// Async counterpart of [`crate::KeyValueDB::iter_range`].
    ///
    /// See the sync trait for semantics.  The default in-memory fallback
    /// loads the full table or prefix and is O(N); every shipped backend
    /// overrides this with a native range-scan.
    async fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let items = match &range.prefix {
            Some(p) => self.iter_from_prefix(table_name, p.as_str()).await?,
            None => self.iter(table_name).await?,
        };
        Ok(apply_range_in_memory(items, &range))
    }

    /// Cursor-based pagination over the full table.
    async fn iter_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut range = KeyRange::all().with_direction(direction).with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        self.iter_range(table_name, range).await
    }

    /// Cursor-based pagination restricted to a prefix.
    async fn iter_from_prefix_paginated(
        &self,
        table_name: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut range = KeyRange::prefix(prefix)
            .with_direction(direction)
            .with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        self.iter_range(table_name, range).await
    }

    /// Cursor-based pagination returning only keys.
    async fn keys_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<String>, io::Error> {
        Ok(self
            .iter_paginated(table_name, start_after, limit, direction)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    /// Cursor-based pagination returning only values.
    async fn values_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .iter_paginated(table_name, start_after, limit, direction)
            .await?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
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
