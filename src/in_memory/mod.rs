//! In-memory key-value store backed by [`DashMap`].
//!
//! This backend is useful for testing, caching, and any scenario where
//! persistence is not required. Data is lost when the [`InMemoryDB`] instance
//! is dropped.
//!
//! The store uses [`DashMap`] for concurrent, lock-free access at the table
//! level, making it safe to share across threads (or tasks on WASM) without
//! a global lock. Clone is cheap (`Arc`-backed).
//!
//! ## WASM compatibility
//!
//! `InMemoryDB` compiles and works on `wasm32-unknown-unknown`. On WASM there
//! is no true multi-threading (unless shared-memory threads are enabled), so
//! the `DashMap` sharding adds negligible overhead while keeping the API
//! identical across targets.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

#[cfg(feature = "async")]
use async_trait::async_trait;
use dashmap::DashMap;

use crate::KeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

/// An in-memory key-value database using [`DashMap<String, HashMap<String, Vec<u8>>>`].
///
/// The outer `DashMap` maps table names to their contents; each inner
/// `HashMap` stores the key-value pairs within that table. Because `DashMap`
/// uses fine-grained, per-shard locking, operations on **different tables**
/// can proceed fully in parallel without contention.
///
/// # Examples
///
/// ```
/// use keyvalue::{KeyValueDB, in_memory::InMemoryDB};
///
/// let db = InMemoryDB::new();
/// db.insert("table", "key", b"value").unwrap();
/// assert_eq!(db.get("table", "key").unwrap(), Some(b"value".to_vec()));
/// ```
#[derive(Debug, Clone, Default)]
pub struct InMemoryDB {
    map: Arc<DashMap<String, HashMap<String, Vec<u8>>>>,
}

impl InMemoryDB {
    /// Creates a new, empty in-memory database.
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
        }
    }
}

#[cfg(feature = "async")]
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl crate::AsyncKeyValueDB for InMemoryDB {
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::insert(self, table_name, key, value)
    }
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::get(self, table_name, key)
    }
    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::remove(self, table_name, key)
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KeyValueDB::iter(self, table_name)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KeyValueDB::table_names(self)
    }
    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KeyValueDB::iter_from_prefix(self, table_name, prefix)
    }
    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_table(self, table_name)
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_key(self, table_name, key)
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KeyValueDB::keys(self, table_name)
    }
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        KeyValueDB::values(self, table_name)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        KeyValueDB::delete_table(self, table_name)
    }
    async fn clear(&self) -> Result<(), io::Error> {
        KeyValueDB::clear(self)
    }
}

impl KeyValueDB for InMemoryDB {
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .entry(table_name.to_owned())
            .or_default()
            .insert(key.to_owned(), value.to_owned()))
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .get(table_name)
            .and_then(|table| table.get(key).cloned()))
    }

    fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .get_mut(table_name)
            .and_then(|mut table| table.remove(key)))
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        Ok(self
            .map
            .get(table_name)
            .map(|table| {
                table
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        Ok(self.map.iter().map(|r| r.key().clone()).collect())
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        Ok(self
            .map
            .get(table_name)
            .map(|table| {
                table
                    .iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.map.contains_key(table_name))
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self
            .map
            .get(table_name)
            .map(|table| table.contains_key(key))
            .unwrap_or(false))
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self
            .map
            .get(table_name)
            .map(|table| table.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .get(table_name)
            .map(|table| table.values().cloned().collect())
            .unwrap_or_default())
    }

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.map.remove(table_name);
        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        self.map.clear();
        Ok(())
    }
}
