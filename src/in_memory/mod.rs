//! In-memory key-value store.
//!
//! Useful for testing, caching, and any scenario where persistence is not
//! required. Data is lost when the [`InMemoryDB`] instance is dropped.
//!
//! The backing store is `Arc<RwLock<BTreeMap<table, BTreeMap<key, value>>>>`:
//! a single global lock provides atomicity for snapshots and commits, while
//! the inner `BTreeMap` keeps entries sorted so that range scans are native
//! (O(range + limit)) instead of loading and re-sorting the whole table.
//!
//! ## WASM compatibility
//!
//! `InMemoryDB` compiles and works on `wasm32-unknown-unknown`.  On WASM
//! there is no true multi-threading (unless shared-memory threads are
//! enabled), so the lock is effectively free.

use std::collections::BTreeMap;
use std::io;
use std::ops::Bound as StdBound;
use std::sync::{Arc, RwLock};

#[cfg(feature = "async")]
use async_trait::async_trait;

use crate::{Bound, Direction, KeyRange, KeyValueDB};

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

pub(crate) type Table = BTreeMap<String, Vec<u8>>;
pub(crate) type Store = BTreeMap<String, Table>;

pub(crate) fn lock_poisoned() -> io::Error {
    io::Error::other("InMemoryDB RwLock poisoned")
}

/// Produce the `std::ops::Bound` tuple corresponding to a [`KeyRange`].
pub(crate) fn std_range_bounds(range: &KeyRange) -> (StdBound<String>, StdBound<String>) {
    let lower = match &range.lower {
        Bound::Unbounded => StdBound::Unbounded,
        Bound::Included(k) => StdBound::Included(k.clone()),
        Bound::Excluded(k) => StdBound::Excluded(k.clone()),
    };
    let upper = match &range.upper {
        Bound::Unbounded => StdBound::Unbounded,
        Bound::Included(k) => StdBound::Included(k.clone()),
        Bound::Excluded(k) => StdBound::Excluded(k.clone()),
    };
    (lower, upper)
}

/// Walks a sorted [`Table`] yielding entries whose key starts with
/// `prefix`, using the BTreeMap's natural ordering so iteration stops at
/// the first non-match.
pub(crate) fn collect_prefix(table: &Table, prefix: &str) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let start: StdBound<&str> = StdBound::Included(prefix);
    let end: StdBound<&str> = StdBound::Unbounded;
    for (k, v) in table.range::<str, _>((start, end)) {
        if !k.starts_with(prefix) {
            break;
        }
        out.push((k.clone(), v.clone()));
    }
    out
}

/// Walks a sorted [`Table`] honouring a [`KeyRange`] (bounds, prefix,
/// direction, limit).  Shared between the non-transactional and
/// transactional in-memory backends.
pub(crate) fn collect_range(table: &Table, range: &KeyRange) -> Vec<(String, Vec<u8>)> {
    let bounds = std_range_bounds(range);
    let limit = range.limit.unwrap_or(usize::MAX);
    let mut out: Vec<(String, Vec<u8>)> = Vec::new();

    let push = |k: &String, v: &Vec<u8>, out: &mut Vec<(String, Vec<u8>)>| -> bool {
        if range.is_beyond_far_end(k) {
            return false;
        }
        if !range.matches_prefix(k) {
            return true;
        }
        out.push((k.clone(), v.clone()));
        out.len() < limit
    };

    match range.direction {
        Direction::Forward => {
            for (k, v) in table.range(bounds) {
                if !push(k, v, &mut out) {
                    break;
                }
            }
        }
        Direction::Reverse => {
            for (k, v) in table.range(bounds).rev() {
                if !push(k, v, &mut out) {
                    break;
                }
            }
        }
    }

    out
}

/// An in-memory key-value database.
///
/// Cloning is cheap (the handle is refcounted) and all clones share the
/// same backing store.
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
    pub(crate) map: Arc<RwLock<Store>>,
}

/// Configuration for an [`InMemoryDB`] instance.
#[derive(Debug, Clone, Default)]
pub struct InMemoryConfig {
    /// Initial outer-map capacity hint.  Currently unused by the BTreeMap
    /// backing store; retained for API compatibility with earlier versions
    /// and as a forward-compatibility knob.
    pub initial_capacity: Option<usize>,
}

impl InMemoryConfig {
    /// Sets the initial capacity hint (no-op for the BTreeMap backing
    /// store; retained for compatibility).
    #[must_use]
    pub fn initial_capacity(mut self, n: usize) -> Self {
        self.initial_capacity = Some(n);
        self
    }
}

impl InMemoryDB {
    /// Creates a new, empty in-memory database.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new in-memory database with the given config (currently
    /// equivalent to [`InMemoryDB::new`]).
    pub fn new_with_config(_config: InMemoryConfig) -> Self {
        Self::new()
    }

    pub(crate) fn read(&self) -> io::Result<std::sync::RwLockReadGuard<'_, Store>> {
        self.map.read().map_err(|_| lock_poisoned())
    }

    pub(crate) fn write(&self) -> io::Result<std::sync::RwLockWriteGuard<'_, Store>> {
        self.map.write().map_err(|_| lock_poisoned())
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
        let mut store = self.write()?;
        Ok(store
            .entry(table_name.to_owned())
            .or_default()
            .insert(key.to_owned(), value.to_owned()))
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let store = self.read()?;
        Ok(store.get(table_name).and_then(|t| t.get(key).cloned()))
    }

    fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let mut store = self.write()?;
        Ok(store.get_mut(table_name).and_then(|t| t.remove(key)))
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let store = self.read()?;
        Ok(store
            .get(table_name)
            .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default())
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        Ok(self.read()?.keys().cloned().collect())
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let store = self.read()?;
        let Some(table) = store.get(table_name) else {
            return Ok(Vec::new());
        };
        Ok(collect_prefix(table, prefix))
    }

    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let store = self.read()?;
        match store.get(table_name) {
            Some(table) => Ok(collect_range(table, &range)),
            None => Ok(Vec::new()),
        }
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.read()?.contains_key(table_name))
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self
            .read()?
            .get(table_name)
            .is_some_and(|t| t.contains_key(key)))
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self
            .read()?
            .get(table_name)
            .map(|t| t.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .read()?
            .get(table_name)
            .map(|t| t.values().cloned().collect())
            .unwrap_or_default())
    }

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.write()?.remove(table_name);
        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        self.write()?.clear();
        Ok(())
    }
}
