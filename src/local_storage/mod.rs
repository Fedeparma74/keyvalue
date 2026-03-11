//! Browser [`LocalStorage`](https://developer.mozilla.org/en-US/docs/Web/API/Window/localStorage)-backed
//! key-value store (**WASM-only**).
//!
//! Entries are persisted across page reloads inside the browser's
//! `localStorage` API. Data is scoped by a *database name* passed to
//! [`LocalStorageDB::open`] and stored with the key format
//! `<db_name>/<table_name>/<key>`. This means **neither table names nor keys
//! may contain `/`**.
//!
//! ## Capacity
//!
//! Most browsers limit `localStorage` to ~5 MiB.  Values are serialised as
//! JSON arrays of bytes via `gloo-storage`, so actual usable capacity is
//! lower than the raw limit.
//!
//! ## Threading
//!
//! `LocalStorage` is only accessible from the main thread.  The struct does
//! not implement `Send`/`Sync` on non-WASM targets (the `MaybeSendSync`
//! trait relaxes these bounds on `wasm32`).

use std::{collections::HashSet, io};

#[cfg(feature = "async")]
use async_trait::async_trait;
use gloo_storage::{LocalStorage, Storage, errors::StorageError};

use crate::KeyValueDB;

/// Validates that a name does not contain `/`, which would break the `db/table/key` storage format.
fn validate_name(kind: &str, name: &str) -> Result<(), io::Error> {
    if name.contains('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{kind} must not contain '/'"),
        ));
    }
    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{kind} must not be empty"),
        ));
    }
    Ok(())
}

/// Configuration for a [`LocalStorageDB`] instance.
///
/// Use [`Default::default()`] for sensible defaults.
#[derive(Debug, Clone)]
pub struct LocalStorageConfig {
    /// Database name used as prefix for all `localStorage` keys.
    pub db_name: String,
}

impl LocalStorageConfig {
    /// Sets the database name prefix.
    #[must_use]
    pub fn db_name(mut self, name: impl Into<String>) -> Self {
        self.db_name = name.into();
        self
    }
}

/// Browser `localStorage`-backed key-value database (**WASM-only**).
///
/// Created via [`LocalStorageDB::open`].  Each instance is scoped to a
/// database name, so multiple independent stores can coexist in the same
/// origin.
#[derive(Debug)]
pub struct LocalStorageDB {
    name: String,
}

impl LocalStorageDB {
    /// Opens a `LocalStorage`-backed store with the given `db_name`.
    ///
    /// The name is used as a prefix for all `localStorage` keys.
    pub fn open(db_name: &str) -> io::Result<Self> {
        Ok(Self {
            name: db_name.to_string(),
        })
    }

    /// Opens a `LocalStorage`-backed store with custom [`LocalStorageConfig`].
    pub fn open_with_config(config: LocalStorageConfig) -> io::Result<Self> {
        Ok(Self {
            name: config.db_name,
        })
    }
}

impl KeyValueDB for LocalStorageDB {
    fn insert(&self, table_name: &str, key: &str, value: &[u8]) -> io::Result<Option<Vec<u8>>> {
        validate_name("table name", table_name)?;
        validate_name("key", key)?;
        let old_value = KeyValueDB::get(self, table_name, key)?;

        LocalStorage::set(format!("{}/{}/{}", self.name, table_name, key), value)
            .map_err(storage_error_to_io_error)?;

        Ok(old_value)
    }

    fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        validate_name("table name", table_name)?;
        validate_name("key", key)?;
        match LocalStorage::get::<Vec<u8>>(&format!("{}/{}/{}", self.name, table_name, key)) {
            Ok(value) => Ok(Some(value)),
            Err(gloo_storage::errors::StorageError::KeyNotFound(_)) => Ok(None),
            Err(e) => Err(storage_error_to_io_error(e)),
        }
    }

    fn remove(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        validate_name("table name", table_name)?;
        validate_name("key", key)?;
        if let Some(old_value) = KeyValueDB::get(self, table_name, key)? {
            LocalStorage::delete(format!("{}/{}/{}", self.name, table_name, key));

            Ok(Some(old_value))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        validate_name("table name", table_name)?;
        let prefix = format!("{}/{}/", self.name, table_name);

        let local_storage = LocalStorage::raw();
        let length = LocalStorage::length();

        let mut key_values = Vec::new();
        for i in 0..length {
            let key = local_storage
                .key(i)
                .map_err(|e| {
                    io::Error::other(format!("Failed to get key at index {}: {:?}", i, e))
                })?
                .unwrap_or_default();
            if key.starts_with(&prefix) {
                let value = LocalStorage::get::<Vec<u8>>(&key).map_err(|e| {
                    io::Error::other(format!("Failed to get value for key {}: {:?}", key, e))
                })?;
                let key = key.replacen(&format!("{}/{}/", self.name, table_name), "", 1);

                key_values.push((key, value));
            }
        }

        Ok(key_values)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let prefix = format!("{}/", self.name);

        let local_storage = LocalStorage::raw();
        let length = LocalStorage::length();

        let mut table_names = HashSet::new();
        for i in 0..length {
            let key = local_storage
                .key(i)
                .map_err(|e| {
                    io::Error::other(format!("Failed to get key at index {}: {:?}", i, e))
                })?
                .unwrap_or_default();
            if key.starts_with(&prefix) {
                let key = key.replacen(&format!("{}/", self.name), "", 1);
                let key = key.split('/').next().unwrap_or_default();

                table_names.insert(key.to_string());
            }
        }

        let mut result: Vec<String> = table_names.into_iter().collect();
        result.sort();
        Ok(result)
    }

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        validate_name("table name", table_name)?;
        let prefix = format!("{}/{}/", self.name, table_name);

        let local_storage = LocalStorage::raw();
        let length = LocalStorage::length();

        let mut keys_to_delete = Vec::new();
        for i in 0..length {
            let key = local_storage
                .key(i)
                .map_err(|e| {
                    io::Error::other(format!("Failed to get key at index {}: {:?}", i, e))
                })?
                .unwrap_or_default();
            if key.starts_with(&prefix) {
                keys_to_delete.push(key);
            }
        }

        for key in keys_to_delete {
            LocalStorage::delete(key);
        }

        Ok(())
    }

    fn clear(&self) -> io::Result<()> {
        // Only delete keys belonging to this database, not all of localStorage
        let prefix = format!("{}/", self.name);

        let local_storage = LocalStorage::raw();
        let length = LocalStorage::length();

        let mut keys_to_delete = Vec::new();
        for i in 0..length {
            let key = local_storage
                .key(i)
                .map_err(|e| {
                    io::Error::other(format!("Failed to get key at index {}: {:?}", i, e))
                })?
                .unwrap_or_default();
            if key.starts_with(&prefix) {
                keys_to_delete.push(key);
            }
        }

        for key in keys_to_delete {
            LocalStorage::delete(key);
        }

        Ok(())
    }
}

#[cfg(feature = "async")]
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl crate::AsyncKeyValueDB for LocalStorageDB {
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

fn storage_error_to_io_error(e: StorageError) -> io::Error {
    match e {
        StorageError::KeyNotFound(key) => io::Error::new(io::ErrorKind::NotFound, key),
        StorageError::SerdeError(e) => {
            if let Some(e) = e.io_error_kind() {
                io::Error::new(e, e.to_string())
            } else if e.is_syntax() {
                io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
            } else if e.is_data() {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            } else if e.is_eof() {
                io::Error::new(io::ErrorKind::UnexpectedEof, e.to_string())
            } else {
                io::Error::other(e.to_string())
            }
        }
        StorageError::JsError(e) => io::Error::other(e),
    }
}
