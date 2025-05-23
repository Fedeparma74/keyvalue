use std::{collections::HashSet, io};

use gloo_storage::{LocalStorage, Storage, errors::StorageError};

use crate::KeyValueDB;

#[derive(Debug)]
pub struct LocalStorageDB {
    name: String,
}

impl LocalStorageDB {
    pub fn open(db_name: &str) -> io::Result<Self> {
        Ok(Self {
            name: db_name.to_string(),
        })
    }
}

impl KeyValueDB for LocalStorageDB {
    fn insert(&self, table_name: &str, key: &str, value: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let old_value = KeyValueDB::get(self, table_name, key)?;

        LocalStorage::set(format!("{}/{}/{}", self.name, table_name, key), value)
            .map_err(storage_error_to_io_error)?;

        Ok(old_value)
    }

    fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        match LocalStorage::get::<Vec<u8>>(&format!("{}/{}/{}", self.name, table_name, key)) {
            Ok(value) => Ok(Some(value)),
            Err(gloo_storage::errors::StorageError::KeyNotFound(_)) => Ok(None),
            Err(e) => Err(storage_error_to_io_error(e)),
        }
    }

    fn remove(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        if let Some(old_value) = KeyValueDB::get(self, table_name, key)? {
            LocalStorage::delete(format!("{}/{}/{}", self.name, table_name, key));

            Ok(Some(old_value))
        } else {
            Ok(None)
        }
    }

    fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
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

        Ok(table_names.into_iter().collect())
    }

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        let prefix = format!("{}/{}", self.name, table_name);

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
        LocalStorage::clear();

        Ok(())
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
