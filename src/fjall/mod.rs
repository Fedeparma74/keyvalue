use std::{
    collections::HashSet,
    io,
    path::Path,
    sync::{Arc, RwLock},
};

use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};

use crate::KeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

const META_DELETED_KEYSPACE: &str = "_meta_deleted";

#[derive(Clone)]
pub struct FjallDB {
    inner: OptimisticTxDatabase,
    deleted_tables: Arc<RwLock<HashSet<String>>>,
}

impl FjallDB {
    pub fn open(path: &Path) -> io::Result<Self> {
        let inner = OptimisticTxDatabase::builder(path)
            .open()
            .map_err(io::Error::other)?;

        let deleted = Arc::new(RwLock::new(HashSet::new()));

        // Load persisted deleted tables
        if inner.keyspace_exists(META_DELETED_KEYSPACE) {
            let meta_ks = inner
                .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
                .map_err(io::Error::other)?;

            let snapshot = inner.read_tx();
            for item in snapshot.iter(&meta_ks) {
                let (key_bytes, _) = item.into_inner().map_err(io::Error::other)?;
                let table_name = String::from_utf8(key_bytes.to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let mut deleted_tables = deleted.write().unwrap();
                deleted_tables.insert(table_name);
            }
        }

        Ok(Self {
            inner,
            deleted_tables: deleted,
        })
    }

    /// Persist a deleted table name (insert key with empty value)
    fn mark_deleted(&self, table: &str) -> io::Result<()> {
        let meta_ks = self
            .inner
            .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        // Insert table name as key with empty value
        meta_ks
            .insert(table.as_bytes(), [])
            .map_err(io::Error::other)?;

        // Also update in-memory set
        {
            let mut deleted_tables = self.deleted_tables.write().unwrap();
            deleted_tables.insert(table.to_string());
        }

        Ok(())
    }

    /// Remove all keys from the given keyspace
    fn clear_keyspace(&self, table: &str) -> io::Result<()> {
        if !self.inner.keyspace_exists(table) {
            return Ok(());
        }

        let ks = self
            .inner
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        // Use a write tx to remove all keys
        let mut tx = self.inner.write_tx().map_err(io::Error::other)?;

        for item in tx.iter(&ks) {
            let (key_bytes, _) = item.into_inner().map_err(io::Error::other)?;
            tx.remove(&ks, key_bytes);
        }

        tx.commit()
            .map_err(io::Error::other)?
            .map_err(io::Error::other)?;

        Ok(())
    }
}

impl KeyValueDB for FjallDB {
    fn insert(&self, table: &str, key: &str, value: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot insert into meta deleted keyspace",
            ));
        }

        let table_str = table.to_string();

        // Check if this table was previously deleted
        let was_deleted = {
            let guard = self.deleted_tables.read().unwrap();
            guard.contains(&table_str)
        };

        if was_deleted {
            // "Recreate" the table: remove from deleted list (both memory and persistent meta)
            {
                let mut guard = self.deleted_tables.write().unwrap();
                guard.remove(&table_str);
            }

            // Remove the deletion marker from _meta_deleted
            let meta_ks = self
                .inner
                .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
                .map_err(io::Error::other)?;

            meta_ks.remove(table.as_bytes()).map_err(io::Error::other)?;
        }

        // Now proceed with the insert (keyspace will be created/opened automatically)
        let ks = self
            .inner
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        let key_bytes = key.as_bytes();
        let old = ks
            .get(key_bytes)
            .map_err(io::Error::other)?
            .map(|v| v.to_vec());
        ks.insert(key_bytes, value).map_err(io::Error::other)?;

        Ok(old)
    }
    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(None);
        }

        {
            let deleted_tables = self.deleted_tables.read().unwrap();
            if deleted_tables.contains(table) {
                return Ok(None);
            }
        }

        if !self.inner.keyspace_exists(table) {
            return Ok(None);
        }

        let ks = self
            .inner
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        Ok(ks
            .get(key.as_bytes())
            .map_err(io::Error::other)?
            .map(|v| v.to_vec()))
    }

    fn remove(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(None);
        }

        {
            let deleted_tables = self.deleted_tables.read().unwrap();
            if deleted_tables.contains(table) {
                return Ok(None);
            }
        }

        if !self.inner.keyspace_exists(table) {
            return Ok(None);
        }

        let ks = self
            .inner
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        let key_bytes = key.as_bytes();
        let old = ks
            .get(key_bytes)
            .map_err(io::Error::other)?
            .map(|v| v.to_vec());
        ks.remove(key_bytes).map_err(io::Error::other)?;
        Ok(old)
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }

        {
            let deleted_tables = self.deleted_tables.read().unwrap();
            if deleted_tables.contains(table) {
                return Ok(Vec::new());
            }
        }

        if !self.inner.keyspace_exists(table) {
            return Ok(Vec::new());
        }

        let ks = self
            .inner
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        let mut result = Vec::new();
        let snapshot = self.inner.read_tx();
        for item in snapshot.iter(&ks) {
            let (k, v) = item.into_inner().map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        // Exclude internal meta and deleted tables
        let deleted_tables = self.deleted_tables.read().unwrap();
        let names = self
            .inner
            .list_keyspace_names()
            .iter()
            .filter_map(|n| {
                if n.as_ref() != META_DELETED_KEYSPACE && !deleted_tables.contains(n.as_ref()) {
                    Some(n.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<String>>();

        Ok(names)
    }

    fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }

        {
            let deleted_tables = self.deleted_tables.read().unwrap();
            if deleted_tables.contains(table) {
                return Ok(Vec::new());
            }
        }

        if !self.inner.keyspace_exists(table) {
            return Ok(Vec::new());
        }

        let ks = self
            .inner
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        let mut result = Vec::new();
        let snapshot = self.inner.read_tx();
        for item in snapshot.prefix(&ks, prefix.as_bytes()) {
            let (k, v) = item.into_inner().map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn contains_table(&self, table: &str) -> Result<bool, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(false);
        }

        let deleted_tables = self.deleted_tables.read().unwrap();
        if deleted_tables.contains(table) {
            return Ok(false);
        }
        Ok(self.inner.keyspace_exists(table))
    }

    fn delete_table(&self, table: &str) -> Result<(), io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete meta deleted keyspace",
            ));
        }

        // Skip if already deleted
        {
            let deleted_tables = self.deleted_tables.read().unwrap();
            if deleted_tables.contains(table) {
                return Ok(());
            }
        }

        // 1. Clear all data in the keyspace
        self.clear_keyspace(table)?;

        // 2. Mark as deleted (add key with empty value to meta)
        self.mark_deleted(table)?;

        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        let current_tables = self.table_names()?;
        for table in current_tables {
            self.delete_table(&table)?;
        }
        Ok(())
    }
}
