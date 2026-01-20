use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, RwLock};

use fjall::{
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, OptimisticWriteTx, Readable,
    Snapshot,
};

use crate::{KVReadTransaction, KVWriteTransaction, TransactionalKVDB};

use super::{FjallDB, META_DELETED_KEYSPACE};

pub struct ReadTransaction {
    snapshot: Snapshot,
    db: OptimisticTxDatabase,
    deleted_tables: Arc<RwLock<HashSet<String>>>,
}

pub struct WriteTransaction {
    db: OptimisticTxDatabase,
    tx: OptimisticWriteTx,
    pending: HashMap<String, HashMap<String, Option<Vec<u8>>>>, // For RYOW
    tx_deleted_tables: HashSet<String>,                         // Local to this transaction
    global_deleted_tables: Arc<RwLock<HashSet<String>>>,        // Shared global state
    meta_deleted_keyspace: OptimisticTxKeyspace,
}

impl KVReadTransaction for ReadTransaction {
    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(None);
        }

        {
            let guard = self.deleted_tables.read().unwrap();
            if guard.contains(table) {
                return Ok(None);
            }
        }

        if !self.db.keyspace_exists(table) {
            return Ok(None);
        }

        let ks = self
            .db
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        Ok(self
            .snapshot
            .get(&ks, key.as_bytes())
            .map_err(io::Error::other)?
            .map(|b| b.to_vec()))
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }

        {
            let guard = self.deleted_tables.read().unwrap();
            if guard.contains(table) {
                return Ok(Vec::new());
            }
        }

        if !self.db.keyspace_exists(table) {
            return Ok(Vec::new());
        }

        let ks = self
            .db
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        let mut result = Vec::new();
        for item in self.snapshot.iter(&ks) {
            let (k, v) = item.into_inner().map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut names = self
            .db
            .list_keyspace_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        {
            let guard = self.deleted_tables.read().unwrap();
            names.retain(|n| n != META_DELETED_KEYSPACE && !guard.contains(n));
        }

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
            let guard = self.deleted_tables.read().unwrap();
            if guard.contains(table) {
                return Ok(Vec::new());
            }
        }

        if !self.db.keyspace_exists(table) {
            return Ok(Vec::new());
        }

        let ks = self
            .db
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        let mut result = Vec::new();
        for item in self.snapshot.prefix(&ks, prefix.as_bytes()) {
            let (k, v) = item.into_inner().map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }
}

impl KVReadTransaction for WriteTransaction {
    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(None);
        }

        // Check local transaction deletion
        if self.tx_deleted_tables.contains(table) {
            return Ok(None);
        }

        // Check pending changes (RYOW)
        if let Some(map) = self.pending.get(table)
            && let Some(v) = map.get(key)
        {
            return Ok(v.clone());
        }

        // Check global persistent deletion
        {
            let guard = self.global_deleted_tables.read().unwrap();
            if guard.contains(table) {
                return Ok(None);
            }
        }

        if !self.db.keyspace_exists(table) {
            return Ok(None);
        }

        let ks = self
            .db
            .keyspace(table, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        Ok(self
            .tx
            .get(&ks, key.as_bytes())
            .map_err(io::Error::other)?
            .map(|b| b.to_vec()))
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Ok(Vec::new());
        }

        let table_s = table.to_string();

        // Local deletion
        if self.tx_deleted_tables.contains(&table_s) {
            return Ok(Vec::new());
        }

        // Global persistent deletion
        {
            let guard = self.global_deleted_tables.read().unwrap();
            if guard.contains(table) {
                return Ok(Vec::new());
            }
        }

        let mut map = self.pending.get(&table_s).cloned().unwrap_or_default();

        if self.db.keyspace_exists(table) {
            let ks = self
                .db
                .keyspace(table, KeyspaceCreateOptions::default)
                .map_err(io::Error::other)?;

            for item in self.tx.iter(&ks) {
                let (k, v) = item.into_inner().map_err(io::Error::other)?;
                let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
                map.entry(k_str).or_insert(Some(v.to_vec()));
            }
        }

        let mut result = map
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect::<Vec<_>>();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut names = self
            .db
            .list_keyspace_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // Exclude global persistent deletions
        {
            let guard = self.global_deleted_tables.read().unwrap();
            names.retain(|n| n != META_DELETED_KEYSPACE && !guard.contains(n));
        }

        // Exclude local transaction deletions
        names.retain(|n| n != META_DELETED_KEYSPACE && !self.tx_deleted_tables.contains(n));

        // Add pending new tables (not yet committed)
        for table in self.pending.keys() {
            if !names.contains(table) {
                names.push(table.clone());
            }
        }

        names.sort();
        Ok(names)
    }
}

impl KVWriteTransaction for WriteTransaction {
    fn insert(
        &mut self,
        table: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot insert into meta deleted keyspace",
            ));
        }

        // If table was deleted in this transaction, "recreate" it
        if self.tx_deleted_tables.contains(table) {
            self.tx_deleted_tables.remove(table);
        }

        let old = self.get(table, key)?;

        let map = self.pending.entry(table.to_string()).or_default();
        map.insert(key.to_string(), Some(value.to_vec()));

        Ok(old)
    }

    fn remove(&mut self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot remove from meta deleted keyspace",
            ));
        }

        let table_s = table.to_string();
        if self.tx_deleted_tables.contains(&table_s) {
            return Ok(None);
        }

        let old = self.get(table, key)?;

        if old.is_none() {
            return Ok(None);
        }

        let map = self.pending.entry(table_s).or_default();
        map.insert(key.to_string(), None);

        Ok(old)
    }

    fn delete_table(&mut self, table: &str) -> Result<(), io::Error> {
        if table == META_DELETED_KEYSPACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete meta deleted keyspace",
            ));
        }

        let table_s = table.to_string();

        // Remove any pending changes
        self.pending.remove(&table_s);

        // Mark for deletion in this transaction
        self.tx_deleted_tables.insert(table_s);

        Ok(())
    }

    fn clear(&mut self) -> Result<(), io::Error> {
        let tables = self.table_names()?;
        for table in tables {
            self.delete_table(&table)?;
        }
        Ok(())
    }

    fn commit(mut self) -> Result<(), io::Error> {
        // Apply pending inserts/removes
        for (table, map) in self.pending {
            let ks = self
                .db
                .keyspace(&table, KeyspaceCreateOptions::default)
                .map_err(io::Error::other)?;

            for (key_str, v_opt) in map {
                let key_bytes = key_str.as_bytes();
                match v_opt {
                    Some(v) => self.tx.insert(&ks, key_bytes, &v),

                    None => self.tx.remove(&ks, key_bytes),
                }
            }

            // remove table from deleted meta if it was previously deleted
            let was_deleted = self.global_deleted_tables.read().unwrap().contains(&table);
            if was_deleted {
                self.tx
                    .insert(&self.meta_deleted_keyspace, table.as_bytes(), []);
                self.global_deleted_tables.write().unwrap().remove(&table);
            }
        }

        // Apply deletions (clear keyspaces marked deleted in this tx)
        for table in self.tx_deleted_tables {
            let ks = self
                .db
                .keyspace(&table, KeyspaceCreateOptions::default)
                .map_err(io::Error::other)?;

            for item in self.tx.iter(&ks) {
                let (key_bytes, _) = item.into_inner().map_err(io::Error::other)?;
                self.tx.remove(&ks, key_bytes);
            }

            // Mark as deleted in persistent meta
            self.tx
                .insert(&self.meta_deleted_keyspace, table.as_bytes(), []);

            // Update global deleted tables set
            self.global_deleted_tables.write().unwrap().insert(table);
        }

        self.tx
            .commit()
            .map_err(io::Error::other)?
            .map_err(io::Error::other)?;

        Ok(())
    }

    fn abort(self) -> Result<(), io::Error> {
        self.tx.rollback();
        Ok(())
    }
}

impl TransactionalKVDB for FjallDB {
    type ReadTransaction = ReadTransaction;
    type WriteTransaction = WriteTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error> {
        Ok(ReadTransaction {
            snapshot: self.inner.read_tx(),
            db: self.inner.clone(),
            deleted_tables: self.deleted_tables.clone(),
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error> {
        let meta_deleted_keyspace = self
            .inner
            .keyspace(META_DELETED_KEYSPACE, KeyspaceCreateOptions::default)
            .map_err(io::Error::other)?;

        Ok(WriteTransaction {
            db: self.inner.clone(),
            tx: self.inner.write_tx().map_err(io::Error::other)?,
            pending: HashMap::new(),
            tx_deleted_tables: HashSet::new(),
            global_deleted_tables: self.deleted_tables.clone(),
            meta_deleted_keyspace,
        })
    }
}
