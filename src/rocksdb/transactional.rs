use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

use rust_rocksdb::{Direction, IteratorMode, Options, SnapshotWithThreadMode, WriteBatch};

use crate::{KVReadTransaction, KVWriteTransaction, TransactionalKVDB};

use super::{DEFAULT_CF, Rocks, RocksDB};

type Snapshot<'a> = SnapshotWithThreadMode<'a, Rocks>;

pub struct ReadTransaction<'a> {
    db: Arc<Rocks>,
    snapshot: Snapshot<'a>,
    path: String,
    opts: Arc<Options>,
}

pub struct WriteTransaction<'a> {
    db: Arc<Rocks>,
    snapshot: Snapshot<'a>,
    pending: HashMap<String, HashMap<String, Option<Vec<u8>>>>, // For RYOW
    tx_deleted_tables: HashSet<String>,                         // Local to this transaction
    path: String,
    opts: Arc<Options>,
}

impl<'a> KVReadTransaction<'a> for ReadTransaction<'a> {
    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let cf = match self.db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        self.snapshot
            .get_cf(&cf, key.as_bytes())
            .map_err(io::Error::other)?
            .map(Ok)
            .transpose()
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let cf = match self.db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let mut result = Vec::new();
        let iter = self.snapshot.iterator_cf(&cf, IteratorMode::Start);
        for item in iter {
            let (k, v) = item.map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut names = Rocks::list_cf(&self.opts, &self.path)
            .map_err(io::Error::other)?
            .into_iter()
            .filter(|n| n != DEFAULT_CF)
            .collect::<Vec<_>>();

        names.sort();
        Ok(names)
    }

    fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let cf = match self.db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let prefix_b = prefix.as_bytes();
        let iter = self
            .snapshot
            .iterator_cf(&cf, IteratorMode::From(prefix_b, Direction::Forward));

        let mut result = Vec::new();
        for item in iter {
            let (k, v) = item.map_err(io::Error::other)?;
            if !k.starts_with(prefix_b) {
                break;
            }
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }
}

impl<'a> KVReadTransaction<'a> for WriteTransaction<'a> {
    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
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

        let cf = match self.db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        self.snapshot
            .get_cf(&cf, key.as_bytes())
            .map_err(io::Error::other)?
            .map(Ok)
            .transpose()
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let table_s = table.to_string();

        // Local deletion
        if self.tx_deleted_tables.contains(&table_s) {
            return Ok(Vec::new());
        }

        let mut map = self.pending.get(&table_s).cloned().unwrap_or_default();

        if let Some(cf) = self.db.cf_handle(table) {
            let iter = self.snapshot.iterator_cf(&cf, IteratorMode::Start);
            for item in iter {
                let (k, v) = item.map_err(io::Error::other)?;
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
        let mut names = Rocks::list_cf(&self.opts, &self.path)
            .map_err(io::Error::other)?
            .into_iter()
            .filter(|n| n != DEFAULT_CF)
            .collect::<Vec<_>>();

        // Exclude local transaction deletions
        names.retain(|n| !self.tx_deleted_tables.contains(n));

        // Add pending new tables (not yet committed)
        for table in self.pending.keys() {
            if !names.contains(table) {
                names.push(table.clone());
            }
        }

        names.sort();
        Ok(names)
    }

    fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let table_s = table.to_string();

        // Local deletion
        if self.tx_deleted_tables.contains(&table_s) {
            return Ok(Vec::new());
        }

        let mut map = HashMap::new();

        if let Some(cf) = self.db.cf_handle(table) {
            let prefix_b = prefix.as_bytes();
            let iter = self
                .snapshot
                .iterator_cf(&cf, IteratorMode::From(prefix_b, Direction::Forward));
            for item in iter {
                let (k, v) = item.map_err(io::Error::other)?;
                if !k.starts_with(prefix_b) {
                    break;
                }
                let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
                map.insert(k_str, Some(v.to_vec()));
            }
        }

        if let Some(pend) = self.pending.get(&table_s) {
            for (k, v) in pend {
                if k.starts_with(prefix) {
                    map.insert(k.clone(), v.clone());
                }
            }
        }

        let mut result = map
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect::<Vec<_>>();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }
}

impl<'a> KVWriteTransaction<'a> for WriteTransaction<'a> {
    fn insert(
        &mut self,
        table: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
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
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
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
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete default column family",
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

    fn commit(self) -> Result<(), io::Error> {
        let db = &*self.db;
        let mut batch = WriteBatch::default();

        // Apply pending inserts/removes (create CFs first if needed)
        for (table, map) in &self.pending {
            let cf = if let Some(cf) = db.cf_handle(table) {
                cf
            } else {
                db.create_cf(table, &self.opts).map_err(io::Error::other)?;
                db.cf_handle(table).ok_or(io::Error::other(
                    "Failed to get column family after creation",
                ))?
            };
            for (key_str, v_opt) in map {
                let key_bytes = key_str.as_bytes();
                match v_opt {
                    Some(v) => batch.put_cf(&cf, key_bytes, v),
                    None => batch.delete_cf(&cf, key_bytes),
                }
            }
        }

        db.write(&batch).map_err(io::Error::other)?;

        // Apply deletions by dropping CFs
        for table in self.tx_deleted_tables {
            if db.cf_handle(&table).is_some() {
                db.drop_cf(&table).map_err(io::Error::other)?;
            }
        }

        Ok(())
    }

    fn abort(self) -> Result<(), io::Error> {
        Ok(())
    }
}

impl TransactionalKVDB for RocksDB {
    type ReadTransaction<'a> = ReadTransaction<'a>;
    type WriteTransaction<'a> = WriteTransaction<'a>;

    fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        // Note: Snapshot lifetime is 'static as it's bound to the DB
        Ok(ReadTransaction {
            db: self.inner.clone(),
            snapshot: Snapshot::new(&self.inner),
            path: self.path.clone(),
            opts: self.opts.clone(),
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        Ok(WriteTransaction {
            db: self.inner.clone(),
            snapshot: Snapshot::new(&self.inner),
            pending: HashMap::new(),
            tx_deleted_tables: HashSet::new(),
            path: self.path.clone(),
            opts: self.opts.clone(),
        })
    }
}
