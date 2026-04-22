use std::collections::{HashMap, HashSet};
use std::io;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use rust_rocksdb::{Direction, IteratorMode, Options, SnapshotWithThreadMode, WriteBatch};

use crate::{KVReadTransaction, KVWriteTransaction, TransactionalKVDB};

use super::{DEFAULT_CF, Rocks, RocksDB, collect_rocks_range, rocks_iterator_mode};

// ---------------------------------------------------------------------------
// Owned snapshot helper
// ---------------------------------------------------------------------------
// `SnapshotWithThreadMode<'a, Rocks>` borrows from the DB. We co-locate
// it with the `Arc<Rocks>` that owns the DB so the snapshot is always valid.
// `ManuallyDrop` + a custom `Drop` ensure the snapshot is released before
// the `Arc` refcount is decremented.

fn new_owned_snapshot(db: &Arc<Rocks>) -> ManuallyDrop<SnapshotWithThreadMode<'static, Rocks>> {
    let snapshot = SnapshotWithThreadMode::new(db.as_ref());
    // SAFETY: The snapshot internally stores a raw pointer obtained from
    // the DB.  We hold an `Arc<Rocks>` in the same struct, guaranteeing
    // the DB outlives the snapshot (enforced by our `Drop` impl).
    let snapshot: SnapshotWithThreadMode<'static, Rocks> = unsafe { std::mem::transmute(snapshot) };
    ManuallyDrop::new(snapshot)
}

// ---------------------------------------------------------------------------
// Transaction types (owned – no lifetime parameter)
// ---------------------------------------------------------------------------

/// Read-only snapshot of a RocksDB database.
///
/// The underlying [`SnapshotWithThreadMode`] is co-located with an
/// `Arc<Rocks>` so the database outlives the snapshot.  A custom
/// [`Drop`] impl ensures the snapshot is released before the `Arc`
/// ref-count is decremented.
pub struct ReadTransaction {
    db: Arc<Rocks>,
    snapshot: ManuallyDrop<SnapshotWithThreadMode<'static, Rocks>>,
    path: String,
    opts: Arc<Options>,
}

impl Drop for ReadTransaction {
    fn drop(&mut self) {
        // SAFETY: drop the snapshot while the Arc<Rocks> is still alive.
        unsafe { ManuallyDrop::drop(&mut self.snapshot) };
    }
}

/// Read-write transaction for RocksDB.
///
/// Mutations are buffered in `pending` and applied atomically via a
/// [`WriteBatch`] on [`commit`](KVWriteTransaction::commit).  A snapshot
/// is held for consistent reads with RYOW semantics.
pub struct WriteTransaction {
    db: Arc<Rocks>,
    snapshot: ManuallyDrop<SnapshotWithThreadMode<'static, Rocks>>,
    pending: HashMap<String, HashMap<String, Option<Vec<u8>>>>,
    tx_deleted_tables: HashSet<String>,
    /// Tables deleted-then-re-populated within this transaction.  Their
    /// column family is dropped before the new inserts are applied so that
    /// stale keys do not survive (DeleteTable + Insert = recreate semantics).
    tx_recreated_tables: HashSet<String>,
    path: String,
    opts: Arc<Options>,
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        // SAFETY: drop the snapshot while the Arc<Rocks> is still alive.
        unsafe { ManuallyDrop::drop(&mut self.snapshot) };
    }
}

impl<'a> KVReadTransaction<'a> for ReadTransaction {
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

    fn iter_range(
        &self,
        table: &str,
        range: crate::KeyRange,
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
        let mode = rocks_iterator_mode(&range);
        collect_rocks_range(self.snapshot.iterator_cf(&cf, mode), &range)
    }
}

impl<'a> KVReadTransaction<'a> for WriteTransaction {
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

        // Add pending new tables (not yet committed), but only if they have at least one insert
        for (table, map) in &self.pending {
            if !names.contains(table) && map.values().any(|v| v.is_some()) {
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

    fn iter_range(
        &self,
        table: &str,
        range: crate::KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }
        let table_s = table.to_string();
        if self.tx_deleted_tables.contains(&table_s) {
            return Ok(Vec::new());
        }

        // Overlay pending on top of a range-restricted snapshot scan.
        let mut map = self.pending.get(&table_s).cloned().unwrap_or_default();

        if let Some(cf) = self.db.cf_handle(table) {
            let mode = rocks_iterator_mode(&range);
            for item in self.snapshot.iterator_cf(&cf, mode) {
                let (k, v) = item.map_err(io::Error::other)?;
                let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
                if range.is_beyond_far_end(&k_str) {
                    break;
                }
                if !range.contains(&k_str) {
                    continue;
                }
                map.entry(k_str).or_insert(Some(v.to_vec()));
            }
        }

        // Materialize with range filter, direction and limit.
        let mut result: Vec<(String, Vec<u8>)> = map
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .filter(|(k, _)| range.contains(k))
            .collect();
        result.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        if range.direction == crate::Direction::Reverse {
            result.reverse();
        }
        if let Some(limit) = range.limit {
            result.truncate(limit);
        }
        Ok(result)
    }
}

impl<'a> KVWriteTransaction<'a> for WriteTransaction {
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

        // If table was deleted in this transaction, recreate it:
        // track it in tx_recreated_tables so commit() drops the old CF first.
        if self.tx_deleted_tables.remove(table) {
            self.tx_recreated_tables.insert(table.to_string());
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

    fn commit(mut self) -> Result<(), io::Error> {
        let db = &*self.db;
        let mut batch = WriteBatch::default();

        // Drop CFs for tables that were deleted-then-recreated in this tx.
        // Must happen before applying pending inserts so the old data is gone.
        for table in &self.tx_recreated_tables {
            if db.cf_handle(table).is_some() {
                db.drop_cf(table).map_err(io::Error::other)?;
            }
        }

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
        let deleted = std::mem::take(&mut self.tx_deleted_tables);
        for table in deleted {
            if db.cf_handle(&table).is_some() {
                db.drop_cf(&table).map_err(io::Error::other)?;
            }
        }

        Ok(())
    }

    fn abort(self) -> Result<(), io::Error> {
        Ok(())
    }

    fn batch_commit(
        mut self,
        ops: Vec<super::super::transactional::WriteOp>,
    ) -> Result<(), io::Error> {
        use super::super::transactional::WriteOp;
        for op in ops {
            match op {
                WriteOp::Insert {
                    table_name,
                    key,
                    value,
                } => {
                    if self.tx_deleted_tables.remove(&table_name) {
                        self.tx_recreated_tables.insert(table_name.clone());
                    }
                    self.pending
                        .entry(table_name)
                        .or_default()
                        .insert(key, Some(value));
                }
                WriteOp::Remove { table_name, key } => {
                    if !self.tx_deleted_tables.contains(&table_name) {
                        self.pending
                            .entry(table_name)
                            .or_default()
                            .insert(key, None);
                    }
                }
                WriteOp::DeleteTable { table_name } => {
                    self.tx_recreated_tables.remove(&table_name);
                    self.delete_table(&table_name)?;
                }
                WriteOp::Clear => {
                    self.tx_recreated_tables.clear();
                    self.clear()?;
                }
            }
        }
        self.commit()
    }
}

impl TransactionalKVDB for RocksDB {
    type ReadTransaction<'a> = ReadTransaction;
    type WriteTransaction<'a> = WriteTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        let db = self.inner()?;
        let snapshot = new_owned_snapshot(&db);
        Ok(ReadTransaction {
            db,
            snapshot,
            path: self.path.clone(),
            opts: self.opts.clone(),
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        let db = self.inner()?;
        let snapshot = new_owned_snapshot(&db);
        Ok(WriteTransaction {
            db,
            snapshot,
            pending: HashMap::new(),
            tx_deleted_tables: HashSet::new(),
            tx_recreated_tables: HashSet::new(),
            path: self.path.clone(),
            opts: self.opts.clone(),
        })
    }

    fn try_recover(&self) -> io::Result<()> {
        self.try_recover_from_error()
    }
}

#[cfg(all(feature = "async", feature = "tokio"))]
mod async_impl {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use crate::{
        AsyncTransactionalKVDB,
        transactional::async_kvdb::{SpawnBlockingReadTx, SpawnBlockingWriteTx},
    };

    use super::super::RocksDB;
    use super::{ReadTransaction, WriteTransaction};

    #[async_trait]
    impl AsyncTransactionalKVDB for RocksDB {
        type ReadTransaction<'a> = SpawnBlockingReadTx<ReadTransaction>;
        type WriteTransaction<'a> = SpawnBlockingWriteTx<WriteTransaction>;

        async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, std::io::Error> {
            let db = self.clone();
            tokio::task::spawn_blocking(move || {
                let tx = crate::TransactionalKVDB::begin_read(&db)?;
                Ok(SpawnBlockingReadTx {
                    inner: Arc::new(Mutex::new(Some(tx))),
                })
            })
            .await
            .map_err(std::io::Error::other)?
        }

        async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, std::io::Error> {
            let db = self.clone();
            tokio::task::spawn_blocking(move || {
                let tx = crate::TransactionalKVDB::begin_write(&db)?;
                Ok(SpawnBlockingWriteTx {
                    inner: Arc::new(Mutex::new(Some(tx))),
                })
            })
            .await
            .map_err(std::io::Error::other)?
        }

        async fn try_recover(&self) -> Result<(), std::io::Error> {
            let db = self.clone();
            tokio::task::spawn_blocking(move || crate::TransactionalKVDB::try_recover(&db))
                .await
                .map_err(std::io::Error::other)?
        }
    }
}
