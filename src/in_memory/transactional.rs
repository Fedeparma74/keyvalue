//! Transactional support for [`InMemoryDB`](super::InMemoryDB).
//!
//! Provides snapshot-isolated read transactions and buffered write
//! transactions with explicit commit / abort semantics.
//!
//! * [`ReadTransaction`] captures a point-in-time deep-clone of the
//!   database and serves all reads from that snapshot.
//! * [`WriteTransaction`] layers a pending-change buffer on top of the
//!   snapshot so that reads reflect uncommitted writes (RYOW).  On
//!   [`commit`](crate::KVWriteTransaction::commit) the buffered mutations
//!   are applied atomically under a single write lock so no concurrent
//!   operation observes a partially-committed state.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;

use crate::{KVReadTransaction, KVWriteTransaction, KeyRange, TransactionalKVDB};

use super::{InMemoryDB, Store, Table, collect_prefix, collect_range};

// ---------------------------------------------------------------------------
// Transaction types
// ---------------------------------------------------------------------------

/// Read-only snapshot of an [`InMemoryDB`].
pub struct ReadTransaction {
    snapshot: Store,
}

/// Read-write transaction for [`InMemoryDB`].
pub struct WriteTransaction {
    /// Reference to the shared backing store – used only during `commit()`.
    db: InMemoryDB,
    /// Point-in-time snapshot taken at `begin_write()`.
    snapshot: Store,
    /// Buffered mutations: `None` means the key was deleted.
    pending: HashMap<String, HashMap<String, Option<Vec<u8>>>>,
    /// Tables that have been entirely deleted within this transaction.
    deleted_tables: HashSet<String>,
}

impl WriteTransaction {
    /// Merge the snapshot and pending layers for `table_name`, respecting
    /// transaction-local deletions, into a single sorted [`Table`].
    fn materialize_table(&self, table_name: &str) -> Table {
        let table_deleted = self.deleted_tables.contains(table_name);
        let mut merged: Table = if table_deleted {
            Table::new()
        } else {
            self.snapshot.get(table_name).cloned().unwrap_or_default()
        };
        if let Some(pending) = self.pending.get(table_name) {
            for (k, v) in pending {
                match v {
                    Some(val) => {
                        merged.insert(k.clone(), val.clone());
                    }
                    None => {
                        merged.remove(k);
                    }
                }
            }
        }
        merged
    }
}

// ---------------------------------------------------------------------------
// KVReadTransaction — ReadTransaction
// ---------------------------------------------------------------------------

impl<'a> KVReadTransaction<'a> for ReadTransaction {
    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self
            .snapshot
            .get(table_name)
            .and_then(|table| table.get(key).cloned()))
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        Ok(self
            .snapshot
            .get(table_name)
            .map(|table| table.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default())
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        Ok(self.snapshot.keys().cloned().collect())
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        match self.snapshot.get(table_name) {
            Some(table) => Ok(collect_prefix(table, prefix)),
            None => Ok(Vec::new()),
        }
    }

    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        match self.snapshot.get(table_name) {
            Some(table) => Ok(collect_range(table, &range)),
            None => Ok(Vec::new()),
        }
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.snapshot.contains_key(table_name))
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self
            .snapshot
            .get(table_name)
            .is_some_and(|t| t.contains_key(key)))
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self
            .snapshot
            .get(table_name)
            .map(|t| t.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .snapshot
            .get(table_name)
            .map(|t| t.values().cloned().collect())
            .unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// KVReadTransaction — WriteTransaction (RYOW)
// ---------------------------------------------------------------------------

impl<'a> KVReadTransaction<'a> for WriteTransaction {
    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        // Check pending first (RYOW) even if the table was deleted – the
        // same transaction may have re-populated it after deletion.
        if let Some(map) = self.pending.get(table_name)
            && let Some(val) = map.get(key)
        {
            return Ok(val.clone()); // None → deleted, Some(v) → inserted
        }
        if self.deleted_tables.contains(table_name) {
            return Ok(None);
        }
        Ok(self
            .snapshot
            .get(table_name)
            .and_then(|t| t.get(key).cloned()))
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        Ok(self.materialize_table(table_name).into_iter().collect())
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut names: BTreeMap<String, ()> = BTreeMap::new();
        for name in self.snapshot.keys() {
            if !self.deleted_tables.contains(name) {
                names.insert(name.clone(), ());
            }
        }
        for (table, entries) in &self.pending {
            // A table exists if it has at least one non-deleted entry.
            if entries.values().any(|v| v.is_some()) {
                names.insert(table.clone(), ());
            }
        }
        Ok(names.into_keys().collect())
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let table = self.materialize_table(table_name);
        Ok(collect_prefix(&table, prefix))
    }

    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let table = self.materialize_table(table_name);
        Ok(collect_range(&table, &range))
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        // Fast path – avoid materialising table_names.
        if let Some(pending) = self.pending.get(table_name)
            && pending.values().any(|v| v.is_some())
        {
            return Ok(true);
        }
        if self.deleted_tables.contains(table_name) {
            return Ok(false);
        }
        Ok(self.snapshot.contains_key(table_name))
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key)?.is_some())
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self.materialize_table(table_name).into_keys().collect())
    }

    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self.materialize_table(table_name).into_values().collect())
    }
}

// ---------------------------------------------------------------------------
// KVWriteTransaction — WriteTransaction
// ---------------------------------------------------------------------------

impl<'a> KVWriteTransaction<'a> for WriteTransaction {
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let old = self.get(table_name, key)?;
        self.pending
            .entry(table_name.to_owned())
            .or_default()
            .insert(key.to_owned(), Some(value.to_owned()));
        Ok(old)
    }

    fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let old = self.get(table_name, key)?;
        if old.is_none() {
            return Ok(None);
        }
        self.pending
            .entry(table_name.to_owned())
            .or_default()
            .insert(key.to_owned(), None);
        Ok(old)
    }

    fn commit(self) -> Result<(), io::Error> {
        // A single write lock makes the commit truly atomic: no concurrent
        // reader or writer can observe a partially-applied state.
        let mut store = self.db.write()?;

        for table in &self.deleted_tables {
            store.remove(table);
        }

        for (table_name, entries) in self.pending {
            let has_inserts = entries.values().any(|v| v.is_some());
            if has_inserts {
                let table_entry = store.entry(table_name).or_default();
                for (key, value) in entries {
                    match value {
                        Some(val) => {
                            table_entry.insert(key, val);
                        }
                        None => {
                            table_entry.remove(&key);
                        }
                    }
                }
            } else if let Some(table_entry) = store.get_mut(&table_name) {
                for (key, _) in entries {
                    table_entry.remove(&key);
                }
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
                    // NOTE: we intentionally do NOT remove `table_name` from
                    // `deleted_tables` here.  If the table was deleted
                    // earlier in this batch, the commit phase will first
                    // drop the live table and then apply the buffered
                    // inserts, giving `DeleteTable + Insert` the expected
                    // "recreate" semantics.
                    self.pending
                        .entry(table_name)
                        .or_default()
                        .insert(key, Some(value));
                }
                WriteOp::Remove { table_name, key } => {
                    if !self.deleted_tables.contains(&table_name) {
                        self.pending
                            .entry(table_name)
                            .or_default()
                            .insert(key, None);
                    }
                }
                WriteOp::DeleteTable { table_name } => {
                    self.delete_table(&table_name)?;
                }
                WriteOp::Clear => {
                    self.clear()?;
                }
            }
        }
        self.commit()
    }

    fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        self.deleted_tables.insert(table_name.to_owned());
        self.pending.remove(table_name);
        Ok(())
    }

    fn clear(&mut self) -> Result<(), io::Error> {
        for name in self.snapshot.keys() {
            self.deleted_tables.insert(name.clone());
        }
        for name in self.pending.keys() {
            self.deleted_tables.insert(name.clone());
        }
        self.pending.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TransactionalKVDB for InMemoryDB
// ---------------------------------------------------------------------------

impl TransactionalKVDB for InMemoryDB {
    type ReadTransaction<'a> = ReadTransaction;
    type WriteTransaction<'a> = WriteTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        let snapshot = self.read()?.clone();
        Ok(ReadTransaction { snapshot })
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        let snapshot = self.read()?.clone();
        Ok(WriteTransaction {
            db: self.clone(),
            snapshot,
            pending: HashMap::new(),
            deleted_tables: HashSet::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Async transactional support (via spawn_blocking on tokio targets)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "async", feature = "tokio"))]
mod async_impl {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use crate::{
        AsyncTransactionalKVDB,
        transactional::async_kvdb::{SpawnBlockingReadTx, SpawnBlockingWriteTx},
    };

    use super::super::InMemoryDB;
    use super::{ReadTransaction, WriteTransaction};

    #[async_trait]
    impl AsyncTransactionalKVDB for InMemoryDB {
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
    }
}

// ---------------------------------------------------------------------------
// Async transactional support (direct, non-tokio async targets e.g. WASM)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "async", not(feature = "tokio")))]
mod async_impl_direct {
    use std::io;

    use async_trait::async_trait;

    use crate::{
        AsyncKVReadTransaction, AsyncKVWriteTransaction, AsyncTransactionalKVDB, KVReadTransaction,
        KVWriteTransaction,
    };

    use super::super::InMemoryDB;
    use super::{ReadTransaction, WriteTransaction};

    // The in-memory operations are non-blocking, so we can implement the
    // async trait directly by delegating to the sync implementations.

    #[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
    #[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
    impl<'a> AsyncKVReadTransaction<'a> for ReadTransaction {
        async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
            KVReadTransaction::get(self, table_name, key)
        }
        async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
            KVReadTransaction::iter(self, table_name)
        }
        async fn table_names(&self) -> Result<Vec<String>, io::Error> {
            KVReadTransaction::table_names(self)
        }
        async fn iter_from_prefix(
            &self,
            table_name: &str,
            prefix: &str,
        ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
            KVReadTransaction::iter_from_prefix(self, table_name, prefix)
        }
        async fn iter_range(
            &self,
            table_name: &str,
            range: crate::KeyRange,
        ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
            KVReadTransaction::iter_range(self, table_name, range)
        }
        async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
            KVReadTransaction::contains_table(self, table_name)
        }
        async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
            KVReadTransaction::contains_key(self, table_name, key)
        }
        async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
            KVReadTransaction::keys(self, table_name)
        }
        async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
            KVReadTransaction::values(self, table_name)
        }
    }

    #[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
    #[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
    impl<'a> AsyncKVReadTransaction<'a> for WriteTransaction {
        async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
            KVReadTransaction::get(self, table_name, key)
        }
        async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
            KVReadTransaction::iter(self, table_name)
        }
        async fn table_names(&self) -> Result<Vec<String>, io::Error> {
            KVReadTransaction::table_names(self)
        }
        async fn iter_from_prefix(
            &self,
            table_name: &str,
            prefix: &str,
        ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
            KVReadTransaction::iter_from_prefix(self, table_name, prefix)
        }
        async fn iter_range(
            &self,
            table_name: &str,
            range: crate::KeyRange,
        ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
            KVReadTransaction::iter_range(self, table_name, range)
        }
        async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
            KVReadTransaction::contains_table(self, table_name)
        }
        async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
            KVReadTransaction::contains_key(self, table_name, key)
        }
        async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
            KVReadTransaction::keys(self, table_name)
        }
        async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
            KVReadTransaction::values(self, table_name)
        }
    }

    #[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
    #[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
    impl<'a> AsyncKVWriteTransaction<'a> for WriteTransaction {
        async fn insert(
            &mut self,
            table_name: &str,
            key: &str,
            value: &[u8],
        ) -> Result<Option<Vec<u8>>, io::Error> {
            KVWriteTransaction::insert(self, table_name, key, value)
        }
        async fn remove(
            &mut self,
            table_name: &str,
            key: &str,
        ) -> Result<Option<Vec<u8>>, io::Error> {
            KVWriteTransaction::remove(self, table_name, key)
        }
        async fn commit(self) -> Result<(), io::Error> {
            KVWriteTransaction::commit(self)
        }
        async fn abort(self) -> Result<(), io::Error> {
            KVWriteTransaction::abort(self)
        }
        async fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
            KVWriteTransaction::delete_table(self, table_name)
        }
        async fn clear(&mut self) -> Result<(), io::Error> {
            KVWriteTransaction::clear(self)
        }
    }

    #[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
    #[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
    impl AsyncTransactionalKVDB for InMemoryDB {
        type ReadTransaction<'a> = ReadTransaction;
        type WriteTransaction<'a> = WriteTransaction;

        async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, std::io::Error> {
            crate::TransactionalKVDB::begin_read(self)
        }

        async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, std::io::Error> {
            crate::TransactionalKVDB::begin_write(self)
        }
    }
}
