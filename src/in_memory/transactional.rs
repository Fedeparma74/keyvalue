//! Transactional support for [`InMemoryDB`](super::InMemoryDB).
//!
//! Provides snapshot-isolated read transactions and buffered write
//! transactions with explicit commit / abort semantics.
//!
//! * [`ReadTransaction`] captures a point-in-time clone of the database
//!   and serves all reads from that snapshot.
//! * [`WriteTransaction`] layers a pending-change buffer on top of the
//!   snapshot so that reads reflect uncommitted writes (RYOW). On
//!   [`commit()`](crate::KVWriteTransaction::commit) the buffered
//!   mutations are applied atomically to the shared [`DashMap`](dashmap::DashMap).

use std::collections::HashMap;
use std::io;

use crate::{KVReadTransaction, KVWriteTransaction, TransactionalKVDB};

use super::InMemoryDB;

// ---------------------------------------------------------------------------
// Snapshot helper
// ---------------------------------------------------------------------------

/// Takes a consistent snapshot of every table in the `DashMap`.
fn snapshot(
    map: &dashmap::DashMap<String, HashMap<String, Vec<u8>>>,
) -> HashMap<String, HashMap<String, Vec<u8>>> {
    map.iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// Transaction types
// ---------------------------------------------------------------------------

/// Read-only snapshot of an [`InMemoryDB`].
///
/// All reads are served from the snapshot taken at
/// [`begin_read()`](TransactionalKVDB::begin_read) time, providing a
/// consistent point-in-time view regardless of concurrent writes.
pub struct ReadTransaction {
    snapshot: HashMap<String, HashMap<String, Vec<u8>>>,
}

/// Read-write transaction for [`InMemoryDB`].
///
/// Mutations are buffered in `pending` and only applied to the backing
/// `DashMap` on [`commit()`](KVWriteTransaction::commit). Reads first
/// check the pending buffer (read-your-own-writes) and then fall through
/// to the snapshot.
pub struct WriteTransaction {
    /// Reference to the shared backing store – used only during `commit()`.
    db: InMemoryDB,
    /// Point-in-time snapshot taken at `begin_write()`.
    snapshot: HashMap<String, HashMap<String, Vec<u8>>>,
    /// Buffered mutations: `None` means the key was deleted.
    pending: HashMap<String, HashMap<String, Option<Vec<u8>>>>,
    /// Tables that have been entirely deleted within this transaction.
    deleted_tables: std::collections::HashSet<String>,
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
        Ok(self
            .snapshot
            .get(table_name)
            .map(|table| {
                table
                    .iter()
                    .filter(|(k, _)| k.starts_with(prefix))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default())
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
        // Table was deleted in this transaction.
        if self.deleted_tables.contains(table_name) {
            // Still check pending – the table may have been re-populated
            // after deletion within the same transaction.
            if let Some(map) = self.pending.get(table_name)
                && let Some(val) = map.get(key)
            {
                return Ok(val.clone());
            }
            return Ok(None);
        }

        // Check pending (RYOW).
        if let Some(map) = self.pending.get(table_name)
            && let Some(val) = map.get(key)
        {
            return Ok(val.clone()); // `None` → deleted, `Some(v)` → inserted
        }

        // Fall through to snapshot.
        Ok(self
            .snapshot
            .get(table_name)
            .and_then(|t| t.get(key).cloned()))
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let table_deleted = self.deleted_tables.contains(table_name);

        // Start from snapshot (unless the table was deleted).
        let mut merged: HashMap<String, Vec<u8>> = if table_deleted {
            HashMap::new()
        } else {
            self.snapshot.get(table_name).cloned().unwrap_or_default()
        };

        // Layer pending changes on top.
        if let Some(pending_table) = self.pending.get(table_name) {
            for (k, v) in pending_table {
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

        Ok(merged.into_iter().collect())
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut names: std::collections::HashSet<String> = self.snapshot.keys().cloned().collect();

        // Remove tables that were deleted in this transaction.
        for deleted in &self.deleted_tables {
            names.remove(deleted);
        }

        // Add tables that were (re-)created via pending inserts.
        for (table, entries) in &self.pending {
            // A table exists if it has at least one non-deleted entry.
            if entries.values().any(|v| v.is_some()) {
                names.insert(table.clone());
            }
        }

        Ok(names.into_iter().collect())
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        Ok(self
            .iter(table_name)?
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect())
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names()?.contains(&table_name.to_string()))
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key)?.is_some())
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self.iter(table_name)?.into_iter().map(|(k, _)| k).collect())
    }

    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self.iter(table_name)?.into_iter().map(|(_, v)| v).collect())
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
        self.pending
            .entry(table_name.to_owned())
            .or_default()
            .insert(key.to_owned(), None);
        Ok(old)
    }

    fn commit(self) -> Result<(), io::Error> {
        // Apply deleted tables first.
        for table in &self.deleted_tables {
            self.db.map.remove(table);
        }

        // Apply pending mutations atomically.
        for (table_name, entries) in self.pending {
            let has_inserts = entries.values().any(|v| v.is_some());
            if has_inserts {
                // `entry(table_name)` moves the String; no extra clone needed.
                let mut table_ref = self.db.map.entry(table_name).or_default();
                for (key, value) in entries {
                    match value {
                        Some(val) => {
                            table_ref.insert(key, val);
                        }
                        None => {
                            table_ref.remove(&key);
                        }
                    }
                }
            } else {
                // Removals only — avoid creating a new empty inner HashMap.
                if let Some(mut table_ref) = self.db.map.get_mut(&table_name) {
                    for (key, _) in entries {
                        table_ref.remove(&key);
                    }
                }
            }
        }

        Ok(())
    }

    fn abort(self) -> Result<(), io::Error> {
        // Simply drop – pending changes are discarded.
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
        // Clear any pending changes for this table.
        self.pending.remove(table_name);
        Ok(())
    }

    fn clear(&mut self) -> Result<(), io::Error> {
        // Mark all snapshot tables and any pending-created tables as deleted.
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
        Ok(ReadTransaction {
            snapshot: snapshot(&self.map),
        })
    }

    fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        Ok(WriteTransaction {
            db: self.clone(),
            snapshot: snapshot(&self.map),
            pending: HashMap::new(),
            deleted_tables: std::collections::HashSet::new(),
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
