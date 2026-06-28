use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::Infallible,
    io,
    pin::Pin,
    rc::Rc,
    sync::{Arc, atomic::AtomicU32},
};

use async_trait::async_trait;

use futures::FutureExt;
use indexed_db::{Database, Factory, ObjectStore, Transaction, VersionChangeEvent};
use js_sys::{Uint8Array, wasm_bindgen::JsValue};
use wasmt::sync::{RwLock, mpsc::UnboundedSender, oneshot};

use crate::{
    AsyncKVReadTransaction, AsyncKVWriteTransaction, AsyncTransactionalKVDB,
    indexed_db::{CommandRequestClosure, CommandResponse, indexed_db_error_to_io_error},
};

use super::IndexedDB;

type WriteOperation = dyn Fn(
        Rc<ObjectStore<Infallible>>,
    )
        -> Pin<Box<dyn Future<Output = Result<Option<TableUpdate>, indexed_db::Error<Infallible>>>>>
    + Send
    + Sync;

enum TableUpdate {
    Created(String), // table name
    Deleted(String), // table name
}

/// In-memory mirror of a [`WriteTransaction`]'s queued operations, kept so reads
/// (`get`/`iter`/`table_names`/`keys_range`) can be answered from the committed
/// database plus this overlay in O(1)/O(table) time.
///
/// Without it, every read replays the *entire* pending-operation log inside an
/// aborted read-write transaction to honour read-your-own-writes.
#[derive(Default)]
struct WriteOverlay {
    /// `table -> key -> Some(value)` for a pending put, or `None` for a pending
    /// delete (tombstone).
    writes: HashMap<String, HashMap<String, Option<Vec<u8>>>>,
    /// Tables emptied by `delete_table`/`clear` since the transaction began:
    /// their committed rows are invisible to reads; only entries re-added
    /// afterwards (present in `writes`) show through.
    cleared_tables: HashSet<String>,
}

impl WriteOverlay {
    fn put(&mut self, table_name: &str, key: &str, value: Vec<u8>) {
        self.writes
            .entry(table_name.to_string())
            .or_default()
            .insert(key.to_string(), Some(value));
    }

    fn tombstone(&mut self, table_name: &str, key: &str) {
        self.writes
            .entry(table_name.to_string())
            .or_default()
            .insert(key.to_string(), None);
    }

    fn delete_table(&mut self, table_name: &str) {
        self.writes.remove(table_name);
        self.cleared_tables.insert(table_name.to_string());
    }

    /// `Some(opt)` when the overlay fully determines the value (`opt` is the
    /// pending value, or `None` for a deleted/cleared key); `None` when the read
    /// must consult the committed database.
    fn resolve(&self, table_name: &str, key: &str) -> Option<Option<Vec<u8>>> {
        if let Some(value) = self.writes.get(table_name).and_then(|t| t.get(key)) {
            return Some(value.clone());
        }
        if self.cleared_tables.contains(table_name) {
            return Some(None);
        }
        None
    }
}

/// Read-only IndexedDB transaction.
///
/// Reads are dispatched through the shared command channel and executed
/// inside a read-only IDB transaction.
pub struct ReadTransaction {
    command_request_sender:
        UnboundedSender<(CommandRequestClosure, oneshot::Sender<CommandResponse>)>,
}

/// Read-write IndexedDB transaction.
///
/// Write operations are collected in `operations` and replayed inside a
/// single read-write IDB transaction on [`commit`](AsyncKVWriteTransaction::commit).
/// Reads are answered from the committed database merged with [`overlay`], an
/// in-memory mirror of the pending writes, giving read-your-own-write semantics
/// without replaying the operation log on every read.
///
/// [`overlay`]: WriteOverlay
pub struct WriteTransaction {
    name: String,
    version: Arc<AtomicU32>,
    command_request_sender:
        UnboundedSender<(CommandRequestClosure, oneshot::Sender<CommandResponse>)>,
    operations: Vec<(
        String, // table name
        Arc<WriteOperation>,
    )>,
    overlay: WriteOverlay,
}

impl WriteTransaction {
    /// A read-only view of the *committed* database (pending writes excluded).
    /// Reads merge its results with [`Self::overlay`].
    fn committed(&self) -> ReadTransaction {
        ReadTransaction {
            command_request_sender: self.command_request_sender.clone(),
        }
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a> AsyncKVReadTransaction<'a> for ReadTransaction {
    async fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let table_name = table_name.to_string();
        let key = key.to_string();
        let get_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let table_name = table_name.to_string();
                let key = key.to_string();
                let value = match db
                    .transaction(&[&table_name])
                    .run::<_, Infallible>(move |tx: Transaction<Infallible>| async move {
                        let table = tx.object_store(&table_name)?;
                        let value = table.get(&JsValue::from(key)).await?;
                        Ok(value)
                    })
                    .await
                    .map_err(indexed_db_error_to_io_error)
                {
                    Ok(value) => value,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            return Ok(CommandResponse::Get(None));
                        } else {
                            return Err(e);
                        }
                    }
                };

                Ok::<_, std::io::Error>(CommandResponse::Get(
                    value.map(|v| Uint8Array::from(v).to_vec()),
                ))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .send((Box::new(get_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::Get(value) = response {
            return Ok(value);
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let table_name = table_name.to_string();
        let iter_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let table_name = table_name.to_string();
                let values = match db
                    .transaction(&[&table_name])
                    .run::<_, Infallible>(move |tx: Transaction<Infallible>| async move {
                        let table = tx.object_store(&table_name)?;
                        let mut key_values = Vec::new();
                        for key in table.get_all_keys(None).await? {
                            if let Some(value) = table.get(&key).await? {
                                let key = key.as_string().unwrap_or_default();
                                let value = Uint8Array::from(value).to_vec();
                                key_values.push((key, value));
                            }
                        }

                        Ok(key_values)
                    })
                    .await
                    .map_err(indexed_db_error_to_io_error)
                {
                    Ok(values) => values,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            return Ok(CommandResponse::Iter(Vec::new()));
                        } else {
                            return Err(e);
                        }
                    }
                };

                Ok::<_, std::io::Error>(CommandResponse::Iter(values))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .send((Box::new(iter_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::Iter(values) = response {
            return Ok(values);
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }
        Err(io::Error::other("Unexpected response type"))
    }

    async fn table_names(&self) -> io::Result<Vec<String>> {
        let table_names_closure = |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let table_names = db.object_store_names();
                Ok::<_, std::io::Error>(CommandResponse::TableNames(table_names))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .send((Box::new(table_names_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;
        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::TableNames(table_names) = response {
            return Ok(table_names);
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn keys_range(
        &self,
        table_name: &str,
        range: crate::KeyRange,
    ) -> io::Result<Vec<String>> {
        // Native key-only fetch via IDB `getAllKeys`, then apply the range
        // client-side.  No value blobs are touched.
        let keys = read_all_keys(&self.command_request_sender, table_name).await?;
        Ok(keys_apply_range(keys, &range))
    }
}

/// Dispatches a read-only `getAllKeys` command via the actor channel and
/// returns the raw key list.  Shared by read and write transactional
/// `keys_range` implementations.
async fn read_all_keys(
    sender: &UnboundedSender<(CommandRequestClosure, oneshot::Sender<CommandResponse>)>,
    table_name: &str,
) -> io::Result<Vec<String>> {
    let table_name = table_name.to_string();
    let keys_closure = move |db: Rc<RwLock<Database>>| {
        async move {
            let db = db.read().await;
            let table_name = table_name.to_string();
            let keys = match db
                .transaction(&[&table_name])
                .run::<_, Infallible>(move |tx: Transaction<Infallible>| async move {
                    let table = tx.object_store(&table_name)?;
                    let raw = table.get_all_keys(None).await?;
                    Ok(raw
                        .into_iter()
                        .map(|k| k.as_string().unwrap_or_default())
                        .collect::<Vec<_>>())
                })
                .await
                .map_err(indexed_db_error_to_io_error)
            {
                Ok(keys) => keys,
                Err(e) => {
                    if e.kind() == io::ErrorKind::NotFound {
                        return Ok(CommandResponse::Keys(Vec::new()));
                    } else {
                        return Err(e);
                    }
                }
            };
            Ok::<_, io::Error>(CommandResponse::Keys(keys))
        }
        .boxed_local()
    };

    let (response_sender, response_receiver) = oneshot::channel();
    sender
        .send((Box::new(keys_closure), response_sender))
        .map_err(|_| io::Error::other("Failed to send command request"))?;
    let response = response_receiver
        .await
        .map_err(|_| io::Error::other("Failed to receive command response"))?;
    match response {
        CommandResponse::Keys(keys) => Ok(keys),
        CommandResponse::Error(e) => Err(e),
        _ => Err(io::Error::other("Unexpected response type")),
    }
}

/// Apply a [`crate::KeyRange`] to a raw key list — sort, filter, direction,
/// limit.  Does not touch any values.
fn keys_apply_range(keys: Vec<String>, range: &crate::KeyRange) -> Vec<String> {
    let mut filtered: Vec<String> = keys.into_iter().filter(|k| range.contains(k)).collect();
    filtered.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    if range.direction == crate::Direction::Reverse {
        filtered.reverse();
    }
    if let Some(limit) = range.limit {
        filtered.truncate(limit);
    }
    filtered
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a> AsyncKVReadTransaction<'a> for WriteTransaction {
    async fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        if let Some(resolved) = self.overlay.resolve(table_name, key) {
            return Ok(resolved);
        }
        self.committed().get(table_name, key).await
    }

    async fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        // Committed rows (skipped if the table was cleared in this tx), then the
        // pending writes layered on top — puts override, tombstones drop.
        let base = if self.overlay.cleared_tables.contains(table_name) {
            Vec::new()
        } else {
            self.committed().iter(table_name).await?
        };
        let mut merged: BTreeMap<String, Vec<u8>> = base.into_iter().collect();
        if let Some(writes) = self.overlay.writes.get(table_name) {
            for (key, value) in writes {
                match value {
                    Some(value) => {
                        merged.insert(key.clone(), value.clone());
                    }
                    None => {
                        merged.remove(key);
                    }
                }
            }
        }
        Ok(merged.into_iter().collect())
    }

    async fn table_names(&self) -> io::Result<Vec<String>> {
        // Committed stores minus those cleared in this tx, plus any table a
        // pending put has (re)created.
        let mut names: HashSet<String> = self
            .committed()
            .table_names()
            .await?
            .into_iter()
            .filter(|name| !self.overlay.cleared_tables.contains(name))
            .collect();
        for (table_name, writes) in &self.overlay.writes {
            if writes.values().any(Option::is_some) {
                names.insert(table_name.clone());
            }
        }
        Ok(names.into_iter().collect())
    }

    async fn keys_range(
        &self,
        table_name: &str,
        range: crate::KeyRange,
    ) -> io::Result<Vec<String>> {
        // Committed keys (skipped if cleared in this tx), with pending puts added
        // and tombstones removed; the range/sort/limit is applied last.
        let base = if self.overlay.cleared_tables.contains(table_name) {
            Vec::new()
        } else {
            read_all_keys(&self.command_request_sender, table_name).await?
        };
        let mut keys: BTreeSet<String> = base.into_iter().collect();
        if let Some(writes) = self.overlay.writes.get(table_name) {
            for (key, value) in writes {
                if value.is_some() {
                    keys.insert(key.clone());
                } else {
                    keys.remove(key);
                }
            }
        }
        Ok(keys_apply_range(keys.into_iter().collect(), &range))
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
    ) -> io::Result<Option<Vec<u8>>> {
        let old_value = self.get(table_name, key).await?;

        let table_name_clone = table_name.to_string();
        let key_clone = key.to_string();
        let value_owned = value.to_vec();
        let value_clone = value_owned.clone();
        let op = move |table: Rc<ObjectStore<Infallible>>| -> Pin<
            Box<dyn Future<Output = Result<Option<TableUpdate>, indexed_db::Error<Infallible>>>>,
        > {
            let table_name = table_name_clone.to_string();
            let key = key_clone.to_string();
            let value = value_clone.clone();
            Box::pin(async move {
                table
                    .put_kv(&JsValue::from(key), &Uint8Array::from(value.as_ref()))
                    .await?;
                // If the table did not exist before, it will be created
                Ok(Some(TableUpdate::Created(table_name)))
            })
        };

        self.operations.push((table_name.to_string(), Arc::new(op)));
        self.overlay.put(table_name, key, value_owned);

        Ok(old_value)
    }

    async fn remove(&mut self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let old_value = self.get(table_name, key).await?;

        let table_name_clone = table_name.to_string();
        let key_clone = key.to_string();
        let op = move |table: Rc<ObjectStore<Infallible>>| -> Pin<
            Box<dyn Future<Output = Result<Option<TableUpdate>, indexed_db::Error<Infallible>>>>,
        > {
            let key = key_clone.to_string();
            Box::pin(async move {
                match table.delete(&JsValue::from(key)).await {
                    Ok(()) => {}
                    Err(indexed_db::Error::DoesNotExist) => {
                        // If the key does not exist, do nothing
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
                Ok(None)
            })
        };

        self.operations.push((table_name_clone, Arc::new(op)));
        self.overlay.tombstone(table_name, key);

        Ok(old_value)
    }

    async fn delete_table(&mut self, table_name: &str) -> io::Result<()> {
        let table_name_clone = table_name.to_string();
        let op = move |db_table: Rc<ObjectStore<Infallible>>| -> Pin<
            Box<dyn Future<Output = Result<Option<TableUpdate>, indexed_db::Error<Infallible>>>>,
        > {
            let table_name = table_name_clone.to_string();
            Box::pin(async move {
                for key in db_table.get_all_keys(None).await? {
                    db_table.delete(&key).await?;
                }

                Ok(Some(TableUpdate::Deleted(table_name)))
            })
        };

        self.operations.push((table_name.to_string(), Arc::new(op)));
        self.overlay.delete_table(table_name);

        Ok(())
    }

    async fn clear(&mut self) -> io::Result<()> {
        let table_names = self.table_names().await?;
        for table_name in table_names {
            self.delete_table(&table_name).await?;
        }
        Ok(())
    }

    async fn commit(self) -> io::Result<()> {
        let operations = self.operations;
        let name = self.name.clone();
        let version = self.version.clone();
        let commit_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let mut table_names = HashSet::new();
                for (op_table_name, _) in &operations {
                    table_names.insert(op_table_name.clone());
                }

                if table_names.is_empty() {
                    return Ok(CommandResponse::Commit);
                }

                let mut db = db.write().await;

                // create object stores if they do not exist
                let new_table_names: Vec<String> = table_names
                    .difference(&db.object_store_names().into_iter().collect())
                    .cloned()
                    .collect();

                if !new_table_names.is_empty() {
                    db.close();

                    let current = version.load(std::sync::atomic::Ordering::SeqCst);
                    let new_version = current
                        .checked_add(1)
                        .ok_or_else(|| io::Error::other("IndexedDB version overflow"))?;
                    version.store(new_version, std::sync::atomic::Ordering::SeqCst);

                    *db = Factory::get()
                        .map_err(indexed_db_error_to_io_error)?
                        .open::<Infallible>(
                            &name,
                            new_version,
                            move |evt: VersionChangeEvent<Infallible>| async move {
                                for table_name in new_table_names {
                                    evt.build_object_store(&table_name).create()?;
                                }
                                Ok(())
                            },
                        )
                        .await
                        .map_err(indexed_db_error_to_io_error)?
                        .into_manual_close();
                }

                let deleted_tables = Rc::new(RefCell::new(HashSet::new()));
                let deleted_tables_clone = Rc::clone(&deleted_tables);
                let res = match db
                    .transaction(
                        &table_names
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<&str>>(),
                    )
                    .rw()
                    .run::<_, Infallible>(move |tx: Transaction<Infallible>| async move {
                        // cache opened tables
                        let mut tables = HashMap::<String, Rc<ObjectStore<Infallible>>>::new();

                        // perform all queued operations
                        for (op_table_name, op) in operations {
                            let table = if let Some(t) = tables.get(&op_table_name) {
                                t.clone()
                            } else {
                                let t = Rc::new(tx.object_store(&op_table_name)?);
                                tables.insert(op_table_name.clone(), Rc::clone(&t));
                                t
                            };

                            if let Some(table_update) = op(table).await? {
                                match table_update {
                                    TableUpdate::Created(name) => {
                                        deleted_tables_clone.borrow_mut().remove(&name);
                                    }
                                    TableUpdate::Deleted(name) => {
                                        deleted_tables_clone.borrow_mut().insert(name);
                                    }
                                }
                            }
                        }

                        Ok::<_, indexed_db::Error<Infallible>>(())
                    })
                    .await
                    .map_err(indexed_db_error_to_io_error)
                {
                    Ok(()) => Ok(CommandResponse::Commit),
                    Err(e) => Err(e),
                };

                if !deleted_tables.borrow().is_empty() {
                    db.close();

                    let current = version.load(std::sync::atomic::Ordering::SeqCst);
                    let new_version = current
                        .checked_add(1)
                        .ok_or_else(|| io::Error::other("IndexedDB version overflow"))?;
                    version.store(new_version, std::sync::atomic::Ordering::SeqCst);

                    *db = Factory::get()
                        .map_err(indexed_db_error_to_io_error)?
                        .open::<Infallible>(
                            &name,
                            new_version,
                            move |evt: VersionChangeEvent<Infallible>| async move {
                                for table_name in deleted_tables.borrow().iter() {
                                    evt.delete_object_store(table_name)?;
                                }
                                Ok(())
                            },
                        )
                        .await
                        .map_err(indexed_db_error_to_io_error)?
                        .into_manual_close();
                }

                res
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .send((Box::new(commit_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::Commit = response {
            return Ok(());
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn abort(self) -> io::Result<()> {
        // Simply drop the transaction without committing
        Ok(())
    }

    async fn batch_commit(mut self, ops: Vec<crate::transactional::WriteOp>) -> io::Result<()> {
        use crate::transactional::WriteOp;
        for op in ops {
            match op {
                WriteOp::Insert {
                    table_name,
                    key,
                    value,
                } => {
                    self.insert(&table_name, &key, &value).await?;
                }
                WriteOp::Remove { table_name, key } => {
                    self.remove(&table_name, &key).await?;
                }
                WriteOp::DeleteTable { table_name } => {
                    self.delete_table(&table_name).await?;
                }
                WriteOp::Clear => {
                    self.clear().await?;
                }
            }
        }
        self.commit().await
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncTransactionalKVDB for IndexedDB {
    type ReadTransaction<'a> = ReadTransaction;
    type WriteTransaction<'a> = WriteTransaction;

    async fn begin_read(&self) -> io::Result<Self::ReadTransaction<'_>> {
        Ok(ReadTransaction {
            command_request_sender: self.command_request_sender.clone(),
        })
    }

    async fn begin_write(&self) -> io::Result<Self::WriteTransaction<'_>> {
        Ok(WriteTransaction {
            name: self.name.clone(),
            version: self.version.clone(),
            command_request_sender: self.command_request_sender.clone(),
            operations: Vec::new(),
            overlay: WriteOverlay::default(),
        })
    }

    async fn try_recover(&self) -> io::Result<()> {
        self.try_recover_from_error().await
    }
}
