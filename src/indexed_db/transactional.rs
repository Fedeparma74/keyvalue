use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    convert::Infallible,
    io,
    pin::Pin,
    rc::Rc,
    sync::{Arc, atomic::AtomicU32},
};

use async_trait::async_trait;

use futures::{
    FutureExt,
    channel::{mpsc::UnboundedSender, oneshot},
};
use indexed_db::{Database, Factory, ObjectStore, Transaction, VersionChangeEvent};
use js_sys::{Uint8Array, wasm_bindgen::JsValue};
use tokio::sync::RwLock;

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

pub struct ReadTransaction {
    command_request_sender:
        UnboundedSender<(CommandRequestClosure, oneshot::Sender<CommandResponse>)>,
}

pub struct WriteTransaction {
    name: String,
    version: Arc<AtomicU32>,
    command_request_sender:
        UnboundedSender<(CommandRequestClosure, oneshot::Sender<CommandResponse>)>,
    operations: Vec<(
        String, // table name
        Arc<WriteOperation>,
    )>,
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
            .unbounded_send((Box::new(get_closure), response_sender))
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
            .unbounded_send((Box::new(iter_closure), response_sender))
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
            .unbounded_send((Box::new(table_names_closure), response_sender))
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
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a> AsyncKVReadTransaction<'a> for WriteTransaction {
    async fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let table_name = table_name.to_string();
        let key = key.to_string();
        let operations = self.operations.clone();
        let mut table_names = HashSet::new();
        for (op_table_name, _) in &operations {
            table_names.insert(op_table_name.clone());
        }
        table_names.insert(table_name.clone());
        let name = self.name.clone();
        let version = self.version.clone();
        let get_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let mut db = db.write().await;

                // create object stores if they do not exist
                let new_table_names: Vec<String> = table_names
                    .difference(&db.object_store_names().into_iter().collect())
                    .cloned()
                    .collect();

                if !new_table_names.is_empty() {
                    db.close();

                    let new_version = version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

                    let new_table_names = new_table_names.clone();
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

                let table_name = table_name.to_string();
                let key = key.to_string();
                let value = Rc::new(RefCell::new(None));
                let value_clone = Rc::clone(&value);
                match db
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

                        // perform all queued operations first
                        for (op_table_name, op) in operations {
                            let table = if let Some(t) = tables.get(&op_table_name) {
                                t.clone()
                            } else {
                                let t = Rc::new(tx.object_store(&op_table_name)?);
                                tables.insert(op_table_name.clone(), Rc::clone(&t));
                                t
                            };

                            op(table).await?;
                        }

                        // now perform the get
                        let table = if let Some(t) = tables.get(&table_name) {
                            t.clone()
                        } else {
                            let t = Rc::new(tx.object_store(&table_name)?);
                            tables.insert(table_name.clone(), Rc::clone(&t));
                            t
                        };

                        value.replace(table.get(&JsValue::from(key)).await?);

                        Err::<(), indexed_db::Error<Infallible>>(indexed_db::Error::ReadOnly)
                    })
                    .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        if let indexed_db::Error::DoesNotExist = e {
                            return Ok(CommandResponse::Get(None));
                        } else if let indexed_db::Error::ReadOnly = e {
                            // Expected error to abort the transaction after reads
                        } else {
                            return Err(indexed_db_error_to_io_error(e));
                        }
                    }
                };

                // if new object stores were created, delete them, as we have not committed yet
                if !new_table_names.is_empty() {
                    db.close();

                    let new_version = version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    *db = Factory::get()
                        .map_err(indexed_db_error_to_io_error)?
                        .open::<Infallible>(
                            &name,
                            new_version,
                            move |evt: VersionChangeEvent<Infallible>| async move {
                                for table_name in new_table_names {
                                    evt.delete_object_store(&table_name)?;
                                }
                                Ok(())
                            },
                        )
                        .await
                        .map_err(indexed_db_error_to_io_error)?
                        .into_manual_close();
                }

                Ok::<_, std::io::Error>(CommandResponse::Get(
                    value_clone.take().map(|v| Uint8Array::from(v).to_vec()),
                ))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(get_closure), response_sender))
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
        let operations = self.operations.clone();
        let mut table_names = HashSet::new();
        for (op_table_name, _) in &operations {
            table_names.insert(op_table_name.clone());
        }
        table_names.insert(table_name.clone());
        let iter_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let table_name = table_name.to_string();
                let values = Rc::new(RefCell::new(Vec::new()));
                let values_clone = Rc::clone(&values);
                match db
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

                        // perform all queued operations first
                        for (op_table_name, op) in operations {
                            let table = if let Some(t) = tables.get(&op_table_name) {
                                t.clone()
                            } else {
                                let t = Rc::new(tx.object_store(&op_table_name)?);
                                tables.insert(op_table_name.clone(), Rc::clone(&t));
                                t
                            };

                            op(table).await?;
                        }

                        // now perform the iter
                        let table = if let Some(t) = tables.get(&table_name) {
                            t.clone()
                        } else {
                            let t = Rc::new(tx.object_store(&table_name)?);
                            tables.insert(table_name.clone(), Rc::clone(&t));
                            t
                        };

                        for key in table.get_all_keys(None).await? {
                            if let Some(value) = table.get(&key).await? {
                                let key = key.as_string().unwrap_or_default();
                                let value = Uint8Array::from(value).to_vec();
                                values.borrow_mut().push((key, value));
                            }
                        }

                        Err::<(), indexed_db::Error<Infallible>>(indexed_db::Error::ReadOnly)
                    })
                    .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        if let indexed_db::Error::DoesNotExist = e {
                            // Table does not exist, return empty iterator
                        } else if let indexed_db::Error::ReadOnly = e {
                            // Expected error to abort the transaction after reads
                        } else {
                            return Err(indexed_db_error_to_io_error(e));
                        }
                    }
                };

                Ok::<_, std::io::Error>(CommandResponse::Iter(values_clone.take()))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(iter_closure), response_sender))
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
        let operations = self.operations.clone();
        let mut table_names = HashSet::new();
        for (op_table_name, _) in &operations {
            table_names.insert(op_table_name.clone());
        }
        let table_names_closure = |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let mut table_names_list = HashSet::new();
                table_names_list.extend(db.object_store_names());
                let table_names_list = Rc::new(RefCell::new(table_names_list));
                let table_names_list_clone = Rc::clone(&table_names_list);
                if !table_names.is_empty() {
                    match db
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

                            // perform all queued operations first
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
                                            table_names_list.borrow_mut().insert(name);
                                        }
                                        TableUpdate::Deleted(name) => {
                                            table_names_list.borrow_mut().remove(&name);
                                        }
                                    }
                                }
                            }

                            Err::<(), indexed_db::Error<Infallible>>(indexed_db::Error::ReadOnly)
                        })
                        .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            if let indexed_db::Error::ReadOnly = e {
                                // Expected error to abort the transaction after reads
                            } else if let indexed_db::Error::DoesNotExist = e {
                                // This can happen if the database has no object stores
                            } else {
                                return Err(indexed_db_error_to_io_error(e));
                            }
                        }
                    }
                }

                Ok::<_, std::io::Error>(CommandResponse::TableNames(
                    table_names_list_clone.take().into_iter().collect(),
                ))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(table_names_closure), response_sender))
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
        let key = key.to_string();
        let value = value.to_vec();
        let op = move |table: Rc<ObjectStore<Infallible>>| -> Pin<
            Box<dyn Future<Output = Result<Option<TableUpdate>, indexed_db::Error<Infallible>>>>,
        > {
            let table_name = table_name_clone.to_string();
            let key = key.to_string();
            let value = value.clone();
            Box::pin(async move {
                table
                    .put_kv(&JsValue::from(key), &Uint8Array::from(value.as_ref()))
                    .await?;
                // If the table did not exist before, it will be created
                Ok(Some(TableUpdate::Created(table_name)))
            })
        };

        self.operations.push((table_name.to_string(), Arc::new(op)));

        Ok(old_value)
    }

    async fn remove(&mut self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let old_value = self.get(table_name, key).await?;

        let table_name = table_name.to_string();
        let key = key.to_string();
        let op = move |table: Rc<ObjectStore<Infallible>>| -> Pin<
            Box<dyn Future<Output = Result<Option<TableUpdate>, indexed_db::Error<Infallible>>>>,
        > {
            let key = key.to_string();
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

        self.operations.push((table_name, Arc::new(op)));

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

                    let new_version = version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

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

                    let new_version = version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

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
            .unbounded_send((Box::new(commit_closure), response_sender))
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
        })
    }
}
