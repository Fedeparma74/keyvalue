//! Browser [IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API)-backed
//! key-value store (**WASM-only**, async-only).
//!
//! IndexedDB is a transactional, object-oriented database built into every
//! modern browser.  This module wraps it behind a command-channel pattern:
//! a background task (spawned via [`wasmt`]) owns the database connection and
//! processes requests sequentially, making the API safe to use from any async
//! context.
//!
//! Tables are mapped to IndexedDB *object stores*.  Because object stores
//! can only be created/deleted during a *version change* event, table
//! creation and deletion bump the database version and re-open the
//! connection.  This makes structural changes (create/drop table) heavier
//! than data operations.
//!
//! ## Feature flags
//!
//! * `indexed-db` — Enables this module and pulls in the `indexed-db`,
//!   `js-sys` and `wasmt` crates.
//! * `transactional` — Adds [`AsyncTransactionalKVDB`](crate::AsyncTransactionalKVDB)
//!   support with read/write transaction types.
//!
//! ## Threading
//!
//! When the WASM binary is compiled with `target-feature = +atomics,
//! +bulk-memory, +mutable-globals`, the background task is spawned on a
//! Web Worker via [`wasmt::task::spawn`]; otherwise [`wasmt::task::spawn_local`]
//! is used.

use core::{convert::Infallible, pin::Pin};
use std::{
    io,
    rc::Rc,
    sync::{Arc, atomic::AtomicU32},
};

use async_trait::async_trait;
use futures::{
    FutureExt, StreamExt,
    channel::{mpsc::UnboundedSender, oneshot},
};
use indexed_db::{Database, Factory, Transaction, VersionChangeEvent};
use js_sys::{Uint8Array, wasm_bindgen::JsValue};
use tokio::sync::RwLock;

use crate::AsyncKeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

type CommandRequestClosure = Box<
    dyn FnOnce(
            Rc<RwLock<Database>>,
        ) -> Pin<Box<dyn Future<Output = Result<CommandResponse, std::io::Error>>>>
        + Send,
>;

enum CommandResponse {
    Insert,
    Get(Option<Vec<u8>>),
    Remove(Option<Vec<u8>>),
    Iter(Vec<(String, Vec<u8>)>),
    TableNames(Vec<String>),
    DeleteTable,
    ContainsKey(bool),
    Keys(Vec<String>),
    Values(Vec<Vec<u8>>),
    Clear,
    Commit,
    Recover,
    Error(std::io::Error),
}

/// Configuration for an [`IndexedDB`] instance.
///
/// Use [`Default`] is not provided because `db_name` is always required.
#[derive(Debug, Clone)]
pub struct IndexedDBConfig {
    /// Database name passed to the browser IndexedDB API.
    pub db_name: String,
}

impl IndexedDBConfig {
    /// Creates a new config with the given database name.
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }
}

/// Async key-value database backed by the browser's IndexedDB API (**WASM-only**).
///
/// Internally runs a single-threaded event loop that serialises all
/// IndexedDB operations through a command channel.  This ensures that
/// version-change transactions (needed to create/delete object stores) are
/// never interleaved with data transactions.
///
/// Created via [`IndexedDB::open`].  The database connection is closed
/// automatically when the struct is dropped.
pub struct IndexedDB {
    name: String,
    version: Arc<AtomicU32>,
    idb_dropper: Option<oneshot::Sender<()>>,
    task_handle: wasmt::task::r#async::JoinHandle<()>,
    command_request_sender:
        UnboundedSender<(CommandRequestClosure, oneshot::Sender<CommandResponse>)>,
}

impl Drop for IndexedDB {
    fn drop(&mut self) {
        // Close the database connection
        self.idb_dropper.take().map(|sender| sender.send(()).ok());
        self.task_handle.abort();
    }
}

impl IndexedDB {
    pub async fn open(db_name: &str) -> io::Result<Self> {
        let (init_res_sender, init_res_receiver) =
            futures::channel::oneshot::channel::<io::Result<u32>>();

        let (command_sender, mut command_receiver) = futures::channel::mpsc::unbounded::<(
            CommandRequestClosure,
            oneshot::Sender<CommandResponse>,
        )>();
        let (idb_dropper, mut idb_dropper_receiver) = futures::channel::oneshot::channel::<()>();

        let db_name_clone = db_name.to_string();
        let idb_task = async move {
            let db_factory = match Factory::get() {
                Ok(factory) => factory,
                Err(e) => {
                    init_res_sender
                        .send(Err(indexed_db_error_to_io_error(e)))
                        .ok();
                    return;
                }
            };

            let (db_version, db) = match db_factory.open_latest_version(&db_name_clone).await {
                Ok(db) => (db.version(), Rc::new(RwLock::new(db))),
                Err(e) => {
                    init_res_sender
                        .send(Err(indexed_db_error_to_io_error(e)))
                        .ok();
                    return;
                }
            };

            init_res_sender.send(Ok(db_version)).ok();

            loop {
                futures::select! {
                    (command, command_response_sender) = command_receiver.select_next_some() => {
                        match command(db.clone()).await {
                            Ok(response) => {
                                command_response_sender.send(response).ok();
                            }
                            Err(e) => {
                                command_response_sender.send(CommandResponse::Error(e)).ok();
                            }
                        }
                    }
                    _ = idb_dropper_receiver => {
                        db.read().await.close();
                        return;
                    }
                }
            }
        };

        #[cfg(all(
            target_feature = "atomics",
            target_feature = "bulk-memory",
            target_feature = "mutable-globals"
        ))]
        let task_handle = wasmt::task::spawn(idb_task);

        #[cfg(not(all(
            target_feature = "atomics",
            target_feature = "bulk-memory",
            target_feature = "mutable-globals"
        )))]
        let task_handle = wasmt::task::spawn_local(idb_task);

        let db_version = init_res_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive initialization response"))??;

        Ok(Self {
            name: db_name.to_string(),
            version: Arc::new(AtomicU32::new(db_version)),
            idb_dropper: Some(idb_dropper),
            task_handle,
            command_request_sender: command_sender,
        })
    }

    /// Opens an IndexedDB database with custom [`IndexedDBConfig`].
    pub async fn open_with_config(config: IndexedDBConfig) -> io::Result<Self> {
        Self::open(&config.db_name).await
    }

    /// Attempt to recover from an unrecoverable database error by
    /// closing the current connection and re-opening from the browser.
    pub async fn try_recover_from_error(&self) -> io::Result<()> {
        let name = self.name.clone();
        let version = self.version.clone();

        let recover_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let mut db_guard = db.write().await;
                db_guard.close();

                let new_db = Factory::get()
                    .map_err(indexed_db_error_to_io_error)?
                    .open_latest_version(&name)
                    .await
                    .map_err(indexed_db_error_to_io_error)?;

                let db_version = new_db.version();
                version.store(db_version, std::sync::atomic::Ordering::SeqCst);

                *db_guard = new_db;

                Ok::<_, std::io::Error>(CommandResponse::Recover)
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(recover_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send recovery command"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive recovery response"))?;

        match response {
            CommandResponse::Recover => Ok(()),
            CommandResponse::Error(e) => Err(e),
            _ => Err(io::Error::other("Unexpected response type")),
        }
    }
}

#[async_trait(?Send)]
impl AsyncKeyValueDB for IndexedDB {
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let old_value = self.get(table_name, key).await?;

        let name = self.name.clone();
        let version = self.version.clone();
        let table_name = table_name.to_string();
        let key = key.to_string();
        let value = value.to_vec();
        let insert_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let mut db = db.write().await;
                if !db.object_store_names().into_iter().any(|n| n == table_name) {
                    db.close();

                    let table_name_str = table_name.to_string();
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
                                evt.build_object_store(&table_name_str).create()?;
                                Ok(())
                            },
                        )
                        .await
                        .map_err(indexed_db_error_to_io_error)?
                        .into_manual_close();
                }

                let table_name = table_name.to_string();
                let key = key.to_string();
                let value = value.to_vec();
                db.transaction(&[&table_name])
                    .rw()
                    .run::<_, Infallible>(move |tx: Transaction<Infallible>| async move {
                        let table = tx.object_store(&table_name)?;
                        table
                            .put_kv(
                                &JsValue::from(key),
                                &Uint8Array::from(value.as_ref()).into(),
                            )
                            .await?;
                        Ok(())
                    })
                    .await
                    .map_err(indexed_db_error_to_io_error)?;

                Ok::<_, std::io::Error>(CommandResponse::Insert)
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(insert_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::Insert = response {
            return Ok(old_value);
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
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

    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if let Some(old_value) = self.get(table_name, key).await? {
            let name = self.name.clone();
            let version = self.version.clone();
            let table_name = table_name.to_string();
            let key = key.to_string();

            let remove_closure = move |db: Rc<RwLock<Database>>| {
                async move {
                    let mut db = db.write().await;
                    if !db.object_store_names().into_iter().any(|n| n == table_name) {
                        db.close();

                        let table_name_str = table_name.to_string();
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
                                    evt.build_object_store(&table_name_str).create()?;
                                    Ok(())
                                },
                            )
                            .await
                            .map_err(indexed_db_error_to_io_error)?
                            .into_manual_close();
                    }

                    let table_name = table_name.to_string();
                    let key = key.to_string();
                    if let Err(e) = db
                        .transaction(&[&table_name])
                        .rw()
                        .run::<(), Infallible>(move |tx: Transaction<Infallible>| async move {
                            let table = tx.object_store(&table_name)?;
                            table.delete(&JsValue::from(key)).await?;
                            Ok(())
                        })
                        .await
                        .map_err(indexed_db_error_to_io_error)
                    {
                        if e.kind() == io::ErrorKind::NotFound {
                            return Ok(CommandResponse::Remove(None));
                        } else {
                            return Err(e);
                        }
                    };

                    Ok::<_, std::io::Error>(CommandResponse::Remove(Some(old_value)))
                }
                .boxed_local()
            };

            let (response_sender, response_receiver) = oneshot::channel();
            self.command_request_sender
                .unbounded_send((Box::new(remove_closure), response_sender))
                .map_err(|_| io::Error::other("Failed to send command request"))?;

            let response = response_receiver
                .await
                .map_err(|_| io::Error::other("Failed to receive command response"))?;

            if let CommandResponse::Remove(value) = response {
                return Ok(value);
            }
            if let CommandResponse::Error(e) = response {
                return Err(e);
            }

            Err(io::Error::other("Unexpected response type"))
        } else {
            Ok(None)
        }
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
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

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
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

    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        let name = self.name.clone();
        let version = self.version.clone();
        let table_name = table_name.to_string();
        let delete_table_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let mut db = db.write().await;
                if db.object_store_names().into_iter().any(|n| n == table_name) {
                    db.close();

                    let table_name_str = table_name.to_string();
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
                                evt.delete_object_store(&table_name_str)?;
                                Ok(())
                            },
                        )
                        .await
                        .map_err(indexed_db_error_to_io_error)?
                        .into_manual_close();
                }

                Ok::<_, std::io::Error>(CommandResponse::DeleteTable)
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(delete_table_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::DeleteTable = response {
            return Ok(());
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        let table_name = table_name.to_string();
        let key = key.to_string();
        let contains_key_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let table_name = table_name.to_string();
                let key = key.to_string();
                let contains_key = match db
                    .transaction(&[&table_name])
                    .run(move |tx: Transaction<Infallible>| async move {
                        let table = tx.object_store(&table_name)?;
                        let contains_key = table.contains(&JsValue::from(key)).await?;
                        Ok(contains_key)
                    })
                    .await
                    .map_err(indexed_db_error_to_io_error)
                {
                    Ok(contains_key) => contains_key,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            return Ok(CommandResponse::ContainsKey(false));
                        } else {
                            return Err(e);
                        }
                    }
                };

                Ok::<_, std::io::Error>(CommandResponse::ContainsKey(contains_key))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(contains_key_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::ContainsKey(contains_key) = response {
            return Ok(contains_key);
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let table_name = table_name.to_string();
        let keys_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let table_name = table_name.to_string();
                let keys = match db
                    .transaction(&[&table_name])
                    .run(move |tx: Transaction<Infallible>| async move {
                        let table = tx.object_store(&table_name)?;
                        let mut keys = Vec::new();
                        for key in table.get_all_keys(None).await? {
                            keys.push(key.as_string().unwrap_or_default());
                        }

                        Ok(keys)
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

                Ok::<_, std::io::Error>(CommandResponse::Keys(keys))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(keys_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::Keys(keys) = response {
            return Ok(keys);
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let table_name = table_name.to_string();
        let values_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let db = db.read().await;
                let table_name = table_name.to_string();
                let values = match db
                    .transaction(&[&table_name])
                    .run(move |tx: Transaction<Infallible>| async move {
                        let table = tx.object_store(&table_name)?;
                        let mut values = Vec::new();
                        for value in table.get_all(None).await? {
                            values.push(Uint8Array::from(value).to_vec());
                        }

                        Ok(values)
                    })
                    .await
                    .map_err(indexed_db_error_to_io_error)
                {
                    Ok(values) => values,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::NotFound {
                            return Ok(CommandResponse::Values(Vec::new()));
                        } else {
                            return Err(e);
                        }
                    }
                };

                Ok::<_, std::io::Error>(CommandResponse::Values(values))
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(values_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::Values(values) = response {
            return Ok(values);
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }

    async fn clear(&self) -> io::Result<()> {
        let name = self.name.clone();
        let version = self.version.clone();
        let clear_closure = move |db: Rc<RwLock<Database>>| {
            async move {
                let mut db = db.write().await;
                db.close();

                Factory::get()
                    .map_err(indexed_db_error_to_io_error)?
                    .delete_database(&name)
                    .await
                    .map_err(indexed_db_error_to_io_error)?;

                *db = Factory::get()
                    .map_err(indexed_db_error_to_io_error)?
                    .open_latest_version(&name)
                    .await
                    .map_err(indexed_db_error_to_io_error)?;

                version.store(db.version(), std::sync::atomic::Ordering::SeqCst);

                Ok::<_, std::io::Error>(CommandResponse::Clear)
            }
            .boxed_local()
        };

        let (response_sender, response_receiver) = oneshot::channel();
        self.command_request_sender
            .unbounded_send((Box::new(clear_closure), response_sender))
            .map_err(|_| io::Error::other("Failed to send command request"))?;

        let response = response_receiver
            .await
            .map_err(|_| io::Error::other("Failed to receive command response"))?;

        if let CommandResponse::Clear = response {
            return Ok(());
        }
        if let CommandResponse::Error(e) = response {
            return Err(e);
        }

        Err(io::Error::other("Unexpected response type"))
    }
}

fn indexed_db_error_to_io_error<T>(e: indexed_db::Error<T>) -> io::Error
where
    T: std::fmt::Debug,
{
    match e {
        indexed_db::Error::DoesNotExist => {
            io::Error::new(io::ErrorKind::NotFound, format!("{:?}", e))
        }
        indexed_db::Error::AlreadyExists => {
            io::Error::new(io::ErrorKind::AlreadyExists, format!("{:?}", e))
        }
        indexed_db::Error::DatabaseIsClosed => {
            io::Error::new(io::ErrorKind::NotConnected, format!("{:?}", e))
        }
        indexed_db::Error::InvalidArgument
        | indexed_db::Error::InvalidKey
        | indexed_db::Error::InvalidRange
        | indexed_db::Error::VersionMustNotBeZero
        | indexed_db::Error::VersionTooOld => {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e))
        }
        indexed_db::Error::InvalidCall
        | indexed_db::Error::OperationNotAllowed
        | indexed_db::Error::ReadOnly => {
            io::Error::new(io::ErrorKind::PermissionDenied, format!("{:?}", e))
        }
        indexed_db::Error::FailedClone
        | indexed_db::Error::IndexedDbDisabled
        | indexed_db::Error::NotInBrowser
        | indexed_db::Error::ObjectStoreWasRemoved
        | indexed_db::Error::OperationNotSupported
        | indexed_db::Error::User(_) => io::Error::other(format!("{:?}", e)),
        e => io::Error::other(format!("{:?}", e)),
    }
}
