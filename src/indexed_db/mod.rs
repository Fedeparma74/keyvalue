use std::{io, sync::atomic::AtomicU32};

use async_trait::async_trait;
use futures::{channel::mpsc::UnboundedSender, lock::Mutex};
use indexed_db::{Database, Factory};
use js_sys::{wasm_bindgen::JsValue, Uint8Array};

use crate::{AsyncKeyValueDB, RunBackupEvent, TABLE_VERSIONS};

#[derive(Debug)]
pub struct IndexedDB {
    name: String,
    version: AtomicU32,
    inner: Mutex<Database<()>>,
    backup_notifier_sender: Mutex<Option<UnboundedSender<RunBackupEvent>>>,
}

// Safety: It is safe to implement Send and Sync for IndexedDB because
// it can only be used in a browser environment, that is single-threaded.
unsafe impl Send for IndexedDB {}
unsafe impl Sync for IndexedDB {}

impl IndexedDB {
    pub async fn open(db_name: &str) -> io::Result<Self> {
        let db = Factory::get()
            .map_err(indexed_db_error_to_io_error)?
            .open_latest_version(db_name)
            .await
            .map_err(indexed_db_error_to_io_error)?;

        Ok(Self {
            name: db_name.to_string(),
            version: AtomicU32::new(db.version()),
            inner: Mutex::new(db),
            backup_notifier_sender: Mutex::new(None),
        })
    }
}

#[async_trait(?Send)]
impl AsyncKeyValueDB for IndexedDB {
    async fn add_backup_notifier_sender(&self, sender: UnboundedSender<RunBackupEvent>) {
        self.backup_notifier_sender.lock().await.replace(sender);
    }

    async fn get_table_version(&self, table_name: &str) -> io::Result<Option<u32>> {
        let db = self.inner.lock().await;

        let table_name = table_name.to_string();
        let value = match db
            .transaction(&[TABLE_VERSIONS])
            .run(move |tx| async move {
                let table = tx.object_store(TABLE_VERSIONS)?;
                let value = table.get(&JsValue::from(table_name)).await?;
                Ok::<_, indexed_db::Error<()>>(value)
            })
            .await
            .map_err(indexed_db_error_to_io_error)
        {
            Ok(Some(value)) => value.as_f64().unwrap_or(0.0) as u32,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
            Ok(None) => 0_u32,
        };

        Ok(Some(value))
    }

    async fn restore_backup(
        &self,
        table_name: &str,
        data: Vec<(String, Vec<u8>)>,
        new_version: u32,
    ) -> io::Result<()> {
        let table_name_str = table_name.to_string();
        let table_name_str_clone = table_name_str.clone();

        // Delete the existing table
        self.delete_table(table_name).await?;

        // Increment the version counter only once
        let new_version_indexdb_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;

        // Lock the database and perform operations
        let mut db = self.inner.lock().await;

        // Reopen the database with the new version
        db.close();
        *db = Factory::get()
            .map_err(indexed_db_error_to_io_error)?
            .open(
                &self.name,
                new_version_indexdb_version,
                move |evt| async move {
                    let db = evt.database();
                    db.build_object_store(&table_name_str).create()?;
                    Ok(())
                },
            )
            .await
            .map_err(indexed_db_error_to_io_error)?;

        // Insert the backup data into the table
        for (key, value) in data {
            let table_name_str = table_name.to_string();

            db.transaction(&[&table_name_str])
                .rw()
                .run(move |tx| async move {
                    let table = tx.object_store(&table_name_str)?;
                    table
                        .put_kv(
                            &JsValue::from(key),
                            &Uint8Array::from(value.as_ref()).into(),
                        )
                        .await?;
                    Ok::<_, indexed_db::Error<()>>(())
                })
                .await
                .map_err(indexed_db_error_to_io_error)?;
        }

        // Check if the `table_versions` object store exists
        let new_version_indexdb_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let version_table_name = TABLE_VERSIONS.to_string();
        let version_table_name_clone = version_table_name.clone();
        if !db
            .object_store_names()
            .into_iter()
            .any(|n| n == TABLE_VERSIONS)
        {
            tracing::info!("Creating table_versions object store");

            db.close();
            *db = Factory::get()
                .map_err(indexed_db_error_to_io_error)?
                .open(
                    &self.name,
                    new_version_indexdb_version,
                    move |evt| async move {
                        let db = evt.database();
                        db.build_object_store(&version_table_name).create()?;
                        Ok(())
                    },
                )
                .await
                .map_err(indexed_db_error_to_io_error)?;
        }

        // Update the version in the `table_versions` object store
        db.transaction(&[&version_table_name_clone])
            .rw()
            .run(move |tx| async move {
                let table = tx.object_store(&version_table_name_clone)?;
                table
                    .put_kv(
                        &JsValue::from(table_name_str_clone),
                        &Uint8Array::from(new_version.to_be_bytes().as_ref()).into(),
                    )
                    .await?;
                Ok::<_, indexed_db::Error<()>>(())
            })
            .await
            .map_err(indexed_db_error_to_io_error)?;
            
        tracing::info!(
            "Backup for table {} restored with version {}",
            table_name,
            new_version
        );

        Ok(())
    }

    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let old_value = self.get(table_name, key).await?;
        let new_version_indexdb_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let mut db = self.inner.lock().await;
        tracing::info!(
            "Inserting into table {} with key {} for indexdb version {}",
            table_name,
            key,
            new_version_indexdb_version
        );
        if !db.object_store_names().into_iter().any(|n| n == table_name) {
            db.close();

            let table_name_str = table_name.to_string();

            *db = Factory::get()
                .map_err(indexed_db_error_to_io_error)?
                .open(
                    &self.name,
                    new_version_indexdb_version,
                    move |evt| async move {
                        let db = evt.database();
                        db.build_object_store(&table_name_str).create()?;
                        Ok(())
                    },
                )
                .await
                .map_err(indexed_db_error_to_io_error)?;
        }

        let table_name = table_name.to_string();
        let table_name_clone = table_name.to_string().clone();
        let table_name_clone2 = table_name.to_string().clone();

        let key = key.to_string();
        let value = value.to_vec();
        db.transaction(&[&table_name])
            .rw()
            .run(move |tx| async move {
                let table = tx.object_store(&table_name)?;
                table
                    .put_kv(
                        &JsValue::from(key),
                        &Uint8Array::from(value.as_ref()).into(),
                    )
                    .await?;
                Ok::<_, indexed_db::Error<()>>(())
            })
            .await
            .map_err(indexed_db_error_to_io_error)?;

        // check if version_table exists

        if !db
            .object_store_names()
            .into_iter()
            .any(|n| n == TABLE_VERSIONS)
        {
            db.close();
            tracing::info!("Creating table_versions object store");
            let new_version_indexdb_version = self
                .version
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let table_name_str = TABLE_VERSIONS.to_string();

            *db = Factory::get()
                .map_err(indexed_db_error_to_io_error)?
                .open(
                    &self.name,
                    new_version_indexdb_version,
                    move |evt| async move {
                        let db = evt.database();
                        db.build_object_store(&table_name_str).create()?;
                        Ok(())
                    },
                )
                .await
                .map_err(indexed_db_error_to_io_error)?;
        }

        let version_table_name = TABLE_VERSIONS.to_string();
        let task_version_table_name = version_table_name.clone();
        let value = match db
            .transaction(&[&version_table_name])
            .run(move |tx| async move {
                let table = tx.object_store(&task_version_table_name)?;
                let value = table.get(&JsValue::from(table_name_clone)).await?;
                Ok::<_, indexed_db::Error<()>>(value)
            })
            .await
            .map_err(indexed_db_error_to_io_error)
        {
            Ok(Some(value)) => {
                let value = Uint8Array::from(value).to_vec();
                u32::from_be_bytes(value.as_slice().try_into().unwrap())
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    tracing::info!("Table version not found, setting to 0");
                    0_u32
                } else {
                    tracing::error!("Error getting table version: {:?}", e);
                    return Err(e);
                }
            }
            Ok(None) => 0_u32,
        };

        let new_version = value + 1;
        tracing::info!(
            "Inserting into table_versions with key {} for table version {}",
            table_name_clone2,
            new_version
        );
        let table_name_clone_3 = table_name_clone2.clone();
        db.transaction(&[&version_table_name])
            .rw()
            .run(move |tx| async move {
                let table = tx.object_store(&version_table_name)?;
                table
                    .put_kv(
                        &JsValue::from(table_name_clone2),
                        &Uint8Array::from(new_version.to_be_bytes().as_ref()).into(),
                    )
                    .await?;
                Ok::<_, indexed_db::Error<()>>(())
            })
            .await
            .map_err(indexed_db_error_to_io_error)?;

        let backup_notifier_sender = self.backup_notifier_sender.lock().await;
        if let Some(sender) = &*backup_notifier_sender {
            let event = RunBackupEvent::Insert((table_name_clone_3, new_version));
            match sender.unbounded_send(event) {
                Ok(_) => {}
                Err(_) => {}
            }
        }

        Ok(old_value)
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let db = self.inner.lock().await;

        let table_name = table_name.to_string();
        let key = key.to_string();
        let value = match db
            .transaction(&[&table_name])
            .run(move |tx| async move {
                let table = tx.object_store(&table_name)?;
                let value = table.get(&JsValue::from(key)).await?;
                Ok::<_, indexed_db::Error<()>>(value)
            })
            .await
            .map_err(indexed_db_error_to_io_error)
        {
            Ok(value) => value,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
        };

        Ok(value.map(|v| Uint8Array::from(v).to_vec()))
    }

    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if let Some(old_value) = self.get(table_name, key).await? {
            let mut db = self.inner.lock().await;

            if !db.object_store_names().into_iter().any(|n| n == table_name) {
                db.close();

                let table_name_str = table_name.to_string();
                let new_version = self
                    .version
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                *db = Factory::get()
                    .map_err(indexed_db_error_to_io_error)?
                    .open(&self.name, new_version, move |evt| async move {
                        let db = evt.database();
                        db.build_object_store(&table_name_str).create()?;
                        Ok(())
                    })
                    .await
                    .map_err(indexed_db_error_to_io_error)?;
            }

            let table_name = table_name.to_string();
            let key = key.to_string();
            if let Err(e) = db
                .transaction(&[&table_name])
                .rw()
                .run(move |tx| async move {
                    let table = tx.object_store(&table_name)?;
                    table.delete(&JsValue::from(key)).await?;
                    Ok::<_, indexed_db::Error<()>>(())
                })
                .await
                .map_err(indexed_db_error_to_io_error)
            {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            };

            Ok(Some(old_value))
        } else {
            Ok(None)
        }
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let db = self.inner.lock().await;

        let table_name = table_name.to_string();
        let values = match db
            .transaction(&[&table_name])
            .run(move |tx| async move {
                let table = tx.object_store(&table_name)?;
                let mut key_values = Vec::new();
                for key in table.get_all_keys(None).await? {
                    if let Some(value) = table.get(&key).await? {
                        let key = key.as_string().unwrap_or_default();
                        let value = Uint8Array::from(value).to_vec();
                        key_values.push((key, value));
                    }
                }

                Ok::<_, indexed_db::Error<()>>(key_values)
            })
            .await
            .map_err(indexed_db_error_to_io_error)
        {
            Ok(values) => values,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(Vec::new());
                } else {
                    return Err(e);
                }
            }
        };

        Ok(values)
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let db = self.inner.lock().await;
        Ok(db.object_store_names())
    }

    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        let mut db = self.inner.lock().await;

        if db.object_store_names().into_iter().any(|n| n == table_name) {
            db.close();

            let table_name_str = table_name.to_string();
            let new_version = self
                .version
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            *db = Factory::get()
                .map_err(indexed_db_error_to_io_error)?
                .open(&self.name, new_version, move |evt| async move {
                    let db = evt.database();
                    db.delete_object_store(&table_name_str)?;
                    Ok(())
                })
                .await
                .map_err(indexed_db_error_to_io_error)?;
        }

        Ok(())
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        let db = self.inner.lock().await;

        let table_name = table_name.to_string();
        let key = key.to_string();
        let contains_key = match db
            .transaction(&[&table_name])
            .run(move |tx| async move {
                let table = tx.object_store(&table_name)?;
                let contains_key = table.contains(&JsValue::from(key)).await?;
                Ok::<_, indexed_db::Error<()>>(contains_key)
            })
            .await
            .map_err(indexed_db_error_to_io_error)
        {
            Ok(contains_key) => contains_key,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(false);
                } else {
                    return Err(e);
                }
            }
        };

        Ok(contains_key)
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let db = self.inner.lock().await;

        let table_name = table_name.to_string();
        let keys = match db
            .transaction(&[&table_name])
            .run(move |tx| async move {
                let table = tx.object_store(&table_name)?;
                let mut keys = Vec::new();
                for key in table.get_all_keys(None).await? {
                    keys.push(key.as_string().unwrap_or_default());
                }

                Ok::<_, indexed_db::Error<()>>(keys)
            })
            .await
            .map_err(indexed_db_error_to_io_error)
        {
            Ok(keys) => keys,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(Vec::new());
                } else {
                    return Err(e);
                }
            }
        };

        Ok(keys)
    }

    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let db = self.inner.lock().await;

        let table_name = table_name.to_string();
        let values = match db
            .transaction(&[&table_name])
            .run(move |tx| async move {
                let table = tx.object_store(&table_name)?;
                let mut values = Vec::new();
                for value in table.get_all(None).await? {
                    values.push(Uint8Array::from(value).to_vec());
                }

                Ok::<_, indexed_db::Error<()>>(values)
            })
            .await
            .map_err(indexed_db_error_to_io_error)
        {
            Ok(values) => values,
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    return Ok(Vec::new());
                } else {
                    return Err(e);
                }
            }
        };

        Ok(values)
    }

    async fn clear(&self) -> io::Result<()> {
        let mut db = self.inner.lock().await;
        db.close();

        Factory::get()
            .map_err(indexed_db_error_to_io_error)?
            .delete_database(&self.name)
            .await
            .map_err(indexed_db_error_to_io_error)?;

        *db = Factory::get()
            .map_err(indexed_db_error_to_io_error)?
            .open_latest_version(&self.name)
            .await
            .map_err(indexed_db_error_to_io_error)?;

        self.version
            .store(db.version(), std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }
}

fn indexed_db_error_to_io_error(e: indexed_db::Error<()>) -> io::Error {
    match e {
        indexed_db::Error::AlreadyExists => {
            io::Error::new(io::ErrorKind::AlreadyExists, format!("{:?}", e))
        }
        indexed_db::Error::DatabaseIsClosed => {
            io::Error::new(io::ErrorKind::NotConnected, format!("{:?}", e))
        }
        indexed_db::Error::DoesNotExist => {
            io::Error::new(io::ErrorKind::NotFound, format!("{:?}", e))
        }
        indexed_db::Error::FailedClone => io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
        indexed_db::Error::IndexedDbDisabled => {
            io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
        }
        indexed_db::Error::InvalidArgument => {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e))
        }
        indexed_db::Error::InvalidCall => {
            io::Error::new(io::ErrorKind::PermissionDenied, format!("{:?}", e))
        }
        indexed_db::Error::InvalidKey => {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e))
        }
        indexed_db::Error::InvalidRange => {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e))
        }
        indexed_db::Error::NotInBrowser => io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
        indexed_db::Error::ObjectStoreWasRemoved => {
            io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
        }
        indexed_db::Error::OperationNotAllowed => {
            io::Error::new(io::ErrorKind::PermissionDenied, format!("{:?}", e))
        }
        indexed_db::Error::OperationNotSupported => {
            io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
        }
        indexed_db::Error::ReadOnly => {
            io::Error::new(io::ErrorKind::PermissionDenied, format!("{:?}", e))
        }
        indexed_db::Error::User(e) => io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
        indexed_db::Error::VersionMustNotBeZero => {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e))
        }
        indexed_db::Error::VersionTooOld => {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", e))
        }
        e => io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
    }
}
