use std::{io, path::Path, sync::Mutex};

use futures::channel::mpsc::UnboundedSender;
use redb::{
    CommitError, Database, DatabaseError, ReadableTable, StorageError, TableDefinition, TableError,
    TableHandle, TransactionError,
};

use crate::{KeyValueDB, RunBackupEvent, TABLE_VERSIONS};

#[derive(Debug)]
pub struct RedbDB {
    inner: Database,
    backup_notifier_sender: Mutex<Option<UnboundedSender<RunBackupEvent>>>,
}

impl RedbDB {
    pub fn open(path: &Path) -> io::Result<Self> {
        let inner = Database::create(path).map_err(database_error_to_io_error)?;

        Ok(Self {
            inner,
            backup_notifier_sender: Mutex::new(None),
        })
    }
}

impl KeyValueDB for RedbDB {
    fn add_backup_notifier_sender(&self, sender: UnboundedSender<RunBackupEvent>) {
        self.backup_notifier_sender.lock().unwrap().replace(sender);
    }

    fn restore_backup(
        &self,
        table_name: &str,
        data: Vec<(String, Vec<u8>)>,
        new_version: u32,
    ) -> io::Result<()> {
        self.delete_table(table_name)?; // do it in a single tx?

        let write_transaction = self
            .inner
            .begin_write()
            .map_err(transaction_error_to_io_error)?;
        {
            let mut table = write_transaction
                .open_table(TableDefinition::<&str, &[u8]>::new(table_name))
                .map_err(table_error_to_io_error)?;

            for (key, value) in data {
                table
                    .insert(&*key, &*value)
                    .map_err(storage_error_to_io_error)?;
            }
        }
        write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        let versions_write_transaction = self
            .inner
            .begin_write()
            .map_err(transaction_error_to_io_error)?;
        {
            let mut version_table = versions_write_transaction
                .open_table(TableDefinition::<&str, u32>::new(TABLE_VERSIONS))
                .map_err(table_error_to_io_error)?;

            version_table
                .insert(table_name, &new_version)
                .map_err(storage_error_to_io_error)?;
        }
        versions_write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        println!(
            "Backup for table {} restored with version {}",
            table_name, new_version
        );
        Ok(())
    }

    fn insert(&self, table_name: &str, key: &str, value: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let write_transaction = self
            .inner
            .begin_write()
            .map_err(transaction_error_to_io_error)?;

        let (old_value, new_version) = {
            let mut table = write_transaction
                .open_table(TableDefinition::<&str, &[u8]>::new(table_name))
                .map_err(table_error_to_io_error)?;

            let mut version_table = write_transaction
                .open_table(TableDefinition::<&str, u32>::new(TABLE_VERSIONS))
                .map_err(table_error_to_io_error)?;

            let current_version =  version_table
                .get(table_name)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value())
                .unwrap_or(0_u32);
        
            let new_version = current_version + 1;
       
            let old = table
                .insert(key, value)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value().to_vec());

            version_table
                .insert(table_name, new_version)
                .map_err(storage_error_to_io_error)?;

            (old, new_version)
        };

        write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        let read_transaction = self.inner.begin_read().unwrap();
        let version_table = read_transaction
            .open_table(TableDefinition::<&str, u32>::new(TABLE_VERSIONS))
            .unwrap();
        let version = version_table.get(table_name).unwrap();

        let sender = self.backup_notifier_sender.lock().unwrap();
        if let Some(sender) = sender.as_ref() {
            match sender.unbounded_send(RunBackupEvent::Insert((
                table_name.to_string(),
                new_version,
            ))) {
                Ok(_) => {
                }
                Err(err) => {
                    println!("Error sending backup event: {:#?}", err);
                }
            }
        }

        Ok(old_value)
    }

    // Method to get the current version of a table
    fn get_table_version(&self, table_name: &str) -> io::Result<Option<u32>> {
        let read_transaction = self
            .inner
            .begin_read()
            .map_err(transaction_error_to_io_error)?;

        let version_table = match read_transaction
            .open_table(TableDefinition::<&str, u32>::new(TABLE_VERSIONS))
        {
            Ok(table) => table,
            Err(err) => {
                println!("Error opening table_versions table: {:#?}", err);
                return Ok(None);
            }
        };
        println!("printing versions table {:?}", version_table);

        let version = version_table
            .get(table_name)
            .map_err(storage_error_to_io_error)?
            .map(|v| v.value());

        Ok(version)
    }

    fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let read_transaction = self
            .inner
            .begin_read()
            .map_err(transaction_error_to_io_error)?;
        let value = {
            let table_res =
                read_transaction.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
            let table = match table_res {
                Ok(table) => table,
                Err(TableError::TableDoesNotExist(_)) => {
                    return Ok(None);
                }
                Err(e) => return Err(table_error_to_io_error(e)),
            };
            let val = table
                .get(key)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value().to_vec());

            val
        };

        Ok(value)
    }

    fn remove(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let write_transaction = self
            .inner
            .begin_write()
            .map_err(transaction_error_to_io_error)?;
        let old_value = {
            let table_res =
                write_transaction.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
            let mut table = match table_res {
                Ok(table) => Some(table),
                Err(TableError::TableDoesNotExist(_)) => None,
                Err(e) => return Err(table_error_to_io_error(e)),
            };

            if let Some(table) = table.as_mut() {
                let old = table
                    .remove(key)
                    .map_err(storage_error_to_io_error)?
                    .map(|v| v.value().to_vec());
                old
            } else {
                None
            }
        };

        if old_value.is_none() {
            write_transaction
                .abort()
                .map_err(storage_error_to_io_error)?;
        } else {
            write_transaction
                .commit()
                .map_err(commit_error_to_io_error)?;
        }
        let sender = self.backup_notifier_sender.lock().unwrap();
        if let Some(sender) = sender.as_ref() {
            match sender.unbounded_send(RunBackupEvent::Delete((table_name.to_string(), 0))) {
                Ok(_) => {}
                Err(_) => {}
            }
        }

        Ok(old_value)
    }

    fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let read_transaction = self
            .inner
            .begin_read()
            .map_err(transaction_error_to_io_error)?;
        let table_res =
            read_transaction.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
        let table = match table_res {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(table_error_to_io_error(e)),
        };
        let mut result = Vec::new();
        for item in table.iter().map_err(storage_error_to_io_error)? {
            let (key, value) = item.map_err(storage_error_to_io_error)?;
            result.push((key.value().to_string(), value.value().to_vec()));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let read_transaction = self
            .inner
            .begin_read()
            .map_err(transaction_error_to_io_error)?;
        let mut result = Vec::new();
        let tables_res = read_transaction.list_tables();
        match tables_res {
            Ok(tables) => {
                for table_name in tables {
                    result.push(table_name.name().to_string());
                }
            }
            Err(StorageError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(storage_error_to_io_error(e));
            }
        }
        Ok(result)
    }

    fn delete_table(&self, table_name: &str) -> io::Result<()> {
        let write_transaction = self
            .inner
            .begin_write()
            .map_err(transaction_error_to_io_error)?;
        write_transaction
            .delete_table(TableDefinition::<&str, &[u8]>::new(table_name))
            .map_err(table_error_to_io_error)?;
        write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        Ok(())
    }
}

fn storage_error_to_io_error(e: StorageError) -> io::Error {
    match e {
        StorageError::Io(e) => e,
        StorageError::ValueTooLarge(size) => io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Value is too large: {}", size),
        ),
        StorageError::Corrupted(e) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Database is corrupted: {}", e),
        ),
        StorageError::LockPoisoned(location) => io::Error::new(
            io::ErrorKind::Other,
            format!("Database lock is poisoned: {}", location),
        ),
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}

fn database_error_to_io_error(e: DatabaseError) -> io::Error {
    match e {
        DatabaseError::Storage(e) => storage_error_to_io_error(e),
        DatabaseError::DatabaseAlreadyOpen => {
            io::Error::new(io::ErrorKind::Other, "Database is already open")
        }
        DatabaseError::RepairAborted => {
            io::Error::new(io::ErrorKind::Other, "Database repair was aborted")
        }
        DatabaseError::UpgradeRequired(version) => io::Error::new(
            io::ErrorKind::Other,
            format!("Database upgrade required to version {}", version),
        ),
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}

fn transaction_error_to_io_error(e: TransactionError) -> io::Error {
    match e {
        TransactionError::Storage(e) => storage_error_to_io_error(e),
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}

fn table_error_to_io_error(e: TableError) -> io::Error {
    match e {
        TableError::Storage(e) => storage_error_to_io_error(e),
        TableError::TableAlreadyOpen(name, location) => io::Error::new(
            io::ErrorKind::Other,
            format!("Table {} is already open: {}", name, location),
        ),
        TableError::TableDoesNotExist(name) => io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table {} does not exist", name),
        ),
        TableError::TableIsMultimap(name) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Table {} is a multimap", name),
        ),
        TableError::TableIsNotMultimap(name) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Table {} is not a multimap", name),
        ),
        TableError::TableTypeMismatch { table, key, value } => io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Table {} is not a table of type <{:?}, {:?}>",
                table, key, value
            ),
        ),
        TableError::TypeDefinitionChanged {
            name,
            alignment,
            width,
        } => io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Type definition for {:?} changed. Expected alignment {}, width {:?}",
                name, alignment, width
            ),
        ),
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}

fn commit_error_to_io_error(e: CommitError) -> io::Error {
    match e {
        CommitError::Storage(e) => storage_error_to_io_error(e),
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}
