use std::{io, path::Path};

use ::redb::{CommitError, Database, DatabaseError, StorageError, TableError, TransactionError};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};

use crate::KeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[derive(Debug)]
pub struct RedbDB {
    inner: Database,
}

impl RedbDB {
    pub fn open(path: &Path) -> io::Result<Self> {
        let inner = Database::create(path).map_err(database_error_to_io_error)?;

        Ok(Self { inner })
    }
}

impl KeyValueDB for RedbDB {
    fn insert(&self, table_name: &str, key: &str, value: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let write_transaction = self
            .inner
            .begin_write()
            .map_err(transaction_error_to_io_error)?;
        let old_value = {
            let mut table = write_transaction
                .open_table(TableDefinition::<&str, &[u8]>::new(table_name))
                .map_err(table_error_to_io_error)?;

            table
                .insert(key, value)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value().to_vec())
        };
        write_transaction
            .commit()
            .map_err(commit_error_to_io_error)?;

        Ok(old_value)
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

            table
                .get(key)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value().to_vec())
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
                table
                    .remove(key)
                    .map_err(storage_error_to_io_error)?
                    .map(|v| v.value().to_vec())
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
        StorageError::LockPoisoned(location) => {
            io::Error::other(format!("Database lock is poisoned: {}", location))
        }
        e => io::Error::other(e),
    }
}

fn database_error_to_io_error(e: DatabaseError) -> io::Error {
    match e {
        DatabaseError::Storage(e) => storage_error_to_io_error(e),
        DatabaseError::DatabaseAlreadyOpen => io::Error::other("Database is already open"),
        DatabaseError::RepairAborted => io::Error::other("Database repair was aborted"),
        DatabaseError::UpgradeRequired(version) => {
            io::Error::other(format!("Database upgrade required to version {}", version))
        }
        e => io::Error::other(e),
    }
}

fn transaction_error_to_io_error(e: TransactionError) -> io::Error {
    match e {
        TransactionError::Storage(e) => storage_error_to_io_error(e),
        e => io::Error::other(e),
    }
}

fn table_error_to_io_error(e: TableError) -> io::Error {
    match e {
        TableError::Storage(e) => storage_error_to_io_error(e),
        TableError::TableAlreadyOpen(name, location) => {
            io::Error::other(format!("Table {} is already open: {}", name, location))
        }
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
        e => io::Error::other(e),
    }
}

fn commit_error_to_io_error(e: CommitError) -> io::Error {
    match e {
        CommitError::Storage(e) => storage_error_to_io_error(e),
        e => io::Error::other(e),
    }
}
