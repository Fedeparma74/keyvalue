use std::{io, path::Path};

use ::redb::{CommitError, Database, DatabaseError, StorageError, TableError, TransactionError};

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
