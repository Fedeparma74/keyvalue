use std::io;

use redb::{
    ReadTransaction, ReadableTable, StorageError, TableDefinition, TableError, TableHandle,
    WriteTransaction,
};

use crate::{KVReadTransaction, KVWriteTransaction, TransactionalKVDB};

use super::{
    commit_error_to_io_error, storage_error_to_io_error, table_error_to_io_error,
    transaction_error_to_io_error, RedbDB,
};

impl KVReadTransaction for ReadTransaction {
    fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let table_res = self.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
        let table = match table_res {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => {
                return Ok(None);
            }
            Err(e) => return Err(table_error_to_io_error(e)),
        };
        let value = table
            .get(key)
            .map_err(storage_error_to_io_error)?
            .map(|v| v.value().to_vec());

        Ok(value)
    }

    fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        let table_res = self.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
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

    fn table_names(&self) -> io::Result<Vec<String>> {
        let mut result = Vec::new();
        let tables_res = self.list_tables();
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
}

impl KVReadTransaction for WriteTransaction {
    fn get(&self, table_name: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        // check if the table exists
        if !self.contains_table(table_name)? {
            return Ok(None);
        }

        let table_res = self.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
        let table = match table_res {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => {
                return Ok(None);
            }
            Err(e) => return Err(table_error_to_io_error(e)),
        };
        let value = table
            .get(key)
            .map_err(storage_error_to_io_error)?
            .map(|v| v.value().to_vec());

        Ok(value)
    }

    fn iter(&self, table_name: &str) -> io::Result<Vec<(String, Vec<u8>)>> {
        // check if the table exists
        if !self.contains_table(table_name)? {
            return Ok(Vec::new());
        }

        let table_res = self.open_table(TableDefinition::<&str, &[u8]>::new(table_name));
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

    fn table_names(&self) -> io::Result<Vec<String>> {
        let mut result = Vec::new();
        let tables_res = self.list_tables();
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
}

impl KVWriteTransaction for WriteTransaction {
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let mut table = self
            .open_table(TableDefinition::<&str, &[u8]>::new(table_name))
            .map_err(table_error_to_io_error)?;
        let old = table
            .insert(key, value)
            .map_err(storage_error_to_io_error)?
            .map(|v| v.value().to_vec());
        Ok(old)
    }

    fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if !self.contains_table(table_name)? {
            return Ok(None);
        }

        let old = match self.open_table(TableDefinition::<&str, &[u8]>::new(table_name)) {
            Ok(mut table) => table
                .remove(key)
                .map_err(storage_error_to_io_error)?
                .map(|v| v.value().to_vec()),
            Err(TableError::TableDoesNotExist(_)) => None,
            Err(e) => return Err(table_error_to_io_error(e)),
        };

        Ok(old)
    }

    fn commit(self) -> Result<(), io::Error> {
        redb::WriteTransaction::commit(self).map_err(commit_error_to_io_error)
    }

    fn abort(self) -> Result<(), io::Error> {
        redb::WriteTransaction::abort(self).map_err(storage_error_to_io_error)
    }

    fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        redb::WriteTransaction::delete_table(self, TableDefinition::<&str, &[u8]>::new(table_name))
            .map_err(table_error_to_io_error)?;

        Ok(())
    }
}

impl TransactionalKVDB for RedbDB {
    type ReadTransaction = ReadTransaction;
    type WriteTransaction = WriteTransaction;

    fn begin_read(&self) -> io::Result<ReadTransaction> {
        self.inner
            .begin_read()
            .map_err(transaction_error_to_io_error)
    }

    fn begin_write(&self) -> io::Result<WriteTransaction> {
        self.inner
            .begin_write()
            .map_err(transaction_error_to_io_error)
    }
}
