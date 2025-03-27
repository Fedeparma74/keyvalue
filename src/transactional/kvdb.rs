use crate::{KeyValueDB, io};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

pub trait TransactionalKVDB: Send + Sync + 'static {
    type ReadTransaction: KVReadTransaction;
    type WriteTransaction: KVWriteTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error>;
    fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error>;
}

pub trait KVReadTransaction: Send + Sync + 'static {
    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    #[allow(clippy::type_complexity)]
    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name)? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }
    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names()?.contains(&table_name.to_string()))
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key)?.is_some())
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let mut keys = Vec::new();
        for (key, _) in self.iter(table_name)? {
            keys.push(key);
        }
        Ok(keys)
    }
    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name)? {
            values.push(value);
        }
        Ok(values)
    }
}

pub trait KVWriteTransaction: KVReadTransaction {
    fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;
    fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    fn commit(self) -> Result<(), io::Error>;
    fn abort(self) -> Result<(), io::Error>;

    fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name)? {
            self.remove(table_name, &key)?;
        }
        Ok(())
    }
    fn clear(&mut self) -> Result<(), io::Error> {
        for table_name in self.table_names()? {
            self.delete_table(&table_name)?;
        }
        Ok(())
    }
}

impl<T: TransactionalKVDB> KeyValueDB for T {
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let mut write_transaction = self.begin_write()?;
        match write_transaction.insert(table_name, key, value) {
            Ok(old_value) => {
                write_transaction.commit()?;
                Ok(old_value)
            }
            Err(e) => {
                write_transaction.abort()?;
                Err(e)
            }
        }
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.get(table_name, key)
    }

    fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let mut write_transaction = self.begin_write()?;
        match write_transaction.remove(table_name, key) {
            // Ok(Some(old_value)) => {
            //     write_transaction.commit()?;
            //     Ok(Some(old_value))
            // }
            // Ok(None) => {
            //     write_transaction.abort()?;
            //     Ok(None)
            // }
            Ok(old_value) => {
                write_transaction.commit()?;
                Ok(old_value)
            }
            Err(e) => {
                write_transaction.abort()?;
                Err(e)
            }
        }
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.iter(table_name)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.table_names()
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.iter_from_prefix(table_name, prefix)
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.contains_table(table_name)
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.contains_key(table_name, key)
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.keys(table_name)
    }

    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let read_transaction = self.begin_read()?;
        read_transaction.values(table_name)
    }

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        let mut write_transaction = self.begin_write()?;
        match write_transaction.delete_table(table_name) {
            Ok(()) => {
                write_transaction.commit()?;
                Ok(())
            }
            Err(e) => {
                write_transaction.abort()?;
                Err(e)
            }
        }
    }

    fn clear(&self) -> Result<(), io::Error> {
        let mut write_transaction = self.begin_write()?;
        match write_transaction.clear() {
            Ok(()) => {
                write_transaction.commit()?;
                Ok(())
            }
            Err(e) => {
                write_transaction.abort()?;
                Err(e)
            }
        }
    }
}
