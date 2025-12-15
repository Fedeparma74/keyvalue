use crate::{MaybeSendSync, io};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

pub trait TransactionalKVDB: MaybeSendSync + 'static {
    type ReadTransaction: KVReadTransaction;
    type WriteTransaction: KVWriteTransaction;

    fn begin_read(&self) -> Result<Self::ReadTransaction, io::Error>;
    fn begin_write(&self) -> Result<Self::WriteTransaction, io::Error>;
}

pub trait KVReadTransaction: MaybeSendSync + 'static {
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
