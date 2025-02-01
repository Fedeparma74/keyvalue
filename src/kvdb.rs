use futures::channel::mpsc::UnboundedSender;

use crate::io;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

#[derive(Debug, Clone)]
pub enum RunBackupEvent {
    Insert((String, u32)),
    Delete((String, u32)),
}

pub trait KeyValueDB: Send + Sync {
    fn restore_backup(&self, table_name: &str, data: Vec<(String,Vec<u8>)>, new_version: u32) -> Result<(), io::Error>;
    fn add_backup_notifier_sender(&self, sender: UnboundedSender<RunBackupEvent>);
    fn get_table_version(&self, table_name: &str) -> io::Result<Option<u32>>;
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;
    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    #[allow(clippy::type_complexity)]
    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        for (key, _) in self.iter(table_name)? {
            self.remove(table_name, &key)?;
        }
        Ok(())
    }
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
    fn clear(&self) -> Result<(), io::Error> {
        for table_name in self.table_names()? {
            self.delete_table(&table_name)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn KeyValueDB>> = None;
    }
}
