use crate::{io, RunBackupEvent};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

use async_trait::async_trait;
use futures::channel::mpsc::UnboundedSender;

use crate::kvdb::KeyValueDB;

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKeyValueDB: Send + Sync {
    async fn restore_backup(&self, table_name: &str, data: Vec<(String,Vec<u8>)>, new_version: u32) -> Result<(), io::Error>;
    async fn add_backup_notifier_sender(&self, sender: UnboundedSender<RunBackupEvent>);
    async fn get_table_version(&self, table_name: &str) -> io::Result<Option<u32>>;
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;
    async fn table_names(&self) -> Result<Vec<String>, io::Error>;

    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        for (key, _) in self.iter(table_name).await? {
            self.remove(table_name, &key).await?;
        }
        Ok(())
    }
    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in self.iter(table_name).await? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key).await?.is_some())
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let mut keys = Vec::new();
        for (key, _) in self.iter(table_name).await? {
            keys.push(key);
        }
        Ok(keys)
    }
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let mut values = Vec::new();
        for (_, value) in self.iter(table_name).await? {
            values.push(value);
        }
        Ok(values)
    }
    async fn clear(&self) -> Result<(), io::Error> {
        for table_name in self.table_names().await? {
            self.delete_table(&table_name).await?;
        }
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncKeyValueDB for dyn KeyValueDB {
    async fn restore_backup(&self, table_name: &str, data: Vec<(String,Vec<u8>)>, new_version: u32) -> Result<(), io::Error> {
        KeyValueDB::restore_backup(self, table_name, data, new_version)
    }
    async fn add_backup_notifier_sender(&self, sender: UnboundedSender<RunBackupEvent>) {
        KeyValueDB::add_backup_notifier_sender(self, sender)
    }
    async fn get_table_version(&self, table_name: &str) -> io::Result<Option<u32>> {
        KeyValueDB::get_table_version(self, table_name)
    }

    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::insert(self, table_name, key, value)
    }
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::get(self, table_name, key)
    }
    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::remove(self, table_name, key)
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KeyValueDB::iter(self, table_name)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KeyValueDB::table_names(self)
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KeyValueDB::iter_from_prefix(self, table_name, prefix)
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_key(self, table_name, key)
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KeyValueDB::keys(self, table_name)
    }
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        KeyValueDB::values(self, table_name)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        KeyValueDB::delete_table(self, table_name)
    }
    async fn clear(&self) -> Result<(), io::Error> {
        KeyValueDB::clear(self)
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T: KeyValueDB> AsyncKeyValueDB for T {
    async fn restore_backup(&self, table_name: &str, data: Vec<(String,Vec<u8>)>, new_version: u32) -> Result<(), io::Error> {
        KeyValueDB::restore_backup(self, table_name, data, new_version)
    }
    async fn add_backup_notifier_sender(&self, sender: UnboundedSender<RunBackupEvent>) {
        KeyValueDB::add_backup_notifier_sender(self, sender)
    }
    async fn get_table_version(&self, table_name: &str) -> io::Result<Option<u32>> {
        KeyValueDB::get_table_version(self, table_name)
    }
    
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::insert(self, table_name, key, value)
    }
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::get(self, table_name, key)
    }
    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        KeyValueDB::remove(self, table_name, key)
    }
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KeyValueDB::iter(self, table_name)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KeyValueDB::table_names(self)
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        KeyValueDB::iter_from_prefix(self, table_name, prefix)
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_key(self, table_name, key)
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KeyValueDB::keys(self, table_name)
    }
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        KeyValueDB::values(self, table_name)
    }
    async fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        KeyValueDB::delete_table(self, table_name)
    }
    async fn clear(&self) -> Result<(), io::Error> {
        KeyValueDB::clear(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn AsyncKeyValueDB>> = None;
    }
}
