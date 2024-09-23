use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

#[cfg(feature = "async")]
use async_trait::async_trait;

#[cfg(feature = "async")]
use crate::AsyncKeyValueDB;
use crate::KeyValueDB;

#[derive(Debug, Default)]
pub struct InMemoryDB {
    map: RwLock<HashMap<String, HashMap<String, Vec<u8>>>>,
}

impl InMemoryDB {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }
}

impl KeyValueDB for InMemoryDB {
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .write()
            .unwrap()
            .entry(table_name.to_owned())
            .or_default()
            .insert(key.to_owned(), value.to_owned()))
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .read()
            .unwrap()
            .get(table_name)
            .and_then(|map| map.get(key))
            .cloned())
    }

    fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .write()
            .unwrap()
            .get_mut(table_name)
            .and_then(|map| map.remove(key)))
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        Ok(self
            .map
            .read()
            .unwrap()
            .get(table_name)
            .map(|map| {
                map.iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect()
            })
            .unwrap_or_default())
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        Ok(self.map.read().unwrap().keys().cloned().collect())
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        Ok(self
            .map
            .read()
            .unwrap()
            .get(table_name)
            .map(|map| {
                map.iter()
                    .filter(|(key, _)| key.starts_with(prefix))
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect()
            })
            .unwrap_or_default())
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.map.read().unwrap().contains_key(table_name))
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self
            .map
            .read()
            .unwrap()
            .get(table_name)
            .map(|map| map.contains_key(key))
            .unwrap_or_default())
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self
            .map
            .read()
            .unwrap()
            .get(table_name)
            .map(|map| map.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .map
            .read()
            .unwrap()
            .get(table_name)
            .map(|map| map.values().cloned().collect())
            .unwrap_or_default())
    }

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.map.write().unwrap().remove(table_name);
        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        self.map.write().unwrap().clear();
        Ok(())
    }
}

#[cfg(feature = "async")]
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncKeyValueDB for InMemoryDB {
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

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_table(self, table_name)
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
