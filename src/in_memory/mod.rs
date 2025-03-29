use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

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
