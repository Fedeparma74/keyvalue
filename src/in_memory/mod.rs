use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

use futures::channel::mpsc::UnboundedSender;

use crate::{KeyValueDB, RunBackupEvent, TABLE_VERSIONS};

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
    fn add_backup_notifier_sender(&self, _sender: UnboundedSender<RunBackupEvent>) {}

    fn restore_backup(
        &self,
        table_name: &str,
        data: Vec<(String, Vec<u8>)>,
        new_version: u32,
    ) -> Result<(), io::Error> {
        let mut map = self.map.write().unwrap();

        // get map for table_name
        let mut table = HashMap::new();

        // insert data into table
        for (key, value) in data {
            table.insert(key, value);
        }

        // get versions table
        let versions_table = map
            .entry(TABLE_VERSIONS.to_string())
            .or_insert(HashMap::new());

        // insert new version into versions table
        let version_entry = versions_table
            .entry(table_name.to_string())
            .or_insert_with(|| 0_u32.to_be_bytes().to_vec());

        *version_entry = new_version.to_be_bytes().to_vec();

        // insert table into map
        map.insert(table_name.to_string(), table);

        Ok(())
        



        // update versions table

    }

    fn get_table_version(&self, table_name: &str) -> Result<Option<u32>, io::Error> {
        Ok(self
            .map
            .read()
            .unwrap()
            .get(TABLE_VERSIONS)
            .and_then(|map| map.get(table_name))
            .map(|version| u32::from_be_bytes(version.clone().try_into().unwrap())))
    }

    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let mut map = self.map.write().unwrap();

        // Update the version for the table
        let versions_table = map
            .entry(TABLE_VERSIONS.to_string())
            .or_insert(HashMap::new());

        let version_entry = versions_table
            .entry(table_name.to_string())
            .or_insert_with(|| 0_u32.to_be_bytes().to_vec());

        let mut version_int = u32::from_be_bytes(
            version_entry
                .as_slice()
                .try_into()
                .expect("Invalid version bytes"),
        );
        version_int += 1;
        *version_entry = version_int.to_be_bytes().to_vec();

        // Insert into the actual table using the existing `map`
        let table = map.entry(table_name.to_owned()).or_default();
        Ok(table.insert(key.to_owned(), value.to_owned()))
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

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        self.map.write().unwrap().remove(table_name);
        Ok(())
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

    fn clear(&self) -> Result<(), io::Error> {
        self.map.write().unwrap().clear();
        Ok(())
    }
}
