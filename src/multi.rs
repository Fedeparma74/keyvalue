use crate::io;

use futures::{task::LocalSpawn, Future};
use tracing::error;

use crate::{AsyncKeyValueDB, KeyValueDB};

pub struct MultiDB {
    main: Box<dyn KeyValueDB>,
    kvdbs: Vec<Box<dyn KeyValueDB>>,
    async_kvdbs: Vec<Box<dyn AsyncKeyValueDB>>,
    spawner: Box<dyn LocalSpawn>,
}

pub struct MultiDBBuilder {
    main: Box<dyn KeyValueDB>,
    kvdbs: Vec<Box<dyn KeyValueDB>>,
    async_kvdbs: Vec<Box<dyn AsyncKeyValueDB>>,
    spawner: Box<dyn LocalSpawn>,
}

impl MultiDBBuilder {
    pub fn new<F>(main: Box<dyn KeyValueDB>, spawner: impl Fn(F)) -> Self
    where
        F: Future<Output = ()> + 'static,
    {
        Self {
            main,
            kvdbs: Vec::new(),
            async_kvdbs: Vec::new(),
            spawner,
        }
    }

    pub fn add_kvdb(mut self, kvdb: Box<dyn KeyValueDB>) -> Self {
        self.kvdbs.push(kvdb);
        self
    }

    pub fn add_async_kvdb(mut self, kvdb: Box<dyn AsyncKeyValueDB>) -> Self {
        self.async_kvdbs.push(kvdb);
        self
    }

    pub fn build(self) -> MultiDB {
        MultiDB {
            main: self.main,
            kvdbs: self.kvdbs,
            async_kvdbs: self.async_kvdbs,
            spawner: self.spawner,
        }
    }
}

impl MultiDB {
    pub fn builder(main: Box<dyn KeyValueDB>, spawner: Box<dyn LocalSpawn>) -> MultiDBBuilder {
        MultiDBBuilder::new(main, spawner)
    }
}

impl KeyValueDB for MultiDB {
    fn insert(&self, table_name: &str, key: &str, value: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let old_value = self.main.insert(table_name, key, value)?;

        for kvdb in &self.kvdbs {
            self.spawner
                .spawn_local(if let Err(e) = kvdb.insert(table_name, key, value) {
                    error!("Failed to insert into kvdb: {}", e);
                })
                .expect("Failed to spawn task");
        }

        for async_kvdb in &self.async_kvdbs {
            self.spawner
                .spawn_local(async_kvdb.insert(table_name, key, value))
                .expect("Failed to spawn task");
        }
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        todo!()
    }

    fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        todo!()
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        todo!()
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        todo!()
    }

    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        todo!()
    }
}
