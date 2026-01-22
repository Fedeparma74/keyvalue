use std::{io, path::Path, sync::Arc};

use rust_rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
    WriteBatch,
};

use crate::KeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

const DEFAULT_CF: &str = "default";

type Rocks = DBWithThreadMode<MultiThreaded>;

pub struct RocksDB {
    inner: Arc<Rocks>,
    path: String,
    opts: Arc<Options>,
}

impl RocksDB {
    pub fn open(path: &Path) -> io::Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let cfs = Rocks::list_cf(&opts, path).unwrap_or_default();

        let descriptors = cfs
            .iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf, opts.clone()))
            .collect::<Vec<_>>();

        let db = Rocks::open_cf_descriptors(&opts, path, descriptors).map_err(io::Error::other)?;

        Ok(Self {
            inner: Arc::new(db),
            path: path_str,
            opts: Arc::new(opts),
        })
    }
}

impl KeyValueDB for RocksDB {
    fn insert(&self, table: &str, key: &str, value: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let db = &*self.inner;

        let mut batch = WriteBatch::default();

        // Create cf if not exists
        let cf = if let Some(cf) = db.cf_handle(table) {
            cf
        } else {
            db.create_cf(table, &self.opts).map_err(io::Error::other)?;
            db.cf_handle(table).ok_or(io::Error::other(
                "Failed to get column family after creation",
            ))?
        };

        let key_bytes = key.as_bytes();
        let old = db
            .get_cf(&cf, key_bytes)
            .map_err(io::Error::other)?
            .map(|v| v.to_vec());

        batch.put_cf(&cf, key_bytes, value);

        db.write(&batch).map_err(io::Error::other)?;

        Ok(old)
    }

    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let db = &*self.inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        db.get_cf(&cf, key.as_bytes())
            .map_err(io::Error::other)?
            .map(Ok)
            .transpose()
    }

    fn remove(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let db = &*self.inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        let key_bytes = key.as_bytes();
        let old = db
            .get_cf(&cf, key_bytes)
            .map_err(io::Error::other)?
            .map(|v| v.to_vec());

        db.delete_cf(&cf, key_bytes).map_err(io::Error::other)?;
        Ok(old)
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let db = &*self.inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let mut result = Vec::new();
        let iter = db.iterator_cf(&cf, IteratorMode::Start);
        for item in iter {
            let (k, v) = item.map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut names = Rocks::list_cf(&self.opts, &self.path)
            .map_err(io::Error::other)?
            .into_iter()
            .filter(|n| n != DEFAULT_CF)
            .collect::<Vec<String>>();

        names.sort();
        Ok(names)
    }

    fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let db = &*self.inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let prefix_b = prefix.as_bytes();
        let iter = db.iterator_cf(&cf, IteratorMode::From(prefix_b, Direction::Forward));

        let mut result = Vec::new();
        for item in iter {
            let (k, v) = item.map_err(io::Error::other)?;
            if !k.starts_with(prefix_b) {
                break;
            }
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn contains_table(&self, table: &str) -> Result<bool, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        Ok(self.inner.cf_handle(table).is_some())
    }

    fn delete_table(&self, table: &str) -> Result<(), io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete default column family",
            ));
        }

        let db = &*self.inner;

        if db.cf_handle(table).is_some() {
            db.drop_cf(table).map_err(io::Error::other)?;
        }

        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        let current_tables = Rocks::list_cf(&self.opts, &self.path)
            .map_err(io::Error::other)?
            .into_iter()
            .filter(|n| n != DEFAULT_CF)
            .collect::<Vec<String>>();

        let db = &*self.inner;

        for table_name in current_tables {
            if db.cf_handle(&table_name).is_some() {
                db.drop_cf(&table_name).map_err(io::Error::other)?;
            }
        }

        Ok(())
    }
}
