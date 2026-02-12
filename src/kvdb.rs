use crate::{MaybeSendSync, io};
#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec::Vec,
};

/// Synchronous key-value database trait.
///
/// This is the primary abstraction over all storage backends. Each backend
/// organises data into **tables** (also called namespaces / column families /
/// keyspaces depending on the engine). Within a table, entries are identified
/// by a UTF-8 string **key** and store an arbitrary byte-slice **value**.
///
/// All mutating operations return the **previous value** (if any), following
/// the semantics of `HashMap::insert` / `HashMap::remove`.
///
/// Default method implementations are provided for convenience helpers such as
/// [`contains_key`](KeyValueDB::contains_key),
/// [`keys`](KeyValueDB::keys), [`values`](KeyValueDB::values),
/// [`delete_table`](KeyValueDB::delete_table) and [`clear`](KeyValueDB::clear).
/// Backends are encouraged to override these when a more efficient native
/// operation is available.
pub trait KeyValueDB: MaybeSendSync + 'static {
    /// Inserts a key-value pair into `table_name`.
    ///
    /// Returns the **previous value** associated with `key`, or `None` if the
    /// key did not exist. The table is created implicitly if it does not exist.
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;

    /// Retrieves the value associated with `key` in `table_name`.
    ///
    /// Returns `Ok(None)` if the key or the table does not exist.
    fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;

    /// Removes the entry for `key` from `table_name`.
    ///
    /// Returns the **previous value**, or `None` if the key did not exist.
    fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;

    /// Returns all key-value pairs stored in `table_name`.
    ///
    /// The order of entries is backend-dependent (e.g. sorted for tree-based
    /// engines, arbitrary for hash-based ones). Returns an empty `Vec` when
    /// the table does not exist.
    #[allow(clippy::type_complexity)]
    fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;

    /// Returns the names of all tables currently present in the database.
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    /// Returns all entries whose key starts with `prefix` in `table_name`.
    ///
    /// The default implementation iterates all entries and filters client-side.
    /// Backends with native prefix-scan support (e.g. RocksDB, fjall) override
    /// this for better performance.
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
    /// Returns `true` if a table with the given name exists.
    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names()?.contains(&table_name.to_string()))
    }

    /// Returns `true` if `key` exists in `table_name`.
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key)?.is_some())
    }

    /// Returns all keys stored in `table_name`.
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self.iter(table_name)?.into_iter().map(|(k, _)| k).collect())
    }
    /// Returns all values stored in `table_name`.
    fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self.iter(table_name)?.into_iter().map(|(_, v)| v).collect())
    }
    /// Deletes the entire table and all its entries.
    ///
    /// The default implementation removes each key individually. Backends
    /// should override this with a more efficient bulk-delete when possible.
    fn delete_table(&self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name)? {
            self.remove(table_name, &key)?;
        }
        Ok(())
    }
    /// Removes **all** tables and entries from the database.
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
