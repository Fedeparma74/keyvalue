use crate::{
    Direction, KeyRange, KeyValueDB, MaybeSendSync, apply_range_in_memory, decode, encode, io,
};
#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use super::VersionedObject;

/// Synchronous key-value store with **per-entry versioning**.
///
/// Every value is wrapped in a [`VersionedObject`] that pairs the payload
/// with a monotonically increasing `u64` version.  Two deletion modes are
/// supported:
///
/// * **prune** (`prune = true`) — the entry is physically removed.
/// * **soft-delete** (`prune = false`) — the entry remains but its value
///   is set to `None` and the version is incremented, creating a
///   *tombstone* that downstream consumers can observe.
///
/// A blanket implementation is provided for every `T: KeyValueDB + ?Sized`
/// (including `dyn KeyValueDB`), so any backend automatically gains
/// versioned semantics.
pub trait VersionedKeyValueDB: MaybeSendSync + 'static {
    /// Inserts or updates the value of the key in the table with the specified version.
    /// If value is `None`, the entry is marked as deleted by setting its value to `None` and the specified version.
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    /// Permanently removes the entry from the table.
    fn remove(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    #[allow(clippy::type_complexity)]
    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error>;
    fn table_names(&self) -> Result<Vec<String>, io::Error>;

    /// Updates the value of the key in the table and increases the version by 1.
    /// If the key does not exist, it will be inserted with version 1.
    ///
    /// # Note
    /// This default implementation is **not atomic**: it performs a read followed by a write.
    /// Under concurrent access, callers must provide external synchronization or use the
    /// transactional API to avoid lost updates (TOCTOU).
    fn update(
        &self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let current_object = VersionedKeyValueDB::get(self, table_name, key)?;
        let new_version = match current_object {
            Some(ref obj) => obj.version.checked_add(1).ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "Version overflow",
            ))?,
            None => 1,
        };
        VersionedKeyValueDB::insert(self, table_name, key, value, new_version)
    }

    #[allow(clippy::type_complexity)]
    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in VersionedKeyValueDB::iter(self, table_name)? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }

    /// Versioned counterpart of [`crate::KeyValueDB::iter_range`].
    ///
    /// Default implementation filters the full `iter()` output; the blanket
    /// impl over `T: KeyValueDB` overrides this to delegate to the underlying
    /// backend's `iter_range` so that only at most `limit` entries are decoded.
    #[allow(clippy::type_complexity)]
    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let items = VersionedKeyValueDB::iter(self, table_name)?;
        Ok(apply_range_in_memory(items, &range))
    }

    /// Cursor-based pagination.
    #[allow(clippy::type_complexity)]
    fn iter_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut range = KeyRange::all().with_direction(direction).with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        VersionedKeyValueDB::iter_range(self, table_name, range)
    }

    /// Cursor-based pagination restricted to a prefix.
    #[allow(clippy::type_complexity)]
    fn iter_from_prefix_paginated(
        &self,
        table_name: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut range = KeyRange::prefix(prefix)
            .with_direction(direction)
            .with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        VersionedKeyValueDB::iter_range(self, table_name, range)
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(VersionedKeyValueDB::table_names(self)?.contains(&table_name.to_string()))
    }
    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(VersionedKeyValueDB::get(self, table_name, key)?.is_some())
    }
    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(VersionedKeyValueDB::iter(self, table_name)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }
    fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        Ok(VersionedKeyValueDB::iter(self, table_name)?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }
    /// Deletes all the entries in the specified table. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    fn delete_table(&self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        if prune {
            return self.prune_table(table_name);
        }
        for key in VersionedKeyValueDB::keys(self, table_name)? {
            VersionedKeyValueDB::update(self, table_name, &key, None)?;
        }
        Ok(())
    }
    /// Clears all the tables in the database. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    fn clear(&self, prune: bool) -> Result<(), io::Error> {
        if prune {
            return self.prune_all();
        }
        for table_name in VersionedKeyValueDB::table_names(self)? {
            VersionedKeyValueDB::delete_table(self, &table_name, false)?;
        }
        Ok(())
    }

    /// Physically removes every entry from `table_name`.
    ///
    /// Bridge to the underlying backend's bulk-delete primitive.  The default
    /// implementation removes entries one-by-one via [`Self::remove`]; the
    /// blanket impl over `T: KeyValueDB` overrides this with a single
    /// `delete_table` call.
    #[doc(hidden)]
    fn prune_table(&self, table_name: &str) -> Result<(), io::Error> {
        for key in VersionedKeyValueDB::keys(self, table_name)? {
            VersionedKeyValueDB::remove(self, table_name, &key)?;
        }
        Ok(())
    }

    /// Physically removes every table from the database.
    #[doc(hidden)]
    fn prune_all(&self) -> Result<(), io::Error> {
        for table_name in VersionedKeyValueDB::table_names(self)? {
            VersionedKeyValueDB::prune_table(self, &table_name)?;
        }
        Ok(())
    }
}

impl<T> VersionedKeyValueDB for T
where
    T: KeyValueDB + ?Sized,
{
    fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let obj = VersionedObject {
            value: value.map(|v| v.to_vec()),
            version,
        };

        KeyValueDB::insert(self, table_name, key, &encode(&obj)?)?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        KeyValueDB::get(self, table_name, key)?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn remove(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        KeyValueDB::remove(self, table_name, key)?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        KeyValueDB::iter(self, table_name)?
            .into_iter()
            .map(|(k, v)| Ok((k, decode(&v)?)))
            .collect()
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        KeyValueDB::table_names(self)
    }

    fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        KeyValueDB::iter_from_prefix(self, table_name, prefix)?
            .into_iter()
            .map(|(k, v)| Ok((k, decode(&v)?)))
            .collect()
    }

    fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        KeyValueDB::iter_range(self, table_name, range)?
            .into_iter()
            .map(|(k, v)| Ok((k, decode(&v)?)))
            .collect()
    }

    fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_table(self, table_name)
    }

    fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        KeyValueDB::contains_key(self, table_name, key)
    }

    fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        KeyValueDB::keys(self, table_name)
    }

    fn prune_table(&self, table_name: &str) -> Result<(), io::Error> {
        KeyValueDB::delete_table(self, table_name)
    }

    fn prune_all(&self) -> Result<(), io::Error> {
        KeyValueDB::clear(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn VersionedKeyValueDB>> = None;
    }
}
