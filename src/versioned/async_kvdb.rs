use crate::{
    AsyncKeyValueDB, Direction, KeyRange, MaybeSendSync, apply_range_in_memory, decode, encode, io,
};
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, string::String, vec::Vec};

use async_trait::async_trait;

use super::VersionedObject;

/// Async counterpart of [`VersionedKeyValueDB`](crate::VersionedKeyValueDB).
///
/// Provides identical per-entry versioning semantics — insert with an
/// explicit version, auto-increment via [`update`](Self::update), and
/// soft-delete vs. prune on [`delete_table`](Self::delete_table) /
/// [`clear`](Self::clear).
///
/// A blanket implementation is provided for every `T: AsyncKeyValueDB + ?Sized`
/// (including `dyn AsyncKeyValueDB`).
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncVersionedKeyValueDB: MaybeSendSync + 'static {
    /// Inserts or updates the value of the key in the table with the specified version.
    /// If value is `None`, the entry is marked as deleted by setting its value to `None` and the specified version.
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
        version: u64,
    ) -> Result<Option<VersionedObject>, io::Error>;
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error>;
    /// Permanently removes the entry from the table.
    async fn remove(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error>;
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error>;
    async fn table_names(&self) -> Result<Vec<String>, io::Error>;

    /// Updates the value of the key in the table and increases the version by 1.
    /// If the key does not exist, it will be inserted with version 1.
    ///
    /// # Note
    /// This default implementation is **not atomic**: it performs a read followed by a write.
    /// Under concurrent access, callers must provide external synchronization or use the
    /// transactional API to avoid lost updates (TOCTOU).
    async fn update(
        &self,
        table_name: &str,
        key: &str,
        value: Option<&[u8]>,
    ) -> Result<Option<VersionedObject>, io::Error> {
        let current_object = AsyncVersionedKeyValueDB::get(self, table_name, key).await?;
        let new_version = match current_object {
            Some(ref obj) => obj.version.checked_add(1).ok_or(io::Error::new(
                io::ErrorKind::InvalidData,
                "Version overflow",
            ))?,
            None => 1,
        };
        AsyncVersionedKeyValueDB::insert(self, table_name, key, value, new_version).await
    }

    #[allow(clippy::type_complexity)]
    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let mut result = Vec::new();
        for (key, value) in AsyncVersionedKeyValueDB::iter(self, table_name).await? {
            if key.starts_with(prefix) {
                result.push((key, value));
            }
        }
        Ok(result)
    }

    /// Async versioned counterpart of [`crate::KeyValueDB::iter_range`].
    ///
    /// Default implementation filters full `iter()` output; the blanket
    /// impl over `T: AsyncKeyValueDB` overrides this to delegate to the
    /// backend's native `iter_range` and decode only the returned entries.
    #[allow(clippy::type_complexity)]
    async fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        let items = AsyncVersionedKeyValueDB::iter(self, table_name).await?;
        Ok(apply_range_in_memory(items, &range))
    }

    /// Cursor-based pagination.
    #[allow(clippy::type_complexity)]
    async fn iter_paginated(
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
        AsyncVersionedKeyValueDB::iter_range(self, table_name, range).await
    }

    /// Cursor-based pagination restricted to a prefix.
    #[allow(clippy::type_complexity)]
    async fn iter_from_prefix_paginated(
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
        AsyncVersionedKeyValueDB::iter_range(self, table_name, range).await
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(AsyncVersionedKeyValueDB::table_names(self)
            .await?
            .contains(&table_name.to_string()))
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(AsyncVersionedKeyValueDB::get(self, table_name, key)
            .await?
            .is_some())
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(AsyncVersionedKeyValueDB::iter(self, table_name)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }
    async fn values(&self, table_name: &str) -> Result<Vec<VersionedObject>, io::Error> {
        Ok(AsyncVersionedKeyValueDB::iter(self, table_name)
            .await?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }
    /// Deletes all the entries in the specified table. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    async fn delete_table(&self, table_name: &str, prune: bool) -> Result<(), io::Error> {
        if prune {
            return self.prune_table(table_name).await;
        }
        for key in AsyncVersionedKeyValueDB::keys(self, table_name).await? {
            AsyncVersionedKeyValueDB::update(self, table_name, &key, None).await?;
        }
        Ok(())
    }
    /// Clears all the tables in the database. If `prune` is false, the entries are
    /// marked as deleted by setting their value to `None` and increasing their version.
    /// If `prune` is true, the entries are permanently removed.
    async fn clear(&self, prune: bool) -> Result<(), io::Error> {
        if prune {
            return self.prune_all().await;
        }
        for table_name in AsyncVersionedKeyValueDB::table_names(self).await? {
            AsyncVersionedKeyValueDB::delete_table(self, &table_name, false).await?;
        }
        Ok(())
    }

    /// Physically removes every entry from `table_name`.
    ///
    /// Bridge to the underlying backend's bulk-delete primitive.  The default
    /// implementation removes entries one-by-one; the blanket impl over
    /// `T: AsyncKeyValueDB` overrides this with a single `delete_table` call.
    #[doc(hidden)]
    async fn prune_table(&self, table_name: &str) -> Result<(), io::Error> {
        for key in AsyncVersionedKeyValueDB::keys(self, table_name).await? {
            AsyncVersionedKeyValueDB::remove(self, table_name, &key).await?;
        }
        Ok(())
    }

    /// Physically removes every table from the database.
    #[doc(hidden)]
    async fn prune_all(&self) -> Result<(), io::Error> {
        for table_name in AsyncVersionedKeyValueDB::table_names(self).await? {
            AsyncVersionedKeyValueDB::prune_table(self, &table_name).await?;
        }
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<T> AsyncVersionedKeyValueDB for T
where
    T: AsyncKeyValueDB + ?Sized,
{
    async fn insert(
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

        let old_value = AsyncKeyValueDB::insert(self, table_name, key, &encode(&obj)?).await?;
        old_value.map(|bytes| decode(&bytes)).transpose()
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<VersionedObject>, io::Error> {
        AsyncKeyValueDB::get(self, table_name, key)
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    async fn remove(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<VersionedObject>, io::Error> {
        AsyncKeyValueDB::remove(self, table_name, key)
            .await?
            .map(|bytes| decode(&bytes))
            .transpose()
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        AsyncKeyValueDB::iter(self, table_name)
            .await?
            .into_iter()
            .map(|(k, v)| Ok((k, decode(&v)?)))
            .collect()
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        AsyncKeyValueDB::table_names(self).await
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        AsyncKeyValueDB::iter_from_prefix(self, table_name, prefix)
            .await?
            .into_iter()
            .map(|(k, v)| Ok((k, decode(&v)?)))
            .collect()
    }

    async fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, VersionedObject)>, io::Error> {
        AsyncKeyValueDB::iter_range(self, table_name, range)
            .await?
            .into_iter()
            .map(|(k, v)| Ok((k, decode(&v)?)))
            .collect()
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        AsyncKeyValueDB::contains_table(self, table_name).await
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        AsyncKeyValueDB::contains_key(self, table_name, key).await
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        AsyncKeyValueDB::keys(self, table_name).await
    }

    async fn prune_table(&self, table_name: &str) -> Result<(), io::Error> {
        AsyncKeyValueDB::delete_table(self, table_name).await
    }

    async fn prune_all(&self) -> Result<(), io::Error> {
        AsyncKeyValueDB::clear(self).await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_dyn() {
        let _: Option<Box<dyn AsyncVersionedKeyValueDB>> = None;
    }
}
