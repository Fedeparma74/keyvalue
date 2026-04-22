use crate::{Direction, KeyRange, MaybeSendSync, apply_range_in_memory, io};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use async_trait::async_trait;

use super::WriteOp;
#[cfg(feature = "tokio")]
use super::{KVReadTransaction, KVWriteTransaction};

/// Async counterpart of [`TransactionalKVDB`](crate::TransactionalKVDB).
///
/// Provides the same begin / commit / abort transactional workflow but
/// with `async` methods.  On `tokio`-enabled native targets,
/// [`SpawnBlockingReadTx`] and [`SpawnBlockingWriteTx`] bridge sync
/// transaction types into this async interface.
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncTransactionalKVDB: MaybeSendSync + 'static {
    type ReadTransaction<'a>: AsyncKVReadTransaction<'a>;
    type WriteTransaction<'a>: AsyncKVWriteTransaction<'a>;

    async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error>;
    async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error>;

    /// Attempts to recover the database from an unrecoverable internal state.
    ///
    /// The default implementation is a no-op. Backends that support runtime
    /// recovery should override this.
    async fn try_recover(&self) -> Result<(), io::Error> {
        Ok(())
    }
}

/// Read-only half of an async transaction.  See [`AsyncTransactionalKVDB`].
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVReadTransaction<'a>: MaybeSendSync {
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error>;
    async fn table_names(&self) -> Result<Vec<String>, io::Error>;

    #[allow(clippy::type_complexity)]
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

    /// Async transactional counterpart of
    /// [`crate::KeyValueDB::iter_range`].
    ///
    /// See the non-async trait for semantics.  The default implementation
    /// is an in-memory fallback; every shipped backend overrides this
    /// with a native range-scan honouring the snapshot and pending writes.
    #[allow(clippy::type_complexity)]
    async fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let items = match &range.prefix {
            Some(p) => self.iter_from_prefix(table_name, p.as_str()).await?,
            None => self.iter(table_name).await?,
        };
        Ok(apply_range_in_memory(items, &range))
    }

    /// Cursor-based pagination over the full table.
    #[allow(clippy::type_complexity)]
    async fn iter_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut range = KeyRange::all().with_direction(direction).with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        self.iter_range(table_name, range).await
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
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut range = KeyRange::prefix(prefix)
            .with_direction(direction)
            .with_limit(limit);
        if let Some(k) = start_after {
            range = range.start_after(k);
        }
        self.iter_range(table_name, range).await
    }

    /// Cursor-based pagination returning only keys.
    async fn keys_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<String>, io::Error> {
        Ok(self
            .iter_paginated(table_name, start_after, limit, direction)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    /// Cursor-based pagination returning only values.
    async fn values_paginated(
        &self,
        table_name: &str,
        start_after: Option<&str>,
        limit: usize,
        direction: Direction,
    ) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .iter_paginated(table_name, start_after, limit, direction)
            .await?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names().await?.contains(&table_name.to_string()))
    }
    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        Ok(self.get(table_name, key).await?.is_some())
    }
    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        Ok(self
            .iter(table_name)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }
    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        Ok(self
            .iter(table_name)
            .await?
            .into_iter()
            .map(|(_, v)| v)
            .collect())
    }
}

/// Read-write half of an async transaction.  Extends [`AsyncKVReadTransaction`]
/// with mutating operations, plus [`commit`](Self::commit) and
/// [`abort`](Self::abort) for finalisation.
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
pub trait AsyncKVWriteTransaction<'a>: AsyncKVReadTransaction<'a> {
    async fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error>;
    async fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error>;
    async fn commit(self) -> Result<(), io::Error>;
    async fn abort(self) -> Result<(), io::Error>;

    async fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        for key in self.keys(table_name).await? {
            self.remove(table_name, &key).await?;
        }
        Ok(())
    }
    async fn clear(&mut self) -> Result<(), io::Error> {
        for table in self.table_names().await? {
            self.delete_table(&table).await?;
        }
        Ok(())
    }

    /// Applies a batch of [`WriteOp`]s and commits in a single step.
    ///
    /// The default implementation applies each operation individually and then
    /// calls [`commit`](Self::commit).
    async fn batch_commit(mut self, ops: Vec<WriteOp>) -> Result<(), io::Error>
    where
        Self: Sized,
    {
        for op in ops {
            match op {
                WriteOp::Insert {
                    table_name,
                    key,
                    value,
                } => {
                    self.insert(&table_name, &key, &value).await?;
                }
                WriteOp::Remove { table_name, key } => {
                    self.remove(&table_name, &key).await?;
                }
                WriteOp::DeleteTable { table_name } => {
                    self.delete_table(&table_name).await?;
                }
                WriteOp::Clear => {
                    self.clear().await?;
                }
            }
        }
        self.commit().await
    }
}

// ---------------------------------------------------------------------------
// Generic spawn_blocking adapters for wrapping sync transaction types
// ---------------------------------------------------------------------------

/// Wraps a sync `KVReadTransaction` in an async-safe `Arc<Mutex<Option<T>>>` so it
/// can be used with `tokio::task::spawn_blocking`.
#[cfg(feature = "tokio")]
pub struct SpawnBlockingReadTx<T: Send + 'static> {
    pub(crate) inner: std::sync::Arc<std::sync::Mutex<Option<T>>>,
}

/// Wraps a sync `KVWriteTransaction` in an async-safe `Arc<Mutex<Option<T>>>` so it
/// can be used with `tokio::task::spawn_blocking`.
#[cfg(feature = "tokio")]
pub struct SpawnBlockingWriteTx<T: Send + 'static> {
    pub(crate) inner: std::sync::Arc<std::sync::Mutex<Option<T>>>,
}

#[cfg(feature = "tokio")]
fn mutex_poisoned() -> io::Error {
    io::Error::other("Mutex poisoned")
}

#[cfg(feature = "tokio")]
fn tx_consumed() -> io::Error {
    io::Error::other("Transaction already consumed")
}

// -- AsyncKVReadTransaction for SpawnBlockingReadTx<T> --

#[cfg(feature = "tokio")]
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a, T> AsyncKVReadTransaction<'a> for SpawnBlockingReadTx<T>
where
    T: KVReadTransaction<'a> + Send + 'static,
{
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::get(tx, &table_name, &key)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::iter(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::table_names(tx)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::iter_from_prefix(tx, &table_name, &prefix)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::iter_range(tx, &table_name, range)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::contains_table(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::contains_key(tx, &table_name, &key)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::keys(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::values(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }
}

// -- AsyncKVReadTransaction for SpawnBlockingWriteTx<T> (read part of write tx) --

#[cfg(feature = "tokio")]
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a, T> AsyncKVReadTransaction<'a> for SpawnBlockingWriteTx<T>
where
    T: KVWriteTransaction<'a> + Send + 'static,
{
    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::get(tx, &table_name, &key)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::iter(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::table_names(tx)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn iter_from_prefix(
        &self,
        table_name: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let prefix = prefix.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::iter_from_prefix(tx, &table_name, &prefix)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn iter_range(
        &self,
        table_name: &str,
        range: KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::iter_range(tx, &table_name, range)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::contains_table(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn contains_key(&self, table_name: &str, key: &str) -> Result<bool, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::contains_key(tx, &table_name, &key)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn keys(&self, table_name: &str) -> Result<Vec<String>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::keys(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_ref().ok_or_else(tx_consumed)?;
            KVReadTransaction::values(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }
}

// -- AsyncKVWriteTransaction for SpawnBlockingWriteTx<T> --

#[cfg(feature = "tokio")]
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a, T> AsyncKVWriteTransaction<'a> for SpawnBlockingWriteTx<T>
where
    T: KVWriteTransaction<'a> + Send + 'static,
{
    async fn insert(
        &mut self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let key = key.to_string();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_mut().ok_or_else(tx_consumed)?;
            KVWriteTransaction::insert(tx, &table_name, &key, &value)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn remove(&mut self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_mut().ok_or_else(tx_consumed)?;
            KVWriteTransaction::remove(tx, &table_name, &key)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn delete_table(&mut self, table_name: &str) -> Result<(), io::Error> {
        let inner = self.inner.clone();
        let table_name = table_name.to_string();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_mut().ok_or_else(tx_consumed)?;
            KVWriteTransaction::delete_table(tx, &table_name)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn clear(&mut self) -> Result<(), io::Error> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.as_mut().ok_or_else(tx_consumed)?;
            KVWriteTransaction::clear(tx)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn commit(self) -> Result<(), io::Error> {
        tokio::task::spawn_blocking(move || {
            let mut guard = self.inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.take().ok_or_else(tx_consumed)?;
            KVWriteTransaction::commit(tx)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn abort(self) -> Result<(), io::Error> {
        tokio::task::spawn_blocking(move || {
            let mut guard = self.inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.take().ok_or_else(tx_consumed)?;
            KVWriteTransaction::abort(tx)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn batch_commit(self, ops: Vec<WriteOp>) -> Result<(), io::Error> {
        tokio::task::spawn_blocking(move || {
            let mut guard = self.inner.lock().map_err(|_| mutex_poisoned())?;
            let tx = guard.take().ok_or_else(tx_consumed)?;
            KVWriteTransaction::batch_commit(tx, ops)
        })
        .await
        .map_err(io::Error::other)?
    }
}
