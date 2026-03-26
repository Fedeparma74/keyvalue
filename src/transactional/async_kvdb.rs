use crate::{MaybeSendSync, io};
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
    async fn contains_table(&self, table_name: &str) -> Result<bool, io::Error> {
        Ok(self.table_names().await?.contains(&table_name.to_string()))
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
