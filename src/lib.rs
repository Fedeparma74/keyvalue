//! # keyvalue
//!
//! A unified, backend-agnostic key-value database abstraction library for Rust.
//!
//! This crate provides a common set of traits ([`KeyValueDB`], [`AsyncKeyValueDB`]) that abstract
//! over multiple storage backends, allowing applications to swap implementations without changing
//! business logic. It supports both synchronous and asynchronous APIs, optional transactional
//! semantics via the [`TransactionalKVDB`] / [`AsyncTransactionalKVDB`] traits, and
//! version-tracked entries via the [`VersionedKeyValueDB`] / [`AsyncVersionedKeyValueDB`] traits.
//!
//! ## Supported backends
//!
//! | Backend | Feature flag | Sync | Async | Transactional |
//! |---------|-------------|------|-------|---------------|
//! | In-memory (HashMap) | `in-memory` | ✓ | ✓ | ✓ |
//! | [redb](https://docs.rs/redb) | `redb` | ✓ | ✓ (tokio) | ✓ |
//! | [fjall](https://docs.rs/fjall) | `fjall` | ✓ | ✓ (tokio) | ✓ |
//! | [RocksDB](https://docs.rs/rust-rocksdb) | `rocksdb` | ✓ | ✓ (tokio) | ✓ |
//! | [SQLite (Turso)](https://docs.rs/turso) | `sqlite` | — | ✓ | ✓ |
//! | AWS S3 | `aws-s3` | — | ✓ | — |
//! | LocalStorage (WASM) | `local-storage` | ✓ | ✓ | — |
//! | IndexedDB (WASM) | `indexed-db` | — | ✓ | ✓ |
//!
//! ## Feature flags
//!
//! - **`std`** — Enables standard library support (enabled by default).
//! - **`async`** — Enables the [`AsyncKeyValueDB`] trait and async support (enabled by default).
//! - **`tokio`** — Enables `spawn_blocking`-based async wrappers for synchronous backends.
//! - **`transactional`** — Enables the transactional API traits.
//! - **`versioned`** — Enables the versioned API traits.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use keyvalue::{KeyValueDB, in_memory::InMemoryDB};
//!
//! let db = InMemoryDB::new();
//! db.insert("users", "alice", b"data").unwrap();
//! assert_eq!(db.get("users", "alice").unwrap(), Some(b"data".to_vec()));
//! ```//!
//! ## WASM support
//!
//! The following backends work on `wasm32-unknown-unknown`:
//!
//! | Backend | Notes |
//! |---------|-------|
//! | In-memory | Full sync + async support.  Uses a single `RwLock<HashMap>` internally. |
//! | LocalStorage | Sync + async. ~5 MiB browser limit. Main-thread only. |
//! | IndexedDB | Async-only. Transactional. Uses a command-channel pattern via `wasmt`. |
//! | AWS S3 | Async-only. Uses `reqwest` with `wasm-bindgen` transport. |
//!
//! On WASM the [`MaybeSendSync`] super-trait on all database traits relaxes
//! the `Send + Sync` bounds, so types that are `!Send` (e.g. `Rc`-based
//! browser APIs) can still implement the traits.
#![cfg_attr(all(not(feature = "std"), not(test)), no_std)]

extern crate alloc;

#[cfg(not(feature = "std"))]
use nostd::io;
#[cfg(feature = "std")]
use std::io;

/// Macro that generates an [`AsyncKeyValueDB`] implementation for any
/// `Clone + KeyValueDB + Send + 'static` type by delegating every method to
/// [`tokio::task::spawn_blocking()`].
///
/// The type must be cheaply cloneable (e.g. via internal `Arc`s) because each
/// async call clones the database handle and moves it into the blocking task.
///
/// This macro is used internally by the `redb`, `fjall`, and `rocksdb` backends.
#[cfg(feature = "tokio")]
#[allow(unused_macros)]
macro_rules! impl_async_kvdb_via_spawn_blocking {
    ($type:ty) => {
        #[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait::async_trait)]
        #[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait::async_trait(?Send))]
        impl $crate::AsyncKeyValueDB for $type {
            async fn insert(
                &self,
                table_name: &str,
                key: &str,
                value: &[u8],
            ) -> Result<Option<Vec<u8>>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                let key = key.to_string();
                let value = value.to_vec();
                tokio::task::spawn_blocking(move || {
                    $crate::KeyValueDB::insert(&db, &table_name, &key, &value)
                })
                .await
                .map_err(std::io::Error::other)?
            }

            async fn get(
                &self,
                table_name: &str,
                key: &str,
            ) -> Result<Option<Vec<u8>>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                let key = key.to_string();
                tokio::task::spawn_blocking(move || $crate::KeyValueDB::get(&db, &table_name, &key))
                    .await
                    .map_err(std::io::Error::other)?
            }

            async fn remove(
                &self,
                table_name: &str,
                key: &str,
            ) -> Result<Option<Vec<u8>>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                let key = key.to_string();
                tokio::task::spawn_blocking(move || {
                    $crate::KeyValueDB::remove(&db, &table_name, &key)
                })
                .await
                .map_err(std::io::Error::other)?
            }

            async fn iter(
                &self,
                table_name: &str,
            ) -> Result<Vec<(String, Vec<u8>)>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                tokio::task::spawn_blocking(move || $crate::KeyValueDB::iter(&db, &table_name))
                    .await
                    .map_err(std::io::Error::other)?
            }

            async fn table_names(&self) -> Result<Vec<String>, std::io::Error> {
                let db = self.clone();
                tokio::task::spawn_blocking(move || $crate::KeyValueDB::table_names(&db))
                    .await
                    .map_err(std::io::Error::other)?
            }

            async fn iter_from_prefix(
                &self,
                table_name: &str,
                prefix: &str,
            ) -> Result<Vec<(String, Vec<u8>)>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                let prefix = prefix.to_string();
                tokio::task::spawn_blocking(move || {
                    $crate::KeyValueDB::iter_from_prefix(&db, &table_name, &prefix)
                })
                .await
                .map_err(std::io::Error::other)?
            }

            async fn iter_range(
                &self,
                table_name: &str,
                range: $crate::KeyRange,
            ) -> Result<Vec<(String, Vec<u8>)>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                tokio::task::spawn_blocking(move || {
                    $crate::KeyValueDB::iter_range(&db, &table_name, range)
                })
                .await
                .map_err(std::io::Error::other)?
            }

            async fn contains_table(&self, table_name: &str) -> Result<bool, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                tokio::task::spawn_blocking(move || {
                    $crate::KeyValueDB::contains_table(&db, &table_name)
                })
                .await
                .map_err(std::io::Error::other)?
            }

            async fn contains_key(
                &self,
                table_name: &str,
                key: &str,
            ) -> Result<bool, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                let key = key.to_string();
                tokio::task::spawn_blocking(move || {
                    $crate::KeyValueDB::contains_key(&db, &table_name, &key)
                })
                .await
                .map_err(std::io::Error::other)?
            }

            async fn keys(&self, table_name: &str) -> Result<Vec<String>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                tokio::task::spawn_blocking(move || $crate::KeyValueDB::keys(&db, &table_name))
                    .await
                    .map_err(std::io::Error::other)?
            }

            async fn values(&self, table_name: &str) -> Result<Vec<Vec<u8>>, std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                tokio::task::spawn_blocking(move || $crate::KeyValueDB::values(&db, &table_name))
                    .await
                    .map_err(std::io::Error::other)?
            }

            async fn delete_table(&self, table_name: &str) -> Result<(), std::io::Error> {
                let db = self.clone();
                let table_name = table_name.to_string();
                tokio::task::spawn_blocking(move || {
                    $crate::KeyValueDB::delete_table(&db, &table_name)
                })
                .await
                .map_err(std::io::Error::other)?
            }

            async fn clear(&self) -> Result<(), std::io::Error> {
                let db = self.clone();
                tokio::task::spawn_blocking(move || $crate::KeyValueDB::clear(&db))
                    .await
                    .map_err(std::io::Error::other)?
            }
        }
    };
}

#[cfg(feature = "tokio")]
#[allow(unused_imports)]
use impl_async_kvdb_via_spawn_blocking;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use transactional::*;

#[cfg(feature = "versioned")]
mod versioned;

#[cfg(feature = "versioned")]
pub use versioned::*;

#[cfg(feature = "async")]
mod async_kvdb;
mod kvdb;
mod range;

#[cfg(feature = "async")]
pub use async_kvdb::*;
pub use kvdb::*;
pub use range::*;

#[cfg(feature = "in-memory")]
pub mod in_memory;

#[cfg(feature = "redb")]
pub mod redb;

#[cfg(feature = "fjall")]
pub mod fjall;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "aws-s3")]
pub mod aws_s3;

#[cfg(all(feature = "local-storage", target_arch = "wasm32"))]
pub mod local_storage;

#[cfg(all(feature = "indexed-db", target_arch = "wasm32"))]
pub mod indexed_db;

/// Conditional `Send + Sync` bound that is enforced on native targets with `std`
/// but relaxed on `wasm32` or `no_std` environments.
///
/// This marker trait is used as a super-trait on all database traits so that
/// implementations are thread-safe where the platform supports it, while still
/// allowing single-threaded WASM targets to work without `Send`/`Sync`.
#[cfg(all(not(target_arch = "wasm32"), feature = "std"))]
pub trait MaybeSendSync: Send + Sync {}
/// See the `cfg(not(target_arch = "wasm32"))` variant for documentation.
#[cfg(any(target_arch = "wasm32", not(feature = "std")))]
pub trait MaybeSendSync {}

#[cfg(all(not(target_arch = "wasm32"), feature = "std"))]
impl<T: Send + Sync> MaybeSendSync for T {}
#[cfg(any(target_arch = "wasm32", not(feature = "std")))]
impl<T> MaybeSendSync for T {}
