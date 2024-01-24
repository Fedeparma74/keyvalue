#![cfg_attr(all(not(feature = "std"), not(test)), no_std)]

extern crate alloc;

#[cfg(all(not(feature = "std"), feature = "no-std"))]
use core2::io;
#[cfg(feature = "std")]
use std::io;

#[cfg(feature = "async")]
mod async_kvdb;
mod kvdb;

#[cfg(feature = "async")]
pub use async_kvdb::*;
pub use kvdb::*;

#[cfg(feature = "in-memory")]
pub mod in_memory;

#[cfg(feature = "redb")]
pub mod redb;

#[cfg(all(feature = "local-storage", target_arch = "wasm32"))]
pub mod local_storage;

#[cfg(all(feature = "indexed-db", target_arch = "wasm32"))]
pub mod indexed_db;

// pub mod multi;
