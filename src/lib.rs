#![cfg_attr(all(not(feature = "std"), not(test)), no_std)]

extern crate alloc;

#[cfg(not(feature = "std"))]
use core2::io;
#[cfg(feature = "std")]
use std::io;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use transactional::*;

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

#[cfg(feature = "aws-s3")]
pub mod aws_s3;

#[cfg(all(feature = "local-storage", target_arch = "wasm32"))]
pub mod local_storage;

#[cfg(all(feature = "indexed-db", target_arch = "wasm32"))]
pub mod indexed_db;
