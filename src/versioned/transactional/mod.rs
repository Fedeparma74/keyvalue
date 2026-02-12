//! Versioned transactional key-value traits.
//!
//! Combines the versioning semantics of [`VersionedKeyValueDB`](crate::VersionedKeyValueDB)
//! with the transactional guarantees of [`TransactionalKVDB`](crate::TransactionalKVDB).
//! Blanket implementations are provided so that any `TransactionalKVDB`
//! automatically implements `VersionedTransactionalKVDB`, and any
//! `KVReadTransaction` / `KVWriteTransaction` automatically implements its
//! versioned counterpart.

#[cfg(feature = "async")]
mod async_kvdb;
mod kvdb;

#[cfg(feature = "async")]
pub use async_kvdb::*;
pub use kvdb::*;
