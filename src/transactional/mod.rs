//! Transactional API for key-value databases.
//!
//! Provides [`TransactionalKVDB`](crate::TransactionalKVDB) /
//! [`AsyncTransactionalKVDB`](crate::AsyncTransactionalKVDB) traits that
//! allow grouping multiple reads and writes into atomic transactions with
//! explicit `commit()` / `abort()` control.

#[cfg(feature = "async")]
pub(crate) mod async_kvdb;
mod kvdb;

#[cfg(feature = "async")]
pub use async_kvdb::*;
pub use kvdb::*;
