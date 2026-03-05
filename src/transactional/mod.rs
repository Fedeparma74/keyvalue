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

/// A pre-encoded write operation for batch replay.
///
/// Used with [`AsyncKVWriteTransaction::batch_commit`] to batch multiple
/// operations into a single atomic unit, avoiding per-operation overhead (e.g.
/// individual `spawn_blocking` calls on tokio-backed backends).
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Insert a key-value pair into a table.
    Insert {
        table_name: String,
        key: String,
        value: Vec<u8>,
    },
    /// Remove a key from a table.
    Remove { table_name: String, key: String },
    /// Delete all entries in a table.
    DeleteTable { table_name: String },
    /// Clear all tables in the database.
    Clear,
}
