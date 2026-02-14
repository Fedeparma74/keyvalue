//! Integration tests for every backend, covering all trait combinations:
//!
//! - `KeyValueDB` (sync) + edge cases
//! - `AsyncKeyValueDB`
//! - `VersionedKeyValueDB` (sync) + soft-delete
//! - `AsyncVersionedKeyValueDB`
//! - `TransactionalKVDB` (sync) + RYOW semantics
//! - `AsyncTransactionalKVDB`
//! - `VersionedTransactionalKVDB` (sync)
//! - `AsyncVersionedTransactionalKVDB`
//! - Persistence (close + reopen)

#[cfg(all(feature = "test", not(target_arch = "wasm32")))]
mod common;

#[cfg(all(feature = "test", not(target_arch = "wasm32")))]
mod tests {
    use super::common;

    // =======================================================================
    // InMemoryDB
    // =======================================================================

    #[cfg(feature = "in-memory")]
    #[test]
    fn test_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(feature = "in-memory")]
    #[test]
    fn test_in_memory_edge_cases() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_db_edge_cases(&db);
    }

    #[cfg(all(feature = "async", feature = "in-memory"))]
    #[tokio::test]
    async fn test_async_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "async", feature = "in-memory"))]
    #[tokio::test]
    async fn test_async_in_memory_edge_cases() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_async_db_edge_cases(&db).await;
    }

    #[cfg(all(feature = "versioned", feature = "in-memory"))]
    #[test]
    fn test_versioned_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_versioned_db(&db);
    }

    #[cfg(all(feature = "versioned", feature = "in-memory"))]
    #[test]
    fn test_versioned_in_memory_soft_delete() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_versioned_soft_delete(&db);
    }

    #[cfg(all(feature = "async", feature = "versioned", feature = "in-memory"))]
    #[tokio::test]
    async fn test_async_versioned_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_async_versioned_db(&db).await;
    }

    #[cfg(all(feature = "transactional", feature = "in-memory"))]
    #[test]
    fn test_transactional_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_transactional_db(&db);
        common::persist_test_data(Box::new(db.clone()));
        common::check_test_data(&db);
        {
            let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
            assert!(
                !keyvalue::KVReadTransaction::table_names(&read)
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::TransactionalKVDB::begin_write(&db).unwrap();
        keyvalue::KVWriteTransaction::clear(&mut write).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&write)
                .unwrap()
                .is_empty()
        );
        keyvalue::KVWriteTransaction::commit(write).unwrap();
        let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&read)
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "transactional", feature = "in-memory"))]
    #[test]
    fn test_transactional_in_memory_ryow() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_transactional_ryow(&db);
    }

    #[cfg(all(feature = "async", feature = "transactional", feature = "in-memory"))]
    #[tokio::test]
    async fn test_async_transactional_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_async_transactional_db(&db).await;
        common::persist_test_data_async(Box::new(db.clone())).await;
        common::check_test_data_async(&db).await;
        {
            let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
                .await
                .unwrap();
            assert!(
                !keyvalue::AsyncKVReadTransaction::table_names(&read)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::AsyncTransactionalKVDB::begin_write(&db)
            .await
            .unwrap();
        keyvalue::AsyncKVWriteTransaction::clear(&mut write)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&write)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKVWriteTransaction::commit(write)
            .await
            .unwrap();
        let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&read)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(
        feature = "versioned",
        feature = "transactional",
        feature = "in-memory"
    ))]
    #[test]
    fn test_versioned_transactional_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_versioned_transactional_db(&db);
    }

    #[cfg(all(
        feature = "async",
        feature = "versioned",
        feature = "transactional",
        feature = "in-memory"
    ))]
    #[tokio::test]
    async fn test_async_versioned_transactional_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_async_versioned_transactional_db(&db).await;
    }

    // =======================================================================
    // RedbDB
    // =======================================================================

    #[cfg(feature = "redb")]
    #[test]
    fn test_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::check_test_data(&db);
        assert!(!keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
        keyvalue::KeyValueDB::clear(&db).unwrap();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(feature = "redb")]
    #[test]
    fn test_redb_edge_cases() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_redb_edge_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_db_edge_cases(&db);
    }

    #[cfg(all(feature = "async", feature = "redb"))]
    #[tokio::test]
    async fn test_async_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::check_test_data_async(&db).await;
        assert!(
            !keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKeyValueDB::clear(&db).await.unwrap();
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "versioned", feature = "redb"))]
    #[test]
    fn test_versioned_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_versioned_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_versioned_db(&db);
    }

    #[cfg(all(feature = "versioned", feature = "redb"))]
    #[test]
    fn test_versioned_redb_soft_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_versioned_redb_soft_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_versioned_soft_delete(&db);
    }

    #[cfg(all(feature = "async", feature = "versioned", feature = "redb"))]
    #[tokio::test]
    async fn test_async_versioned_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_versioned_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_async_versioned_db(&db).await;
    }

    #[cfg(all(feature = "transactional", feature = "redb"))]
    #[test]
    fn test_transactional_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_transactional_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_transactional_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::check_test_data(&db);
        {
            let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
            assert!(
                !keyvalue::KVReadTransaction::table_names(&read)
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::TransactionalKVDB::begin_write(&db).unwrap();
        keyvalue::KVWriteTransaction::clear(&mut write).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&write)
                .unwrap()
                .is_empty()
        );
        keyvalue::KVWriteTransaction::commit(write).unwrap();
        let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&read)
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "transactional", feature = "redb"))]
    #[test]
    fn test_transactional_redb_ryow() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_transactional_redb_ryow_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_transactional_ryow(&db);
    }

    #[cfg(all(feature = "async", feature = "transactional", feature = "redb"))]
    #[tokio::test]
    async fn test_async_transactional_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_transactional_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_async_transactional_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::check_test_data_async(&db).await;
        {
            let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
                .await
                .unwrap();
            assert!(
                !keyvalue::AsyncKVReadTransaction::table_names(&read)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::AsyncTransactionalKVDB::begin_write(&db)
            .await
            .unwrap();
        keyvalue::AsyncKVWriteTransaction::clear(&mut write)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&write)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKVWriteTransaction::commit(write)
            .await
            .unwrap();
        let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&read)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "versioned", feature = "transactional", feature = "redb"))]
    #[test]
    fn test_versioned_transactional_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_vtx_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_versioned_transactional_db(&db);
    }

    #[cfg(all(
        feature = "async",
        feature = "versioned",
        feature = "transactional",
        feature = "redb"
    ))]
    #[tokio::test]
    async fn test_async_versioned_transactional_redb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_vtx_redb_db");
        let db = keyvalue::redb::RedbDB::open(&path).unwrap();
        common::test_async_versioned_transactional_db(&db).await;
    }

    // =======================================================================
    // FjallDB
    // =======================================================================

    #[cfg(feature = "fjall")]
    #[test]
    fn test_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::check_test_data(&db);
        assert!(!keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
        keyvalue::KeyValueDB::clear(&db).unwrap();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn test_fjall_edge_cases() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_fjall_edge_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_db_edge_cases(&db);
    }

    #[cfg(all(feature = "async", feature = "fjall"))]
    #[tokio::test]
    async fn test_async_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::check_test_data_async(&db).await;
        assert!(
            !keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKeyValueDB::clear(&db).await.unwrap();
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "versioned", feature = "fjall"))]
    #[test]
    fn test_versioned_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_versioned_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_versioned_db(&db);
    }

    #[cfg(all(feature = "versioned", feature = "fjall"))]
    #[test]
    fn test_versioned_fjall_soft_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_versioned_fjall_soft_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_versioned_soft_delete(&db);
    }

    #[cfg(all(feature = "async", feature = "versioned", feature = "fjall"))]
    #[tokio::test]
    async fn test_async_versioned_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_versioned_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_async_versioned_db(&db).await;
    }

    #[cfg(all(feature = "transactional", feature = "fjall"))]
    #[test]
    fn test_transactional_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_transactional_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_transactional_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::check_test_data(&db);
        {
            let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
            assert!(
                !keyvalue::KVReadTransaction::table_names(&read)
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::TransactionalKVDB::begin_write(&db).unwrap();
        keyvalue::KVWriteTransaction::clear(&mut write).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&write)
                .unwrap()
                .is_empty()
        );
        keyvalue::KVWriteTransaction::commit(write).unwrap();
        let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&read)
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "transactional", feature = "fjall"))]
    #[test]
    fn test_transactional_fjall_ryow() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_transactional_fjall_ryow_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_transactional_ryow(&db);
    }

    #[cfg(all(feature = "async", feature = "transactional", feature = "fjall"))]
    #[tokio::test]
    async fn test_async_transactional_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_transactional_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_async_transactional_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::check_test_data_async(&db).await;
        {
            let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
                .await
                .unwrap();
            assert!(
                !keyvalue::AsyncKVReadTransaction::table_names(&read)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::AsyncTransactionalKVDB::begin_write(&db)
            .await
            .unwrap();
        keyvalue::AsyncKVWriteTransaction::clear(&mut write)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&write)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKVWriteTransaction::commit(write)
            .await
            .unwrap();
        let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&read)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "versioned", feature = "transactional", feature = "fjall"))]
    #[test]
    fn test_versioned_transactional_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_vtx_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_versioned_transactional_db(&db);
    }

    #[cfg(all(
        feature = "async",
        feature = "versioned",
        feature = "transactional",
        feature = "fjall"
    ))]
    #[tokio::test]
    async fn test_async_versioned_transactional_fjall() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_vtx_fjall_db");
        let db = keyvalue::fjall::FjallDB::open(&path).unwrap();
        common::test_async_versioned_transactional_db(&db).await;
    }

    // =======================================================================
    // RocksDB
    // =======================================================================

    #[cfg(feature = "rocksdb")]
    #[test]
    fn test_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_rocksdb");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::check_test_data(&db);
        assert!(!keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
        keyvalue::KeyValueDB::clear(&db).unwrap();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn test_rocksdb_edge_cases() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_rocksdb_edge");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_db_edge_cases(&db);
    }

    #[cfg(all(feature = "async", feature = "rocksdb"))]
    #[tokio::test]
    async fn test_async_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_rocksdb");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::check_test_data_async(&db).await;
        assert!(
            !keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKeyValueDB::clear(&db).await.unwrap();
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "versioned", feature = "rocksdb"))]
    #[test]
    fn test_versioned_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_versioned_rocksdb");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_versioned_db(&db);
    }

    #[cfg(all(feature = "versioned", feature = "rocksdb"))]
    #[test]
    fn test_versioned_rocksdb_soft_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_versioned_rocksdb_soft");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_versioned_soft_delete(&db);
    }

    #[cfg(all(feature = "async", feature = "versioned", feature = "rocksdb"))]
    #[tokio::test]
    async fn test_async_versioned_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_versioned_rocksdb");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_async_versioned_db(&db).await;
    }

    #[cfg(all(feature = "transactional", feature = "rocksdb"))]
    #[test]
    fn test_transactional_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_transactional_rocksdb_db");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_transactional_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::check_test_data(&db);
        {
            let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
            assert!(
                !keyvalue::KVReadTransaction::table_names(&read)
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::TransactionalKVDB::begin_write(&db).unwrap();
        keyvalue::KVWriteTransaction::clear(&mut write).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&write)
                .unwrap()
                .is_empty()
        );
        keyvalue::KVWriteTransaction::commit(write).unwrap();
        let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
        assert!(
            keyvalue::KVReadTransaction::table_names(&read)
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "transactional", feature = "rocksdb"))]
    #[test]
    fn test_transactional_rocksdb_ryow() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_transactional_rocksdb_ryow_db");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_transactional_ryow(&db);
    }

    #[cfg(all(feature = "async", feature = "transactional", feature = "rocksdb"))]
    #[tokio::test]
    async fn test_async_transactional_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_transactional_rocksdb_db");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_async_transactional_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::check_test_data_async(&db).await;
        {
            let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
                .await
                .unwrap();
            assert!(
                !keyvalue::AsyncKVReadTransaction::table_names(&read)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::AsyncTransactionalKVDB::begin_write(&db)
            .await
            .unwrap();
        keyvalue::AsyncKVWriteTransaction::clear(&mut write)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&write)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKVWriteTransaction::commit(write)
            .await
            .unwrap();
        let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&read)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "versioned", feature = "transactional", feature = "rocksdb"))]
    #[test]
    fn test_versioned_transactional_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_vtx_rocksdb_db");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_versioned_transactional_db(&db);
    }

    #[cfg(all(
        feature = "async",
        feature = "versioned",
        feature = "transactional",
        feature = "rocksdb"
    ))]
    #[tokio::test]
    async fn test_async_versioned_transactional_rocksdb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_vtx_rocksdb_db");
        let db = keyvalue::rocksdb::RocksDB::open(&path).unwrap();
        common::test_async_versioned_transactional_db(&db).await;
    }

    // =======================================================================
    // SqliteDB (async-only)
    // =======================================================================

    #[cfg(all(feature = "async", feature = "sqlite"))]
    #[tokio::test]
    async fn test_async_sqlite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_sqlite_db");
        let db = keyvalue::sqlite::SqliteDB::open(&path).await.unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::sqlite::SqliteDB::open(&path).await.unwrap();
        common::check_test_data_async(&db).await;
        assert!(
            !keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKeyValueDB::clear(&db).await.unwrap();
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "async", feature = "sqlite"))]
    #[tokio::test]
    async fn test_async_sqlite_edge_cases() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_sqlite_edge_db");
        let db = keyvalue::sqlite::SqliteDB::open(&path).await.unwrap();
        common::test_async_db_edge_cases(&db).await;
    }

    #[cfg(all(feature = "async", feature = "versioned", feature = "sqlite"))]
    #[tokio::test]
    async fn test_async_versioned_sqlite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_versioned_sqlite_db");
        let db = keyvalue::sqlite::SqliteDB::open(&path).await.unwrap();
        common::test_async_versioned_db(&db).await;
    }

    #[cfg(all(feature = "async", feature = "transactional", feature = "sqlite"))]
    #[tokio::test]
    async fn test_async_transactional_sqlite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_transactional_sqlite_db");
        let db = keyvalue::sqlite::SqliteDB::open(&path).await.unwrap();
        common::test_async_transactional_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::sqlite::SqliteDB::open(&path).await.unwrap();
        common::check_test_data_async(&db).await;
        {
            let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
                .await
                .unwrap();
            assert!(
                !keyvalue::AsyncKVReadTransaction::table_names(&read)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let mut write = keyvalue::AsyncTransactionalKVDB::begin_write(&db)
            .await
            .unwrap();
        keyvalue::AsyncKVWriteTransaction::clear(&mut write)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&write)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKVWriteTransaction::commit(write)
            .await
            .unwrap();
        let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
            .await
            .unwrap();
        assert!(
            keyvalue::AsyncKVReadTransaction::table_names(&read)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(
        feature = "async",
        feature = "versioned",
        feature = "transactional",
        feature = "sqlite"
    ))]
    #[tokio::test]
    async fn test_async_versioned_transactional_sqlite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test_async_vtx_sqlite_db");
        let db = keyvalue::sqlite::SqliteDB::open(&path).await.unwrap();
        common::test_async_versioned_transactional_db(&db).await;
    }

    // =======================================================================
    // AwsS3DB (async-only, requires env vars)
    // =======================================================================

    #[cfg(all(feature = "async", feature = "aws-s3"))]
    #[tokio::test]
    async fn test_async_aws_s3() {
        let endpoint_url = std::env::var("AWS_S3_ENDPOINT_URL").expect("AWS_S3_ENDPOINT_URL");
        let region = std::env::var("AWS_S3_REGION").expect("AWS_S3_REGION");
        let credentials = aws_credential_types::Credentials::from_keys(
            std::env::var("AWS_S3_ACCESS_KEY_ID").expect("AWS_S3_ACCESS_KEY_ID"),
            std::env::var("AWS_S3_SECRET").expect("AWS_S3_SECRET"),
            None,
        );
        let bucket_name = "test-aws-s3-db";
        let db = keyvalue::aws_s3::AwsS3DB::open(
            &endpoint_url,
            &region,
            credentials.clone(),
            bucket_name,
        )
        .await
        .unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::aws_s3::AwsS3DB::open(&endpoint_url, &region, credentials, bucket_name)
            .await
            .unwrap();
        common::check_test_data_async(&db).await;
        assert!(
            !keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
        keyvalue::AsyncKeyValueDB::clear(&db).await.unwrap();
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
