mod common;

#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::common;

    #[cfg(feature = "in-memory")]
    #[test]
    fn test_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(all(feature = "async", feature = "in-memory"))]
    #[tokio::test]
    async fn test_async_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(keyvalue::AsyncKeyValueDB::table_names(&db)
            .await
            .unwrap()
            .is_empty());
    }

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
        assert!(!keyvalue::AsyncKeyValueDB::table_names(&db)
            .await
            .unwrap()
            .is_empty());
        keyvalue::AsyncKeyValueDB::clear(&db).await.unwrap();
        assert!(keyvalue::AsyncKeyValueDB::table_names(&db)
            .await
            .unwrap()
            .is_empty());
    }
}
