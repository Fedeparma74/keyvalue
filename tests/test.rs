#[cfg(all(feature = "test", not(target_arch = "wasm32")))]
mod common;

#[cfg(all(feature = "test", not(target_arch = "wasm32")))]
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
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(all(feature = "versioned", feature = "in-memory"))]
    #[test]
    fn test_versioned_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_versioned_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(all(feature = "async", feature = "versioned", feature = "in-memory"))]
    #[tokio::test]
    async fn test_async_versioned_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_async_versioned_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(
            keyvalue::AsyncKeyValueDB::table_names(&db)
                .await
                .unwrap()
                .is_empty()
        );
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
        let read = keyvalue::TransactionalKVDB::begin_read(&db).unwrap();
        assert!(
            !keyvalue::KVReadTransaction::table_names(&read)
                .unwrap()
                .is_empty()
        );
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
        let read = keyvalue::AsyncTransactionalKVDB::begin_read(&db)
            .await
            .unwrap();
        assert!(
            !keyvalue::AsyncKVReadTransaction::table_names(&read)
                .await
                .unwrap()
                .is_empty()
        );
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
