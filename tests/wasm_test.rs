#[cfg(all(feature = "test-wasm", target_arch = "wasm32"))]
mod common;

#[cfg(all(feature = "test-wasm", target_arch = "wasm32"))]
mod tests {
    use super::common;

    #[cfg(feature = "async")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    #[cfg(feature = "in-memory")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn test_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(all(feature = "async", feature = "in-memory"))]
    #[wasm_bindgen_test::wasm_bindgen_test]
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
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn test_versioned_in_memory() {
        let db = keyvalue::in_memory::InMemoryDB::new();
        common::test_versioned_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::in_memory::InMemoryDB::new();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(all(feature = "async", feature = "versioned", feature = "in-memory"))]
    #[wasm_bindgen_test::wasm_bindgen_test]
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

    #[cfg(feature = "local-storage")]
    #[wasm_bindgen_test::wasm_bindgen_test]
    fn test_local_storage() {
        let name = "test_local_storage_db";
        let db = keyvalue::local_storage::LocalStorageDB::open(name).unwrap();
        common::test_db(&db);
        common::persist_test_data(Box::new(db));
        let db = keyvalue::local_storage::LocalStorageDB::open(name).unwrap();
        common::check_test_data(&db);
        assert!(!keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
        keyvalue::KeyValueDB::clear(&db).unwrap();
        assert!(keyvalue::KeyValueDB::table_names(&db).unwrap().is_empty());
    }

    #[cfg(all(feature = "async", feature = "local-storage"))]
    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn test_async_local_storage() {
        let name = "test_async_local_storage_db";
        let db = keyvalue::local_storage::LocalStorageDB::open(name).unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::local_storage::LocalStorageDB::open(name).unwrap();
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

    #[cfg(all(feature = "async", feature = "indexed-db"))]
    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn test_async_indexed_db() {
        let name = "test_async_indexed_db_db";
        let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
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

    #[cfg(all(feature = "async", feature = "transactional", feature = "indexed-db"))]
    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn test_async_transactional_indexed_db() {
        let name = "test_async_transactional_indexed_db_db";
        let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
        common::test_async_transactional_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
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
        feature = "indexed-db",
        target_feature = "atomics",
        target_feature = "bulk-memory",
        target_feature = "mutable-globals"
    ))]
    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn test_async_indexed_db_in_worker() {
        wasmt::task::spawn(async move {
            let name = "test_async_indexed_db_in_worker_db";
            let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
            futures::executor::block_on(common::test_async_db(&db));
            common::persist_test_data_async(Box::new(db)).await;
            let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
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
        })
        .join()
        .await
        .unwrap();
    }

    #[cfg(all(feature = "async", feature = "aws-s3"))]
    #[wasm_bindgen_test::wasm_bindgen_test]
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
