mod common;

#[cfg(target_arch = "wasm32")]
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
        assert!(keyvalue::AsyncKeyValueDB::table_names(&db)
            .await
            .unwrap()
            .is_empty());
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

    #[cfg(all(feature = "async", feature = "indexed-db"))]
    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn test_async_indexed_db() {
        let name = "test_async_indexed_db_db";
        let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
        common::test_async_db(&db).await;
        common::persist_test_data_async(Box::new(db)).await;
        let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
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

    // #[cfg(all(feature = "async", feature = "indexed-db"))]
    // #[wasm_bindgen_test::wasm_bindgen_test]
    // async fn test_async_indexed_db_in_worker() {
    //     tokio_with_wasm::tokio::task::spawn_blocking(move || {
    //         tokio_with_wasm::tokio::task::spawn(async move {
    //             let name = "test_async_indexed_db_db";
    //             let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
    //             common::test_async_db(&db).await;
    //             common::persist_test_data_async(Box::new(db)).await;
    //             let db = keyvalue::indexed_db::IndexedDB::open(name).await.unwrap();
    //             common::check_test_data_async(&db).await;
    //             assert!(!keyvalue::AsyncKeyValueDB::table_names(&db)
    //                 .await
    //                 .unwrap()
    //                 .is_empty());
    //             keyvalue::AsyncKeyValueDB::clear(&db).await.unwrap();
    //             assert!(keyvalue::AsyncKeyValueDB::table_names(&db)
    //                 .await
    //                 .unwrap()
    //                 .is_empty());
    //         })
    //     });
    // }
}
