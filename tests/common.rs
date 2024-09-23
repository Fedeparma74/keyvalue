use const_format::formatcp;

const TEST_PREFIX: &str = "prefix";
const TEST_DATA: [(&str, &str, &[u8]); 4] = [
    ("table1", "key", "value".as_bytes()),
    ("table1", formatcp!("{}1", TEST_PREFIX), "value1".as_bytes()),
    ("table1", formatcp!("{}2", TEST_PREFIX), "value2".as_bytes()),
    ("table2", "key", "value".as_bytes()),
];

pub fn test_db<D: keyvalue::KeyValueDB>(db: &D) {
    let (table1, key, value) = TEST_DATA[0];

    assert!(db.get(table1, key).unwrap().is_none());
    assert!(db.iter_from_prefix(table1, key).unwrap().is_empty());
    assert!(db.iter(table1).unwrap().is_empty());
    assert!(db.remove(table1, key).unwrap().is_none());
    assert!(!db.contains_key(table1, key).unwrap());
    assert!(db.table_names().unwrap().is_empty());
    assert!(db.keys(table1).unwrap().is_empty());
    assert!(db.values(table1).unwrap().is_empty());
    assert!(db.delete_table(table1).is_ok());
    assert!(db.clear().is_ok());

    assert!(db.insert(table1, key, value).unwrap().is_none());
    assert_eq!(db.get(table1, key).unwrap(), Some(value.to_vec()));

    assert_eq!(db.insert(table1, key, value).unwrap(), Some(value.to_vec()));
    assert_eq!(db.get(table1, key).unwrap(), Some(value.to_vec()));

    assert!(db.insert(table1, key, &[]).unwrap().is_some());
    assert_eq!(db.get(table1, key).unwrap(), Some(vec![]));

    assert!(db.remove(table1, key).unwrap().is_some());
    assert!(db.get(table1, key).unwrap().is_none());

    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    assert_eq!(db.get(table1, key1).unwrap(), None);
    assert_eq!(db.get(table1, key2).unwrap(), None);

    assert!(db.insert(table1, key1, value1).unwrap().is_none());
    assert!(db.insert(table1, key2, value2).unwrap().is_none());

    let iter = db.iter_from_prefix(table1, prefix).unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let iter = db.iter(table1).unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let keys = db.keys(table1).unwrap();
    assert!(keys.len() == 2);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));

    let values = db.values(table1).unwrap();
    assert!(values.len() == 2);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));

    assert!(db.contains_key(table1, key1).unwrap());
    assert!(db.contains_key(table1, key2).unwrap());
    assert!(!db.contains_key(table1, "non-existent").unwrap());
    assert_eq!(db.table_names().unwrap(), vec![table1.to_string()]);

    let (table2, key, value) = TEST_DATA[3];

    assert!(db.insert(table2, key, value).unwrap().is_none());
    assert_eq!(db.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(db.insert(table2, key, value).unwrap(), Some(value.to_vec()));

    assert!(db.delete_table(table1).is_ok());
    assert!(db.get(table1, key1).unwrap().is_none());
    assert!(db.get(table1, key2).unwrap().is_none());
    assert!(db.iter_from_prefix(table1, prefix).unwrap().is_empty());
    assert!(db.iter(table1).unwrap().is_empty());
    assert!(db.keys(table1).unwrap().is_empty());
    assert!(db.values(table1).unwrap().is_empty());
    assert!(!db.contains_key(table1, key1).unwrap());
    assert!(!db.contains_key(table1, key2).unwrap());
    assert!(!db.contains_key(table1, "non-existent").unwrap());
    assert_eq!(db.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(db.table_names().unwrap(), vec![table2.to_string()]);

    assert!(db.clear().is_ok());
    assert!(db.get(table2, key).unwrap().is_none());
    assert!(db.iter_from_prefix(table1, prefix).unwrap().is_empty());
    assert!(db.iter(table2).unwrap().is_empty());
    assert!(db.keys(table1).unwrap().is_empty());
    assert!(db.values(table1).unwrap().is_empty());
    assert!(!db.contains_key(table1, key1).unwrap());
    assert!(!db.contains_key(table1, key2).unwrap());
    assert!(!db.contains_key(table1, "non-existent").unwrap());
    assert!(db.table_names().unwrap().is_empty());
}

#[cfg(feature = "async")]
pub async fn test_async_db<D: keyvalue::AsyncKeyValueDB>(db: &D) {
    let (table1, key, value) = TEST_DATA[0];

    assert!(db.get(table1, key).await.unwrap().is_none());
    assert!(db.iter_from_prefix(table1, key).await.unwrap().is_empty());
    assert!(db.iter(table1).await.unwrap().is_empty());
    assert!(db.remove(table1, key).await.unwrap().is_none());
    assert!(!db.contains_key(table1, key).await.unwrap());
    assert!(db.table_names().await.unwrap().is_empty());
    assert!(db.keys(table1).await.unwrap().is_empty());
    assert!(db.values(table1).await.unwrap().is_empty());
    assert!(db.delete_table(table1).await.is_ok());
    assert!(db.clear().await.is_ok());

    assert!(db.insert(table1, key, value).await.unwrap().is_none());
    assert_eq!(db.get(table1, key).await.unwrap(), Some(value.to_vec()));

    assert_eq!(
        db.insert(table1, key, value).await.unwrap(),
        Some(value.to_vec())
    );
    assert_eq!(db.get(table1, key).await.unwrap(), Some(value.to_vec()));

    assert!(db.insert(table1, key, &[]).await.unwrap().is_some());
    assert_eq!(db.get(table1, key).await.unwrap(), Some(vec![]));

    assert!(db.remove(table1, key).await.unwrap().is_some());
    assert!(db.get(table1, key).await.unwrap().is_none());

    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    assert_eq!(db.get(table1, key1).await.unwrap(), None);
    assert_eq!(db.get(table1, key2).await.unwrap(), None);

    assert!(db.insert(table1, key1, value1).await.unwrap().is_none());
    assert!(db.insert(table1, key2, value2).await.unwrap().is_none());

    let iter = db.iter_from_prefix(table1, prefix).await.unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let iter = db.iter(table1).await.unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let keys = db.keys(table1).await.unwrap();
    assert!(keys.len() == 2);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));

    let values = db.values(table1).await.unwrap();
    assert!(values.len() == 2);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));

    assert!(db.contains_key(table1, key1).await.unwrap());
    assert!(db.contains_key(table1, key2).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(db.table_names().await.unwrap(), vec![table1.to_string()]);

    let (table2, key, value) = TEST_DATA[3];

    assert!(db.insert(table2, key, value).await.unwrap().is_none());
    assert_eq!(db.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(
        db.insert(table2, key, value).await.unwrap(),
        Some(value.to_vec())
    );

    assert!(db.delete_table(table1).await.is_ok());
    assert!(db.get(table1, key1).await.unwrap().is_none());
    assert!(db.get(table1, key2).await.unwrap().is_none());
    assert!(db
        .iter_from_prefix(table1, prefix)
        .await
        .unwrap()
        .is_empty());
    assert!(db.iter(table1).await.unwrap().is_empty());
    assert!(db.keys(table1).await.unwrap().is_empty());
    assert!(db.values(table1).await.unwrap().is_empty());
    assert!(!db.contains_key(table1, key1).await.unwrap());
    assert!(!db.contains_key(table1, key2).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(db.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(db.table_names().await.unwrap(), vec![table2.to_string()]);

    assert!(db.clear().await.is_ok());
    assert!(db.get(table2, key).await.unwrap().is_none());
    assert!(db
        .iter_from_prefix(table1, prefix)
        .await
        .unwrap()
        .is_empty());
    assert!(db.iter(table2).await.unwrap().is_empty());
    assert!(db.keys(table1).await.unwrap().is_empty());
    assert!(db.values(table1).await.unwrap().is_empty());
    assert!(!db.contains_key(table1, key1).await.unwrap());
    assert!(!db.contains_key(table1, key2).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert!(db.table_names().await.unwrap().is_empty());
}

#[cfg(feature = "transactional")]
pub fn test_transactional_db<D: keyvalue::TransactionalKVDB>(db: &D) {
    use keyvalue::{KVReadTransaction, KVWriteTransaction};

    let (table1, key, value) = TEST_DATA[0];

    let read = db.begin_read().unwrap();
    assert!(read.get(table1, key).unwrap().is_none());
    assert!(read.iter_from_prefix(table1, key).unwrap().is_empty());
    assert!(read.iter(table1).unwrap().is_empty());
    assert!(!read.contains_key(table1, key).unwrap());
    assert!(read.table_names().unwrap().is_empty());
    assert!(read.keys(table1).unwrap().is_empty());
    assert!(read.values(table1).unwrap().is_empty());

    let mut write = db.begin_write().unwrap();
    assert!(write.get(table1, key).unwrap().is_none());
    assert!(write.iter_from_prefix(table1, key).unwrap().is_empty());
    assert!(write.iter(table1).unwrap().is_empty());
    assert!(write.remove(table1, key).unwrap().is_none());
    assert!(!write.contains_key(table1, key).unwrap());
    assert!(write.table_names().unwrap().is_empty());
    assert!(write.keys(table1).unwrap().is_empty());
    assert!(write.values(table1).unwrap().is_empty());
    assert!(write.delete_table(table1).is_ok());
    assert!(write.clear().is_ok());
    assert!(write.commit().is_ok());

    let read = db.begin_read().unwrap();
    assert!(read.get(table1, key).unwrap().is_none());
    assert!(read.iter_from_prefix(table1, key).unwrap().is_empty());
    assert!(read.iter(table1).unwrap().is_empty());
    assert!(!read.contains_key(table1, key).unwrap());
    assert!(read.table_names().unwrap().is_empty());
    assert!(read.keys(table1).unwrap().is_empty());
    assert!(read.values(table1).unwrap().is_empty());

    let mut write = db.begin_write().unwrap();
    assert!(write.insert(table1, key, value).unwrap().is_none());
    assert_eq!(write.get(table1, key).unwrap(), Some(value.to_vec()));

    let read = db.begin_read().unwrap();
    assert_eq!(read.get(table1, key).unwrap(), None);

    assert_eq!(
        write.insert(table1, key, value).unwrap(),
        Some(value.to_vec())
    );
    assert_eq!(write.get(table1, key).unwrap(), Some(value.to_vec()));

    write.commit().unwrap();

    let read = db.begin_read().unwrap();
    assert_eq!(read.get(table1, key).unwrap(), Some(value.to_vec()));

    let mut write = db.begin_write().unwrap();
    assert!(write.insert(table1, key, &[]).unwrap().is_some());
    assert_eq!(write.get(table1, key).unwrap(), Some(vec![]));

    write.commit().unwrap();

    let read = db.begin_read().unwrap();
    assert_eq!(read.get(table1, key).unwrap(), Some(vec![]));

    let mut write = db.begin_write().unwrap();
    assert!(write.remove(table1, key).unwrap().is_some());
    assert!(write.get(table1, key).unwrap().is_none());

    let read = db.begin_read().unwrap();
    assert_eq!(read.get(table1, key).unwrap(), Some(vec![]));

    write.commit().unwrap();

    let read = db.begin_read().unwrap();
    assert!(read.get(table1, key).unwrap().is_none());

    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    let mut write = db.begin_write().unwrap();
    assert_eq!(write.get(table1, key1).unwrap(), None);
    assert_eq!(write.get(table1, key2).unwrap(), None);

    assert!(write.insert(table1, key1, value1).unwrap().is_none());
    assert!(write.insert(table1, key2, value2).unwrap().is_none());

    write.commit().unwrap();

    let read = db.begin_read().unwrap();
    let iter = read.iter_from_prefix(table1, prefix).unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let iter = read.iter(table1).unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let keys = read.keys(table1).unwrap();
    assert!(keys.len() == 2);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));

    let values = read.values(table1).unwrap();
    assert!(values.len() == 2);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));

    assert!(read.contains_key(table1, key1).unwrap());
    assert!(read.contains_key(table1, key2).unwrap());
    assert!(!read.contains_key(table1, "non-existent").unwrap());
    assert_eq!(read.table_names().unwrap(), vec![table1.to_string()]);

    let (table2, key, value) = TEST_DATA[3];

    let mut write = db.begin_write().unwrap();
    assert!(write.insert(table2, key, value).unwrap().is_none());
    assert_eq!(write.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(
        write.insert(table2, key, value).unwrap(),
        Some(value.to_vec())
    );

    write.commit().unwrap();

    let mut write = db.begin_write().unwrap();
    assert!(write.delete_table(table1).is_ok());
    assert!(write.get(table1, key1).unwrap().is_none());
    assert!(write.get(table1, key2).unwrap().is_none());
    assert!(write.iter_from_prefix(table1, prefix).unwrap().is_empty());
    assert!(write.iter(table1).unwrap().is_empty());
    assert!(write.keys(table1).unwrap().is_empty());
    assert!(write.values(table1).unwrap().is_empty());
    assert!(!write.contains_key(table1, key1).unwrap());
    assert!(!write.contains_key(table1, key2).unwrap());
    assert!(!write.contains_key(table1, "non-existent").unwrap());
    assert_eq!(write.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(write.table_names().unwrap(), vec![table2.to_string()]);

    write.commit().unwrap();

    let read = db.begin_read().unwrap();
    assert!(read.get(table1, key1).unwrap().is_none());
    assert!(read.get(table1, key2).unwrap().is_none());
    assert!(read.iter_from_prefix(table1, prefix).unwrap().is_empty());
    assert!(read.iter(table1).unwrap().is_empty());
    assert!(read.keys(table1).unwrap().is_empty());
    assert!(read.values(table1).unwrap().is_empty());
    assert!(!read.contains_key(table1, key1).unwrap());
    assert!(!read.contains_key(table1, key2).unwrap());
    assert!(!read.contains_key(table1, "non-existent").unwrap());
    assert_eq!(read.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(read.table_names().unwrap(), vec![table2.to_string()]);

    let mut write = db.begin_write().unwrap();
    assert!(write.clear().is_ok());
    assert!(write.get(table2, key).unwrap().is_none());
    assert!(write.iter_from_prefix(table1, prefix).unwrap().is_empty());
    assert!(write.iter(table2).unwrap().is_empty());
    assert!(write.keys(table1).unwrap().is_empty());
    assert!(write.values(table1).unwrap().is_empty());
    assert!(!write.contains_key(table1, key1).unwrap());
    assert!(!write.contains_key(table1, key2).unwrap());
    assert!(!write.contains_key(table1, "non-existent").unwrap());
    assert!(write.table_names().unwrap().is_empty());

    write.commit().unwrap();

    let read = db.begin_read().unwrap();
    assert!(read.get(table2, key).unwrap().is_none());
    assert!(read.iter_from_prefix(table1, prefix).unwrap().is_empty());
    assert!(read.iter(table2).unwrap().is_empty());
    assert!(read.keys(table1).unwrap().is_empty());
    assert!(read.values(table1).unwrap().is_empty());
    assert!(!read.contains_key(table1, key1).unwrap());
    assert!(!read.contains_key(table1, key2).unwrap());
    assert!(!read.contains_key(table1, "non-existent").unwrap());
    assert!(read.table_names().unwrap().is_empty());

    let mut write = db.begin_write().unwrap();
    assert!(write.insert(table1, key1, value1).unwrap().is_none());
    assert!(write.insert(table1, key2, value2).unwrap().is_none());
    assert!(write.insert(table2, key, value).unwrap().is_none());
    write.abort().unwrap();

    let read = db.begin_read().unwrap();
    assert!(read.get(table1, key1).unwrap().is_none());
    assert!(read.get(table1, key2).unwrap().is_none());
    assert!(read.get(table2, key).unwrap().is_none());
    assert!(read.iter_from_prefix(table1, prefix).unwrap().is_empty());
}

#[cfg(all(feature = "async", feature = "transactional"))]
pub async fn test_async_transactional_db<D: keyvalue::AsyncTransactionalKVDB>(db: &D) {
    use keyvalue::{AsyncKVReadTransaction, AsyncKVWriteTransaction};

    let (table1, key, value) = TEST_DATA[0];

    let read = db.begin_read().await.unwrap();
    assert!(read.get(table1, key).await.unwrap().is_none());
    assert!(read.iter_from_prefix(table1, key).await.unwrap().is_empty());
    assert!(read.iter(table1).await.unwrap().is_empty());
    assert!(!read.contains_key(table1, key).await.unwrap());
    assert!(read.table_names().await.unwrap().is_empty());
    assert!(read.keys(table1).await.unwrap().is_empty());
    assert!(read.values(table1).await.unwrap().is_empty());

    let mut write = db.begin_write().await.unwrap();
    assert!(write.get(table1, key).await.unwrap().is_none());
    assert!(write
        .iter_from_prefix(table1, key)
        .await
        .unwrap()
        .is_empty());
    assert!(write.iter(table1).await.unwrap().is_empty());
    assert!(write.remove(table1, key).await.unwrap().is_none());
    assert!(!write.contains_key(table1, key).await.unwrap());
    assert!(write.table_names().await.unwrap().is_empty());
    assert!(write.keys(table1).await.unwrap().is_empty());
    assert!(write.values(table1).await.unwrap().is_empty());
    assert!(write.delete_table(table1).await.is_ok());
    assert!(write.clear().await.is_ok());
    assert!(write.commit().await.is_ok());

    let read = db.begin_read().await.unwrap();
    assert!(read.get(table1, key).await.unwrap().is_none());
    assert!(read.iter_from_prefix(table1, key).await.unwrap().is_empty());
    assert!(read.iter(table1).await.unwrap().is_empty());
    assert!(!read.contains_key(table1, key).await.unwrap());
    assert!(read.table_names().await.unwrap().is_empty());
    assert!(read.keys(table1).await.unwrap().is_empty());
    assert!(read.values(table1).await.unwrap().is_empty());

    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table1, key, value).await.unwrap().is_none());
    assert_eq!(write.get(table1, key).await.unwrap(), Some(value.to_vec()));

    let read = db.begin_read().await.unwrap();
    assert_eq!(read.get(table1, key).await.unwrap(), None);

    assert_eq!(
        write.insert(table1, key, value).await.unwrap(),
        Some(value.to_vec())
    );
    assert_eq!(write.get(table1, key).await.unwrap(), Some(value.to_vec()));

    write.commit().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert_eq!(read.get(table1, key).await.unwrap(), Some(value.to_vec()));

    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table1, key, &[]).await.unwrap().is_some());
    assert_eq!(write.get(table1, key).await.unwrap(), Some(vec![]));

    write.commit().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert_eq!(read.get(table1, key).await.unwrap(), Some(vec![]));

    let mut write = db.begin_write().await.unwrap();
    assert!(write.remove(table1, key).await.unwrap().is_some());
    assert!(write.get(table1, key).await.unwrap().is_none());

    let read = db.begin_read().await.unwrap();
    assert_eq!(read.get(table1, key).await.unwrap(), Some(vec![]));

    write.commit().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert!(read.get(table1, key).await.unwrap().is_none());

    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    let mut write = db.begin_write().await.unwrap();
    assert_eq!(write.get(table1, key1).await.unwrap(), None);
    assert_eq!(write.get(table1, key2).await.unwrap(), None);

    assert!(write.insert(table1, key1, value1).await.unwrap().is_none());
    assert!(write.insert(table1, key2, value2).await.unwrap().is_none());

    write.commit().await.unwrap();

    let read = db.begin_read().await.unwrap();
    let iter = read.iter_from_prefix(table1, prefix).await.unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let iter = read.iter(table1).await.unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let keys = read.keys(table1).await.unwrap();
    assert!(keys.len() == 2);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));

    let values = read.values(table1).await.unwrap();
    assert!(values.len() == 2);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));

    assert!(read.contains_key(table1, key1).await.unwrap());
    assert!(read.contains_key(table1, key2).await.unwrap());
    assert!(!read.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(read.table_names().await.unwrap(), vec![table1.to_string()]);

    let (table2, key, value) = TEST_DATA[3];

    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table2, key, value).await.unwrap().is_none());
    assert_eq!(write.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(
        write.insert(table2, key, value).await.unwrap(),
        Some(value.to_vec())
    );

    write.commit().await.unwrap();

    let mut write = db.begin_write().await.unwrap();
    assert!(write.delete_table(table1).await.is_ok());
    assert!(write.get(table1, key1).await.unwrap().is_none());
    assert!(write.get(table1, key2).await.unwrap().is_none());
    assert!(write
        .iter_from_prefix(table1, prefix)
        .await
        .unwrap()
        .is_empty());
    assert!(write.iter(table1).await.unwrap().is_empty());
    assert!(write.keys(table1).await.unwrap().is_empty());
    assert!(write.values(table1).await.unwrap().is_empty());
    assert!(!write.contains_key(table1, key1).await.unwrap());
    assert!(!write.contains_key(table1, key2).await.unwrap());
    assert!(!write.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(write.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(write.table_names().await.unwrap(), vec![table2.to_string()]);

    write.commit().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert!(read.get(table1, key1).await.unwrap().is_none());
    assert!(read.get(table1, key2).await.unwrap().is_none());
    assert!(read
        .iter_from_prefix(table1, prefix)
        .await
        .unwrap()
        .is_empty());
    assert!(read.iter(table1).await.unwrap().is_empty());
    assert!(read.keys(table1).await.unwrap().is_empty());
    assert!(read.values(table1).await.unwrap().is_empty());
    assert!(!read.contains_key(table1, key1).await.unwrap());
    assert!(!read.contains_key(table1, key2).await.unwrap());
    assert!(!read.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(read.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(read.table_names().await.unwrap(), vec![table2.to_string()]);

    let mut write = db.begin_write().await.unwrap();
    assert!(write.clear().await.is_ok());
    assert!(write.get(table2, key).await.unwrap().is_none());
    assert!(write
        .iter_from_prefix(table1, prefix)
        .await
        .unwrap()
        .is_empty());
    assert!(write.iter(table2).await.unwrap().is_empty());
    assert!(write.keys(table1).await.unwrap().is_empty());
    assert!(write.values(table1).await.unwrap().is_empty());
    assert!(!write.contains_key(table1, key1).await.unwrap());
    assert!(!write.contains_key(table1, key2).await.unwrap());
    assert!(!write.contains_key(table1, "non-existent").await.unwrap());
    assert!(write.table_names().await.unwrap().is_empty());

    write.commit().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert!(read.get(table2, key).await.unwrap().is_none());
    assert!(read
        .iter_from_prefix(table1, prefix)
        .await
        .unwrap()
        .is_empty());
    assert!(read.iter(table2).await.unwrap().is_empty());
    assert!(read.keys(table1).await.unwrap().is_empty());
    assert!(read.values(table1).await.unwrap().is_empty());
    assert!(!read.contains_key(table1, key1).await.unwrap());
    assert!(!read.contains_key(table1, key2).await.unwrap());
    assert!(!read.contains_key(table1, "non-existent").await.unwrap());
    assert!(read.table_names().await.unwrap().is_empty());

    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table1, key1, value1).await.unwrap().is_none());
    assert!(write.insert(table1, key2, value2).await.unwrap().is_none());
    assert!(write.insert(table2, key, value).await.unwrap().is_none());
    write.abort().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert!(read.get(table1, key1).await.unwrap().is_none());
    assert!(read.get(table1, key2).await.unwrap().is_none());
    assert!(read.get(table2, key).await.unwrap().is_none());
    assert!(read
        .iter_from_prefix(table1, prefix)
        .await
        .unwrap()
        .is_empty());
}

pub fn persist_test_data(db: Box<dyn keyvalue::KeyValueDB>) {
    for (table_name, key, value) in TEST_DATA.iter() {
        db.insert(table_name, key, value).unwrap();
    }
}

pub fn check_test_data(db: &dyn keyvalue::KeyValueDB) {
    for (table_name, key, value) in TEST_DATA.iter() {
        assert_eq!(db.get(table_name, key).unwrap(), Some(value.to_vec()));
    }

    let (table1, key1, value1) = TEST_DATA[1];
    let (_, key2, value2) = TEST_DATA[2];
    let (_, key3, value3) = TEST_DATA[3];
    let iter = db.iter_from_prefix(table1, TEST_PREFIX).unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let (table2, key, value) = TEST_DATA[3];
    let iter = db.iter_from_prefix(table2, "").unwrap();
    assert!(iter.len() == 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let iter = db.iter(table1).unwrap();
    assert!(iter.len() == 3);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));
    assert!(iter.contains(&(key3.to_string(), value3.to_vec())));

    let iter = db.iter(table2).unwrap();
    assert!(iter.len() == 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let keys = db.keys(table1).unwrap();
    assert!(keys.len() == 3);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));
    assert!(keys.contains(&key3.to_string()));

    let keys = db.keys(table2).unwrap();
    assert!(keys.len() == 1);
    assert!(keys.contains(&key.to_string()));

    let values = db.values(table1).unwrap();
    assert!(values.len() == 3);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));
    assert!(values.contains(&value3.to_vec()));

    let values = db.values(table2).unwrap();
    assert!(values.len() == 1);
    assert!(values.contains(&value.to_vec()));

    assert!(db.contains_key(table1, key1).unwrap());
    assert!(db.contains_key(table1, key2).unwrap());
    assert!(db.contains_key(table1, key3).unwrap());
    assert!(!db.contains_key(table1, "non-existent").unwrap());
    assert!(db.contains_key(table2, key).unwrap());
    assert!(!db.contains_key(table2, "non-existent").unwrap());

    let table_names = db.table_names().unwrap();
    assert!(table_names.len() == 2);
    assert!(table_names.contains(&table1.to_string()));
    assert!(table_names.contains(&table2.to_string()));
}

#[cfg(feature = "async")]
pub async fn persist_test_data_async(db: Box<dyn keyvalue::AsyncKeyValueDB>) {
    for (table_name, key, value) in TEST_DATA.iter() {
        db.insert(table_name, key, value).await.unwrap();
    }
}

#[cfg(feature = "async")]
pub async fn check_test_data_async(db: &dyn keyvalue::AsyncKeyValueDB) {
    for (table_name, key, value) in TEST_DATA.iter() {
        assert_eq!(db.get(table_name, key).await.unwrap(), Some(value.to_vec()));
    }

    let (table1, key1, value1) = TEST_DATA[1];
    let (_, key2, value2) = TEST_DATA[2];
    let (_, key3, value3) = TEST_DATA[3];
    let iter = db.iter_from_prefix(table1, TEST_PREFIX).await.unwrap();
    assert!(iter.len() == 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let (table2, key, value) = TEST_DATA[3];
    let iter = db.iter_from_prefix(table2, "").await.unwrap();
    assert!(iter.len() == 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let iter = db.iter(table1).await.unwrap();
    assert!(iter.len() == 3);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));
    assert!(iter.contains(&(key3.to_string(), value3.to_vec())));

    let iter = db.iter(table2).await.unwrap();
    assert!(iter.len() == 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let keys = db.keys(table1).await.unwrap();
    assert!(keys.len() == 3);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));
    assert!(keys.contains(&key3.to_string()));

    let keys = db.keys(table2).await.unwrap();
    assert!(keys.len() == 1);
    assert!(keys.contains(&key.to_string()));

    let values = db.values(table1).await.unwrap();
    assert!(values.len() == 3);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));
    assert!(values.contains(&value3.to_vec()));

    let values = db.values(table2).await.unwrap();
    assert!(values.len() == 1);
    assert!(values.contains(&value.to_vec()));

    assert!(db.contains_key(table1, key1).await.unwrap());
    assert!(db.contains_key(table1, key2).await.unwrap());
    assert!(db.contains_key(table1, key3).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert!(db.contains_key(table2, key).await.unwrap());
    assert!(!db.contains_key(table2, "non-existent").await.unwrap());

    let table_names = db.table_names().await.unwrap();
    assert!(table_names.len() == 2);
    assert!(table_names.contains(&table1.to_string()));
    assert!(table_names.contains(&table2.to_string()));
}
