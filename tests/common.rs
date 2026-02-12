//! Shared test helpers used by all backend integration tests.
//!
//! Each helper exercises the full contract of the corresponding trait
//! (`KeyValueDB`, `AsyncKeyValueDB`, `TransactionalKVDB`, etc.) including
//! edge cases, empty-value handling, multi-table operations, and cleanup.

use const_format::formatcp;

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const TEST_PREFIX: &str = "prefix";
const TEST_DATA: [(&str, &str, &[u8]); 4] = [
    ("table1", "key", "value".as_bytes()),
    ("table1", formatcp!("{}1", TEST_PREFIX), "value1".as_bytes()),
    ("table1", formatcp!("{}2", TEST_PREFIX), "value2".as_bytes()),
    ("table2", "key", "value".as_bytes()),
];

// ---------------------------------------------------------------------------
// KeyValueDB (sync)
// ---------------------------------------------------------------------------

/// Exhaustive test of the [`keyvalue::KeyValueDB`] contract.
///
/// Covers: empty-state queries, insert, get, overwrite, empty-value insert,
/// remove, prefix iteration, full iteration, keys/values helpers,
/// contains_key, contains_table, table_names, delete_table, clear,
/// and multi-table operations.
pub fn test_db<D: keyvalue::KeyValueDB>(db: &D) {
    let (table1, key, value) = TEST_DATA[0];

    // --- initial empty state -----------------------------------------------
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
    assert!(!db.contains_table(table1).unwrap());

    // --- basic insert / get / overwrite / remove ---------------------------
    assert!(db.insert(table1, key, value).unwrap().is_none());
    assert_eq!(db.get(table1, key).unwrap(), Some(value.to_vec()));
    assert!(db.contains_table(table1).unwrap());

    assert_eq!(db.insert(table1, key, value).unwrap(), Some(value.to_vec()));
    assert_eq!(db.get(table1, key).unwrap(), Some(value.to_vec()));

    // overwrite with empty value
    assert!(db.insert(table1, key, &[]).unwrap().is_some());
    assert_eq!(db.get(table1, key).unwrap(), Some(vec![]));

    assert!(db.remove(table1, key).unwrap().is_some());
    assert!(db.get(table1, key).unwrap().is_none());

    // --- prefix iteration & multi-key --------------------------------------
    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    assert_eq!(db.get(table1, key1).unwrap(), None);
    assert_eq!(db.get(table1, key2).unwrap(), None);

    assert!(db.insert(table1, key1, value1).unwrap().is_none());
    assert!(db.insert(table1, key2, value2).unwrap().is_none());

    let iter = db.iter_from_prefix(table1, prefix).unwrap();
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let iter = db.iter(table1).unwrap();
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let keys = db.keys(table1).unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));

    let values = db.values(table1).unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));

    assert!(db.contains_key(table1, key1).unwrap());
    assert!(db.contains_key(table1, key2).unwrap());
    assert!(!db.contains_key(table1, "non-existent").unwrap());
    assert_eq!(db.table_names().unwrap(), vec![table1.to_string()]);

    // --- second table ------------------------------------------------------
    let (table2, key, value) = TEST_DATA[3];

    assert!(db.insert(table2, key, value).unwrap().is_none());
    assert_eq!(db.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(db.insert(table2, key, value).unwrap(), Some(value.to_vec()));

    // --- delete_table ------------------------------------------------------
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

    // --- clear -------------------------------------------------------------
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

/// Edge-case tests for [`keyvalue::KeyValueDB`].
///
/// Covers: empty-string keys, large values, special characters in keys,
/// non-existent table operations, idempotent clear/delete, and re-insert
/// after delete.
pub fn test_db_edge_cases<D: keyvalue::KeyValueDB>(db: &D) {
    // --- large value -------------------------------------------------------
    let large = vec![0xAB_u8; 64 * 1024]; // 64 KiB
    assert!(db.insert("edge", "big", &large).unwrap().is_none());
    assert_eq!(db.get("edge", "big").unwrap(), Some(large.clone()));

    // --- special characters in keys ----------------------------------------
    let specials = [
        "key with spaces",
        "key\twith\ttabs",
        "ключ_utf8",
        "emoji_🔑",
    ];
    for s in &specials {
        assert!(db.insert("edge", s, b"v").unwrap().is_none());
        assert_eq!(db.get("edge", s).unwrap(), Some(b"v".to_vec()));
    }

    // --- operations on non-existent table ----------------------------------
    assert!(db.get("no_such_table", "k").unwrap().is_none());
    assert!(db.remove("no_such_table", "k").unwrap().is_none());
    assert!(db.iter("no_such_table").unwrap().is_empty());
    assert!(db.keys("no_such_table").unwrap().is_empty());
    assert!(db.values("no_such_table").unwrap().is_empty());
    assert!(!db.contains_table("no_such_table").unwrap());

    // --- idempotent clear/delete -------------------------------------------
    assert!(db.clear().is_ok());
    assert!(db.clear().is_ok()); // second clear on empty DB
    assert!(db.delete_table("no_such_table").is_ok()); // delete non-existent table

    // --- re-insert after delete_table --------------------------------------
    db.insert("table_x", "k1", b"a").unwrap();
    db.delete_table("table_x").unwrap();
    assert!(db.get("table_x", "k1").unwrap().is_none());
    db.insert("table_x", "k1", b"b").unwrap();
    assert_eq!(db.get("table_x", "k1").unwrap(), Some(b"b".to_vec()));

    // clean up
    db.clear().unwrap();
}

// ---------------------------------------------------------------------------
// AsyncKeyValueDB
// ---------------------------------------------------------------------------

#[cfg(feature = "async")]
pub async fn test_async_db<D: keyvalue::AsyncKeyValueDB>(db: &D) {
    let (table1, key, value) = TEST_DATA[0];

    // empty state
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
    assert!(!db.contains_table(table1).await.unwrap());

    // insert / get / overwrite
    assert!(db.insert(table1, key, value).await.unwrap().is_none());
    assert_eq!(db.get(table1, key).await.unwrap(), Some(value.to_vec()));
    assert!(db.contains_table(table1).await.unwrap());

    assert_eq!(
        db.insert(table1, key, value).await.unwrap(),
        Some(value.to_vec())
    );
    assert_eq!(db.get(table1, key).await.unwrap(), Some(value.to_vec()));

    // empty value
    assert!(db.insert(table1, key, &[]).await.unwrap().is_some());
    assert_eq!(db.get(table1, key).await.unwrap(), Some(vec![]));

    assert!(db.remove(table1, key).await.unwrap().is_some());
    assert!(db.get(table1, key).await.unwrap().is_none());

    // prefix / multi-key
    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    assert_eq!(db.get(table1, key1).await.unwrap(), None);
    assert_eq!(db.get(table1, key2).await.unwrap(), None);

    assert!(db.insert(table1, key1, value1).await.unwrap().is_none());
    assert!(db.insert(table1, key2, value2).await.unwrap().is_none());

    let iter = db.iter_from_prefix(table1, prefix).await.unwrap();
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let iter = db.iter(table1).await.unwrap();
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let keys = db.keys(table1).await.unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));

    let values = db.values(table1).await.unwrap();
    assert_eq!(values.len(), 2);
    assert!(values.contains(&value1.to_vec()));
    assert!(values.contains(&value2.to_vec()));

    assert!(db.contains_key(table1, key1).await.unwrap());
    assert!(db.contains_key(table1, key2).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(db.table_names().await.unwrap(), vec![table1.to_string()]);

    // second table
    let (table2, key, value) = TEST_DATA[3];

    assert!(db.insert(table2, key, value).await.unwrap().is_none());
    assert_eq!(db.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(
        db.insert(table2, key, value).await.unwrap(),
        Some(value.to_vec())
    );

    // delete_table
    assert!(db.delete_table(table1).await.is_ok());
    assert!(db.get(table1, key1).await.unwrap().is_none());
    assert!(db.get(table1, key2).await.unwrap().is_none());
    assert!(
        db.iter_from_prefix(table1, prefix)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(db.iter(table1).await.unwrap().is_empty());
    assert!(db.keys(table1).await.unwrap().is_empty());
    assert!(db.values(table1).await.unwrap().is_empty());
    assert!(!db.contains_key(table1, key1).await.unwrap());
    assert!(!db.contains_key(table1, key2).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(db.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(db.table_names().await.unwrap(), vec![table2.to_string()]);

    // clear
    assert!(db.clear().await.is_ok());
    assert!(db.get(table2, key).await.unwrap().is_none());
    assert!(
        db.iter_from_prefix(table1, prefix)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(db.iter(table2).await.unwrap().is_empty());
    assert!(db.keys(table1).await.unwrap().is_empty());
    assert!(db.values(table1).await.unwrap().is_empty());
    assert!(!db.contains_key(table1, key1).await.unwrap());
    assert!(!db.contains_key(table1, key2).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert!(db.table_names().await.unwrap().is_empty());
}

/// Async edge-case tests.
#[cfg(feature = "async")]
pub async fn test_async_db_edge_cases<D: keyvalue::AsyncKeyValueDB>(db: &D) {
    // large value
    let large = vec![0xCD_u8; 64 * 1024];
    assert!(db.insert("edge", "big", &large).await.unwrap().is_none());
    assert_eq!(db.get("edge", "big").await.unwrap(), Some(large));

    // special chars
    for s in &["spaced key", "tab\there", "utf8_ü", "emoji_🔐"] {
        assert!(db.insert("edge", s, b"v").await.unwrap().is_none());
        assert_eq!(db.get("edge", s).await.unwrap(), Some(b"v".to_vec()));
    }

    // non-existent table
    assert!(db.get("nope", "k").await.unwrap().is_none());
    assert!(db.remove("nope", "k").await.unwrap().is_none());
    assert!(db.iter("nope").await.unwrap().is_empty());
    assert!(!db.contains_table("nope").await.unwrap());

    // idempotent clear
    db.clear().await.unwrap();
    db.clear().await.unwrap();

    // re-insert after delete
    db.insert("tbl", "k", b"a").await.unwrap();
    db.delete_table("tbl").await.unwrap();
    assert!(db.get("tbl", "k").await.unwrap().is_none());
    db.insert("tbl", "k", b"b").await.unwrap();
    assert_eq!(db.get("tbl", "k").await.unwrap(), Some(b"b".to_vec()));

    db.clear().await.unwrap();
}

// ---------------------------------------------------------------------------
// VersionedKeyValueDB (sync)
// ---------------------------------------------------------------------------

#[cfg(feature = "versioned")]
pub fn test_versioned_db<D: keyvalue::VersionedKeyValueDB>(db: &D) {
    use keyvalue::VersionedObject;

    let (table1, key, value) = TEST_DATA[0];

    // empty state
    assert!(db.get(table1, key).unwrap().is_none());
    assert!(db.iter_from_prefix(table1, key).unwrap().is_empty());
    assert!(db.iter(table1).unwrap().is_empty());
    assert!(db.remove(table1, key).unwrap().is_none());
    assert!(!db.contains_key(table1, key).unwrap());
    assert!(db.table_names().unwrap().is_empty());
    assert!(db.keys(table1).unwrap().is_empty());
    assert!(db.values(table1).unwrap().is_empty());

    // insert with explicit version
    assert!(db.insert(table1, key, Some(value), 1).unwrap().is_none());
    assert_eq!(
        db.get(table1, key).unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 1,
        })
    );

    // overwrite bumps version
    assert_eq!(
        db.insert(table1, key, Some(value), 2).unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 1,
        })
    );
    assert_eq!(
        db.get(table1, key).unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 2,
        })
    );

    // update (auto-increment) to tombstone
    assert!(db.update(table1, key, None).unwrap().is_some());
    assert_eq!(
        db.get(table1, key).unwrap(),
        Some(VersionedObject {
            value: None,
            version: 3,
        })
    );

    // prefix/multi-key
    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];
    assert_eq!(db.get(table1, key1).unwrap(), None);
    assert_eq!(db.get(table1, key2).unwrap(), None);
    assert!(db.update(table1, key1, Some(value1)).unwrap().is_none());
    assert!(
        db.insert(table1, key2, Some(value2), 100)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.get(table1, key1).unwrap(),
        Some(VersionedObject {
            value: Some(value1.to_vec()),
            version: 1,
        })
    );
    assert_eq!(
        db.get(table1, key2).unwrap(),
        Some(VersionedObject {
            value: Some(value2.to_vec()),
            version: 100,
        })
    );

    // update existing: version goes from 100 → 101
    assert!(db.update(table1, key2, Some(value2)).unwrap().is_some());
    let iter = db.iter_from_prefix(table1, prefix).unwrap();
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(
        key1.to_string(),
        VersionedObject {
            value: Some(value1.to_vec()),
            version: 1,
        }
    )));
    assert!(iter.contains(&(
        key2.to_string(),
        VersionedObject {
            value: Some(value2.to_vec()),
            version: 101,
        }
    )));

    // full iter includes the tombstone
    let iter = db.iter(table1).unwrap();
    assert_eq!(iter.len(), 3);
    assert!(iter.contains(&(
        key.to_string(),
        VersionedObject {
            value: None,
            version: 3,
        }
    )));

    let keys = db.keys(table1).unwrap();
    assert_eq!(keys.len(), 3);
    let values = db.values(table1).unwrap();
    assert_eq!(values.len(), 3);

    assert!(db.contains_key(table1, key).unwrap());
    assert!(db.contains_key(table1, key1).unwrap());
    assert!(db.contains_key(table1, key2).unwrap());
    assert!(!db.contains_key(table1, "non-existent").unwrap());
    assert_eq!(db.table_names().unwrap(), vec![table1.to_string()]);

    // second table
    let (table2, key, value) = TEST_DATA[3];
    assert!(db.insert(table2, key, Some(value), 1).unwrap().is_none());
    assert_eq!(
        db.insert(table2, key, Some(value), 2).unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 1,
        })
    );

    // prune delete
    assert!(db.delete_table(table1, true).is_ok());
    assert!(db.get(table1, key1).unwrap().is_none());
    assert!(db.get(table1, key2).unwrap().is_none());
    assert!(db.iter(table1).unwrap().is_empty());
    assert_eq!(db.table_names().unwrap(), vec![table2.to_string()]);

    assert!(db.clear(true).is_ok());
    assert!(db.table_names().unwrap().is_empty());
}

/// Tests versioned soft-delete (prune=false) where entries become tombstones.
#[cfg(feature = "versioned")]
pub fn test_versioned_soft_delete<D: keyvalue::VersionedKeyValueDB>(db: &D) {
    db.insert("t", "a", Some(b"hello"), 1).unwrap();
    db.insert("t", "b", Some(b"world"), 1).unwrap();

    // soft-delete the table (prune=false): entries become tombstones
    db.delete_table("t", false).unwrap();

    // entries still exist but with None value and version incremented
    let a = db.get("t", "a").unwrap().unwrap();
    assert_eq!(a.value, None);
    assert_eq!(a.version, 2);

    let b = db.get("t", "b").unwrap().unwrap();
    assert_eq!(b.value, None);
    assert_eq!(b.version, 2);

    // table still shows up
    assert!(db.contains_table("t").unwrap());

    // soft-clear: sets all entries to tombstones, version incremented again
    db.clear(false).unwrap();
    let a = db.get("t", "a").unwrap().unwrap();
    assert_eq!(a.value, None);
    assert_eq!(a.version, 3);

    // prune clear
    db.clear(true).unwrap();
    assert!(db.get("t", "a").unwrap().is_none());
    assert!(db.table_names().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// AsyncVersionedKeyValueDB
// ---------------------------------------------------------------------------

#[cfg(all(feature = "async", feature = "versioned"))]
pub async fn test_async_versioned_db<D: keyvalue::AsyncVersionedKeyValueDB>(db: &D) {
    use keyvalue::VersionedObject;

    let (table1, key, value) = TEST_DATA[0];

    assert!(db.get(table1, key).await.unwrap().is_none());
    assert!(db.iter_from_prefix(table1, key).await.unwrap().is_empty());
    assert!(db.iter(table1).await.unwrap().is_empty());
    assert!(db.remove(table1, key).await.unwrap().is_none());
    assert!(!db.contains_key(table1, key).await.unwrap());
    assert!(db.table_names().await.unwrap().is_empty());
    assert!(db.keys(table1).await.unwrap().is_empty());
    assert!(db.values(table1).await.unwrap().is_empty());

    assert!(
        db.insert(table1, key, Some(value), 1)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.get(table1, key).await.unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 1,
        })
    );
    assert_eq!(
        db.insert(table1, key, Some(value), 2).await.unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 1,
        })
    );
    assert_eq!(
        db.get(table1, key).await.unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 2,
        })
    );
    assert!(db.update(table1, key, None).await.unwrap().is_some());
    assert_eq!(
        db.get(table1, key).await.unwrap(),
        Some(VersionedObject {
            value: None,
            version: 3,
        })
    );

    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];
    assert_eq!(db.get(table1, key1).await.unwrap(), None);
    assert_eq!(db.get(table1, key2).await.unwrap(), None);
    assert!(
        db.update(table1, key1, Some(value1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.insert(table1, key2, Some(value2), 100)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.update(table1, key2, Some(value2))
            .await
            .unwrap()
            .is_some()
    );
    let iter = db.iter_from_prefix(table1, prefix).await.unwrap();
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(
        key1.to_string(),
        VersionedObject {
            value: Some(value1.to_vec()),
            version: 1,
        }
    )));
    assert!(iter.contains(&(
        key2.to_string(),
        VersionedObject {
            value: Some(value2.to_vec()),
            version: 101,
        }
    )));
    let iter = db.iter(table1).await.unwrap();
    assert_eq!(iter.len(), 3);

    let keys = db.keys(table1).await.unwrap();
    assert_eq!(keys.len(), 3);
    let values = db.values(table1).await.unwrap();
    assert_eq!(values.len(), 3);

    assert!(db.contains_key(table1, key).await.unwrap());
    assert!(db.contains_key(table1, key1).await.unwrap());
    assert!(db.contains_key(table1, key2).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert_eq!(db.table_names().await.unwrap(), vec![table1.to_string()]);

    let (table2, key, value) = TEST_DATA[3];
    assert!(
        db.insert(table2, key, Some(value), 1)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.insert(table2, key, Some(value), 2).await.unwrap(),
        Some(VersionedObject {
            value: Some(value.to_vec()),
            version: 1,
        })
    );

    assert!(db.delete_table(table1, true).await.is_ok());
    assert!(db.get(table1, key1).await.unwrap().is_none());
    assert!(db.iter(table1).await.unwrap().is_empty());
    assert_eq!(db.table_names().await.unwrap(), vec![table2.to_string()]);

    assert!(db.clear(true).await.is_ok());
    assert!(db.table_names().await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// TransactionalKVDB (sync)
// ---------------------------------------------------------------------------

#[cfg(feature = "transactional")]
pub fn test_transactional_db<D: keyvalue::TransactionalKVDB>(db: &D) {
    use keyvalue::{KVReadTransaction, KVWriteTransaction};

    let (table1, key, value) = TEST_DATA[0];

    // empty read
    {
        let read = db.begin_read().unwrap();
        assert!(read.get(table1, key).unwrap().is_none());
        assert!(read.iter_from_prefix(table1, key).unwrap().is_empty());
        assert!(read.iter(table1).unwrap().is_empty());
        assert!(!read.contains_key(table1, key).unwrap());
        assert!(read.table_names().unwrap().is_empty());
        assert!(read.keys(table1).unwrap().is_empty());
        assert!(read.values(table1).unwrap().is_empty());
    }

    // empty write + commit
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

    // still empty
    {
        let read = db.begin_read().unwrap();
        assert!(read.get(table1, key).unwrap().is_none());
        assert!(read.table_names().unwrap().is_empty());
    }

    // insert and commit
    let mut write = db.begin_write().unwrap();
    assert!(write.insert(table1, key, value).unwrap().is_none());
    assert_eq!(write.get(table1, key).unwrap(), Some(value.to_vec()));
    assert_eq!(
        write.insert(table1, key, value).unwrap(),
        Some(value.to_vec())
    );
    assert_eq!(write.get(table1, key).unwrap(), Some(value.to_vec()));
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert_eq!(read.get(table1, key).unwrap(), Some(value.to_vec()));
    }

    // overwrite with empty value
    let mut write = db.begin_write().unwrap();
    assert!(write.insert(table1, key, &[]).unwrap().is_some());
    assert_eq!(write.get(table1, key).unwrap(), Some(vec![]));
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert_eq!(read.get(table1, key).unwrap(), Some(vec![]));
    }

    // remove
    let mut write = db.begin_write().unwrap();
    assert!(write.remove(table1, key).unwrap().is_some());
    assert!(write.get(table1, key).unwrap().is_none());
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert!(read.get(table1, key).unwrap().is_none());
    }

    // multi-key + prefix iteration
    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    let mut write = db.begin_write().unwrap();
    assert_eq!(write.get(table1, key1).unwrap(), None);
    assert_eq!(write.get(table1, key2).unwrap(), None);
    assert!(write.insert(table1, key1, value1).unwrap().is_none());
    assert!(write.insert(table1, key2, value2).unwrap().is_none());
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        let iter = read.iter_from_prefix(table1, prefix).unwrap();
        assert_eq!(iter.len(), 2);
        assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
        assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

        let iter = read.iter(table1).unwrap();
        assert_eq!(iter.len(), 2);

        let keys = read.keys(table1).unwrap();
        assert_eq!(keys.len(), 2);

        let values = read.values(table1).unwrap();
        assert_eq!(values.len(), 2);

        assert!(read.contains_key(table1, key1).unwrap());
        assert!(read.contains_key(table1, key2).unwrap());
        assert!(!read.contains_key(table1, "non-existent").unwrap());
        assert_eq!(read.table_names().unwrap(), vec![table1.to_string()]);
    }

    // second table
    let (table2, key, value) = TEST_DATA[3];

    let mut write = db.begin_write().unwrap();
    assert!(write.insert(table2, key, value).unwrap().is_none());
    assert_eq!(write.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(
        write.insert(table2, key, value).unwrap(),
        Some(value.to_vec())
    );
    write.commit().unwrap();

    // delete_table within write tx
    let mut write = db.begin_write().unwrap();
    assert!(write.delete_table(table1).is_ok());
    assert!(write.get(table1, key1).unwrap().is_none());
    assert!(write.get(table1, key2).unwrap().is_none());
    assert!(write.iter_from_prefix(table1, prefix).unwrap().is_empty());
    assert!(write.iter(table1).unwrap().is_empty());
    assert!(write.keys(table1).unwrap().is_empty());
    assert!(write.values(table1).unwrap().is_empty());
    assert!(!write.contains_key(table1, key1).unwrap());
    assert_eq!(write.get(table2, key).unwrap(), Some(value.to_vec()));
    assert_eq!(write.table_names().unwrap(), vec![table2.to_string()]);
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert!(read.get(table1, key1).unwrap().is_none());
        assert_eq!(read.get(table2, key).unwrap(), Some(value.to_vec()));
        assert_eq!(read.table_names().unwrap(), vec![table2.to_string()]);
    }

    // clear within write tx
    let mut write = db.begin_write().unwrap();
    assert!(write.clear().is_ok());
    assert!(write.get(table2, key).unwrap().is_none());
    assert!(write.table_names().unwrap().is_empty());
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert!(read.table_names().unwrap().is_empty());
    }

    // abort discards changes
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

/// Tests RYOW (read-your-own-writes) within a write transaction.
#[cfg(feature = "transactional")]
pub fn test_transactional_ryow<D: keyvalue::TransactionalKVDB>(db: &D) {
    use keyvalue::{KVReadTransaction, KVWriteTransaction};

    // Write, read back before commit, then commit and verify via read tx
    let mut write = db.begin_write().unwrap();
    write.insert("ryow", "k1", b"v1").unwrap();
    write.insert("ryow", "k2", b"v2").unwrap();

    // within the same write tx we should see the data
    assert_eq!(write.get("ryow", "k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(write.get("ryow", "k2").unwrap(), Some(b"v2".to_vec()));
    assert!(write.contains_key("ryow", "k1").unwrap());
    assert!(write.contains_table("ryow").unwrap());

    let keys = write.keys("ryow").unwrap();
    assert_eq!(keys.len(), 2);

    // remove within tx
    write.remove("ryow", "k2").unwrap();
    assert!(write.get("ryow", "k2").unwrap().is_none());
    assert_eq!(write.keys("ryow").unwrap().len(), 1);

    write.commit().unwrap();

    // verify
    let read = db.begin_read().unwrap();
    assert_eq!(read.get("ryow", "k1").unwrap(), Some(b"v1".to_vec()));
    assert!(read.get("ryow", "k2").unwrap().is_none());

    // clean up
    let mut w = db.begin_write().unwrap();
    w.clear().unwrap();
    w.commit().unwrap();
}

// ---------------------------------------------------------------------------
// AsyncTransactionalKVDB
// ---------------------------------------------------------------------------

#[cfg(all(feature = "async", feature = "transactional"))]
pub async fn test_async_transactional_db<D: keyvalue::AsyncTransactionalKVDB>(db: &D) {
    use keyvalue::{AsyncKVReadTransaction, AsyncKVWriteTransaction};

    let (table1, key, value) = TEST_DATA[0];

    // empty read
    {
        let read = db.begin_read().await.unwrap();
        assert!(read.get(table1, key).await.unwrap().is_none());
        assert!(read.iter_from_prefix(table1, key).await.unwrap().is_empty());
        assert!(read.iter(table1).await.unwrap().is_empty());
        assert!(!read.contains_key(table1, key).await.unwrap());
        assert!(read.table_names().await.unwrap().is_empty());
        assert!(read.keys(table1).await.unwrap().is_empty());
        assert!(read.values(table1).await.unwrap().is_empty());
    }

    // empty write
    let mut write = db.begin_write().await.unwrap();
    assert!(write.get(table1, key).await.unwrap().is_none());
    assert!(
        write
            .iter_from_prefix(table1, key)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(write.iter(table1).await.unwrap().is_empty());
    assert!(write.remove(table1, key).await.unwrap().is_none());
    assert!(!write.contains_key(table1, key).await.unwrap());
    assert!(write.table_names().await.unwrap().is_empty());
    assert!(write.keys(table1).await.unwrap().is_empty());
    assert!(write.values(table1).await.unwrap().is_empty());
    assert!(write.delete_table(table1).await.is_ok());
    assert!(write.clear().await.is_ok());
    write.commit().await.unwrap();

    // still empty
    {
        let read = db.begin_read().await.unwrap();
        assert!(read.get(table1, key).await.unwrap().is_none());
        assert!(read.table_names().await.unwrap().is_empty());
    }

    // insert and commit
    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table1, key, value).await.unwrap().is_none());
    assert_eq!(write.get(table1, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(
        write.insert(table1, key, value).await.unwrap(),
        Some(value.to_vec())
    );
    assert_eq!(write.get(table1, key).await.unwrap(), Some(value.to_vec()));
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert_eq!(read.get(table1, key).await.unwrap(), Some(value.to_vec()));
    }

    // empty value
    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table1, key, &[]).await.unwrap().is_some());
    assert_eq!(write.get(table1, key).await.unwrap(), Some(vec![]));
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert_eq!(read.get(table1, key).await.unwrap(), Some(vec![]));
    }

    // remove
    let mut write = db.begin_write().await.unwrap();
    assert!(write.remove(table1, key).await.unwrap().is_some());
    assert!(write.get(table1, key).await.unwrap().is_none());
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert!(read.get(table1, key).await.unwrap().is_none());
    }

    // multi-key
    let prefix = TEST_PREFIX;
    let (_, key1, value1) = TEST_DATA[1];
    let (table1, key2, value2) = TEST_DATA[2];

    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table1, key1, value1).await.unwrap().is_none());
    assert!(write.insert(table1, key2, value2).await.unwrap().is_none());
    assert!(write.insert(table1, key1, value1).await.unwrap().is_some());
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        let iter = read.iter_from_prefix(table1, prefix).await.unwrap();
        assert_eq!(iter.len(), 2);
        assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
        assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

        let iter = read.iter(table1).await.unwrap();
        assert_eq!(iter.len(), 2);

        assert!(read.contains_key(table1, key1).await.unwrap());
        assert!(read.contains_key(table1, key2).await.unwrap());
        assert!(!read.contains_key(table1, "non-existent").await.unwrap());
        assert_eq!(read.table_names().await.unwrap(), vec![table1.to_string()]);
    }

    // second table
    let (table2, key, value) = TEST_DATA[3];

    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table2, key, value).await.unwrap().is_none());
    assert_eq!(write.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(
        write.insert(table2, key, value).await.unwrap(),
        Some(value.to_vec())
    );
    write.commit().await.unwrap();

    // delete_table
    let mut write = db.begin_write().await.unwrap();
    write.delete_table(table1).await.unwrap();
    assert!(write.get(table1, key1).await.unwrap().is_none());
    assert!(write.get(table1, key2).await.unwrap().is_none());
    assert!(
        write
            .iter_from_prefix(table1, prefix)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(write.iter(table1).await.unwrap().is_empty());
    assert_eq!(write.get(table2, key).await.unwrap(), Some(value.to_vec()));
    assert_eq!(write.table_names().await.unwrap(), vec![table2.to_string()]);
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert!(read.get(table1, key1).await.unwrap().is_none());
        assert_eq!(read.get(table2, key).await.unwrap(), Some(value.to_vec()));
        assert_eq!(read.table_names().await.unwrap(), vec![table2.to_string()]);
    }

    // clear
    let mut write = db.begin_write().await.unwrap();
    write.clear().await.unwrap();
    assert!(write.get(table2, key).await.unwrap().is_none());
    assert!(write.table_names().await.unwrap().is_empty());
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert!(read.table_names().await.unwrap().is_empty());
    }

    // abort
    let mut write = db.begin_write().await.unwrap();
    assert!(write.insert(table1, key1, value1).await.unwrap().is_none());
    assert!(write.insert(table1, key2, value2).await.unwrap().is_none());
    assert!(write.insert(table2, key, value).await.unwrap().is_none());
    write.abort().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert!(read.get(table1, key1).await.unwrap().is_none());
    assert!(read.get(table1, key2).await.unwrap().is_none());
    assert!(read.get(table2, key).await.unwrap().is_none());
    assert!(
        read.iter_from_prefix(table1, prefix)
            .await
            .unwrap()
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// Versioned Transactional (sync)
// ---------------------------------------------------------------------------

#[cfg(all(feature = "versioned", feature = "transactional"))]
pub fn test_versioned_transactional_db<D: keyvalue::VersionedTransactionalKVDB>(db: &D) {
    use keyvalue::{KVReadVersionedTransaction, KVWriteVersionedTransaction, VersionedObject};

    // empty
    {
        let read = db.begin_read().unwrap();
        assert!(read.get("t", "k").unwrap().is_none());
        assert!(read.iter("t").unwrap().is_empty());
        assert!(read.table_names().unwrap().is_empty());
    }

    // insert with version, commit, verify via read
    let mut write = db.begin_write().unwrap();
    write.insert("t", "k1", Some(b"hello"), 1).unwrap();
    assert_eq!(
        write.get("t", "k1").unwrap(),
        Some(VersionedObject {
            value: Some(b"hello".to_vec()),
            version: 1,
        })
    );
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.get("t", "k1").unwrap(),
            Some(VersionedObject {
                value: Some(b"hello".to_vec()),
                version: 1,
            })
        );
    }

    // update auto-increments version
    let mut write = db.begin_write().unwrap();
    write.update("t", "k1", Some(b"world")).unwrap();
    assert_eq!(
        write.get("t", "k1").unwrap(),
        Some(VersionedObject {
            value: Some(b"world".to_vec()),
            version: 2,
        })
    );
    write.commit().unwrap();

    // tombstone via update
    let mut write = db.begin_write().unwrap();
    write.update("t", "k1", None).unwrap();
    assert_eq!(
        write.get("t", "k1").unwrap(),
        Some(VersionedObject {
            value: None,
            version: 3,
        })
    );
    write.commit().unwrap();

    // permanent remove
    let mut write = db.begin_write().unwrap();
    let old = write.remove("t", "k1").unwrap();
    assert!(old.is_some());
    assert!(write.get("t", "k1").unwrap().is_none());
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert!(read.get("t", "k1").unwrap().is_none());
    }

    // soft-delete table (prune=false)
    let mut write = db.begin_write().unwrap();
    write.insert("t", "a", Some(b"1"), 10).unwrap();
    write.insert("t", "b", Some(b"2"), 10).unwrap();
    write.commit().unwrap();

    let mut write = db.begin_write().unwrap();
    write.delete_table("t", false).unwrap();
    // entries should be tombstones with incremented version
    let a = write.get("t", "a").unwrap().unwrap();
    assert_eq!(a.value, None);
    assert_eq!(a.version, 11);
    write.commit().unwrap();

    // prune clear
    let mut write = db.begin_write().unwrap();
    write.clear(true).unwrap();
    write.commit().unwrap();

    {
        let read = db.begin_read().unwrap();
        assert!(read.table_names().unwrap().is_empty());
    }

    // abort
    let mut write = db.begin_write().unwrap();
    write.insert("t", "x", Some(b"abc"), 1).unwrap();
    write.abort().unwrap();

    let read = db.begin_read().unwrap();
    assert!(read.get("t", "x").unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Async Versioned Transactional
// ---------------------------------------------------------------------------

#[cfg(all(feature = "async", feature = "versioned", feature = "transactional"))]
pub async fn test_async_versioned_transactional_db<D: keyvalue::AsyncVersionedTransactionalKVDB>(
    db: &D,
) {
    use keyvalue::{
        AsyncKVReadVersionedTransaction, AsyncKVWriteVersionedTransaction, VersionedObject,
    };

    // empty
    {
        let read = db.begin_read().await.unwrap();
        assert!(read.get("t", "k").await.unwrap().is_none());
        assert!(read.iter("t").await.unwrap().is_empty());
        assert!(read.table_names().await.unwrap().is_empty());
    }

    // insert, commit, verify
    let mut write = db.begin_write().await.unwrap();
    write.insert("t", "k1", Some(b"hello"), 1).await.unwrap();
    assert_eq!(
        write.get("t", "k1").await.unwrap(),
        Some(VersionedObject {
            value: Some(b"hello".to_vec()),
            version: 1,
        })
    );
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert_eq!(
            read.get("t", "k1").await.unwrap(),
            Some(VersionedObject {
                value: Some(b"hello".to_vec()),
                version: 1,
            })
        );
    }

    // update auto-increments version
    let mut write = db.begin_write().await.unwrap();
    write.update("t", "k1", Some(b"world")).await.unwrap();
    assert_eq!(
        write.get("t", "k1").await.unwrap(),
        Some(VersionedObject {
            value: Some(b"world".to_vec()),
            version: 2,
        })
    );
    write.commit().await.unwrap();

    // tombstone
    let mut write = db.begin_write().await.unwrap();
    write.update("t", "k1", None).await.unwrap();
    assert_eq!(
        write.get("t", "k1").await.unwrap(),
        Some(VersionedObject {
            value: None,
            version: 3,
        })
    );
    write.commit().await.unwrap();

    // remove permanently
    let mut write = db.begin_write().await.unwrap();
    let old = write.remove("t", "k1").await.unwrap();
    assert!(old.is_some());
    assert!(write.get("t", "k1").await.unwrap().is_none());
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert!(read.get("t", "k1").await.unwrap().is_none());
    }

    // soft-delete
    let mut write = db.begin_write().await.unwrap();
    write.insert("t", "a", Some(b"1"), 10).await.unwrap();
    write.insert("t", "b", Some(b"2"), 10).await.unwrap();
    write.commit().await.unwrap();

    let mut write = db.begin_write().await.unwrap();
    write.delete_table("t", false).await.unwrap();
    let a = write.get("t", "a").await.unwrap().unwrap();
    assert_eq!(a.value, None);
    assert_eq!(a.version, 11);
    write.commit().await.unwrap();

    // prune clear
    let mut write = db.begin_write().await.unwrap();
    write.clear(true).await.unwrap();
    write.commit().await.unwrap();

    {
        let read = db.begin_read().await.unwrap();
        assert!(read.table_names().await.unwrap().is_empty());
    }

    // abort
    let mut write = db.begin_write().await.unwrap();
    write.insert("t", "x", Some(b"abc"), 1).await.unwrap();
    write.abort().await.unwrap();

    let read = db.begin_read().await.unwrap();
    assert!(read.get("t", "x").await.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Persistence helpers
// ---------------------------------------------------------------------------

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
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let (table2, key, value) = TEST_DATA[3];
    let iter = db.iter_from_prefix(table2, "").unwrap();
    assert_eq!(iter.len(), 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let iter = db.iter(table1).unwrap();
    assert_eq!(iter.len(), 3);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));
    assert!(iter.contains(&(key3.to_string(), value3.to_vec())));

    let iter = db.iter(table2).unwrap();
    assert_eq!(iter.len(), 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let keys = db.keys(table1).unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&key1.to_string()));
    assert!(keys.contains(&key2.to_string()));
    assert!(keys.contains(&key3.to_string()));

    let keys = db.keys(table2).unwrap();
    assert_eq!(keys.len(), 1);
    assert!(keys.contains(&key.to_string()));

    let values = db.values(table1).unwrap();
    assert_eq!(values.len(), 3);

    let values = db.values(table2).unwrap();
    assert_eq!(values.len(), 1);

    assert!(db.contains_key(table1, key1).unwrap());
    assert!(db.contains_key(table1, key2).unwrap());
    assert!(db.contains_key(table1, key3).unwrap());
    assert!(!db.contains_key(table1, "non-existent").unwrap());
    assert!(db.contains_key(table2, key).unwrap());
    assert!(!db.contains_key(table2, "non-existent").unwrap());

    let table_names = db.table_names().unwrap();
    assert_eq!(table_names.len(), 2);
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
    assert_eq!(iter.len(), 2);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));

    let (table2, key, value) = TEST_DATA[3];
    let iter = db.iter_from_prefix(table2, "").await.unwrap();
    assert_eq!(iter.len(), 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let iter = db.iter(table1).await.unwrap();
    assert_eq!(iter.len(), 3);
    assert!(iter.contains(&(key1.to_string(), value1.to_vec())));
    assert!(iter.contains(&(key2.to_string(), value2.to_vec())));
    assert!(iter.contains(&(key3.to_string(), value3.to_vec())));

    let iter = db.iter(table2).await.unwrap();
    assert_eq!(iter.len(), 1);
    assert!(iter.contains(&(key.to_string(), value.to_vec())));

    let keys = db.keys(table1).await.unwrap();
    assert_eq!(keys.len(), 3);

    let keys = db.keys(table2).await.unwrap();
    assert_eq!(keys.len(), 1);

    assert!(db.contains_key(table1, key1).await.unwrap());
    assert!(db.contains_key(table1, key2).await.unwrap());
    assert!(db.contains_key(table1, key3).await.unwrap());
    assert!(!db.contains_key(table1, "non-existent").await.unwrap());
    assert!(db.contains_key(table2, key).await.unwrap());
    assert!(!db.contains_key(table2, "non-existent").await.unwrap());

    let table_names = db.table_names().await.unwrap();
    assert_eq!(table_names.len(), 2);
    assert!(table_names.contains(&table1.to_string()));
    assert!(table_names.contains(&table2.to_string()));
}
