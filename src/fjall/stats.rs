//! Runtime statistics of a [`FjallDB`], see [`FjallDB::stats`].
//!
//! The figures come from fjall's internal accessors. Collecting them never
//! holds two of fjall's locks at once: the keyspace map's guard is dropped
//! before any keyspace is inspected, and each tree accessor takes and
//! releases its own lock.

#[cfg(feature = "transactional")]
use std::{
    backtrace::Backtrace,
    collections::BTreeMap,
    sync::{
        LazyLock, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use std::{collections::HashSet, io, path::PathBuf, sync::Arc, time::Duration};

use fjall::{AbstractTree, Keyspace};

use super::{FjallDB, lock_poisoned};

/// Database-wide statistics of a [`FjallDB`], see [`FjallDB::stats`].
#[derive(Debug, Clone)]
pub struct FjallStats {
    /// Directory the database is stored in.
    pub path: PathBuf,
    /// Transactions currently holding a handle to the database.
    pub live_transactions: usize,
    /// Snapshots currently open, those of live transactions included.
    pub open_snapshots: usize,
    /// Sequence numbers the snapshot tracker keeps an entry for. An entry
    /// whose snapshots have all closed stays until the tracker's next
    /// garbage collection.
    pub tracked_snapshot_seqnos: usize,
    /// Sequence number the next write is assigned.
    pub seqno: u64,
    /// Sequence number new snapshots are taken at; they see the writes
    /// numbered below it.
    pub visible_seqno: u64,
    /// Sequence number below which superseded data may be dropped. It stays
    /// below the sequence number of every open snapshot, and moves only when
    /// the snapshot tracker is garbage-collected: at a memtable rotation in
    /// any keyspace, and after every 10,000 closed snapshots.
    pub gc_watermark: u64,
    /// Approximate bytes in the active and sealed memtables of all
    /// keyspaces, by the estimate described at
    /// [`KeyspaceStats::active_memtable_bytes`].
    pub write_buffer_bytes: u64,
    /// Bytes held by the block cache.
    pub cache_bytes: u64,
    /// Capacity of the block cache, in bytes.
    pub cache_capacity_bytes: u64,
    /// Journal files, the active one included.
    pub journal_count: usize,
    /// Memtable flushes queued and not yet taken up by a worker.
    pub outstanding_flushes: usize,
    /// Compactions running now.
    pub active_compactions: usize,
    /// Compactions completed since the database was opened.
    pub compactions_completed: usize,
    /// Memtable size, in bytes, past which a keyspace's active memtable is
    /// rotated out, as configured on this handle. fjall persists the size a
    /// keyspace was created with, so keyspaces created under another
    /// configuration keep theirs.
    pub max_memtable_bytes: u64,
    /// Sequence number the snapshot of the oldest live transaction reads
    /// at: the live transaction reading at the lowest sequence number, the
    /// first opened among several at that number. The GC watermark stays
    /// below it for as long as that transaction is open.
    pub oldest_transaction_seqno: Option<u64>,
    /// How long the oldest live transaction has been open.
    pub oldest_transaction_age: Option<Duration>,
    /// Where the oldest live transaction was opened. Captured only when the
    /// environment variable `KEYVALUE_FJALL_TX_BACKTRACE` is `1` as the
    /// first transaction opens, since every transaction then pays for a
    /// backtrace.
    pub oldest_transaction_backtrace: Option<String>,
    /// Statistics of each keyspace, ordered by name.
    pub keyspaces: Vec<KeyspaceStats>,
}

/// Statistics of one keyspace, see [`FjallStats::keyspaces`].
#[derive(Debug, Clone)]
pub struct KeyspaceStats {
    /// Keyspace name.
    pub name: String,
    /// Whether the keyspace is soft-deleted: emptied and recorded as a
    /// deleted table.
    pub deleted: bool,
    /// Approximate bytes in the active memtable: fjall's estimate, which
    /// counts each entry's key and value lengths plus a fixed per-entry
    /// overhead, not the heap bytes allocated.
    pub active_memtable_bytes: u64,
    /// Entries in the active memtable, every version and tombstone counted.
    pub active_memtable_items: usize,
    /// Sealed memtables in the current version, awaiting their flush.
    pub sealed_memtables: usize,
    /// Superseded versions of the keyspace's tree still held, each keeping
    /// the memtables and tables it references alive. A version is dropped
    /// once the GC watermark has passed the sequence number of the version
    /// after it, checked at every flush, compaction and memtable rotation,
    /// and a flush reads the watermark before it adds its own version. A
    /// few retained versions with no snapshot open are therefore normal
    /// until the next rotation; a count that keeps growing means the
    /// watermark is held back.
    pub retained_versions: usize,
    /// Tables (SST files) in the current version. Tables replaced by a
    /// compaction stay on disk while a retained version references them,
    /// and are not counted.
    pub tables: usize,
    /// Disjoint runs in level 0 of the current version.
    pub l0_runs: usize,
    /// Bytes on disk of the current version's tables and blob files.
    pub disk_bytes: u64,
    /// Bytes of filter blocks the current version's tables pin in memory.
    pub pinned_filter_bytes: usize,
    /// Bytes of index blocks the current version's tables pin in memory.
    pub pinned_index_bytes: usize,
    /// Highest sequence number in an active or sealed memtable.
    pub highest_memtable_seqno: Option<u64>,
    /// Highest sequence number flushed to a table.
    pub highest_persisted_seqno: Option<u64>,
}

/// Whether transactions record where they were opened: set by the
/// environment variable `KEYVALUE_FJALL_TX_BACKTRACE=1`, read once.
#[cfg(feature = "transactional")]
static CAPTURE_BACKTRACES: LazyLock<bool> = LazyLock::new(|| {
    std::env::var_os("KEYVALUE_FJALL_TX_BACKTRACE").is_some_and(|value| value == "1")
});

/// The live transactions of one [`FjallDB`], keyed by the sequence number
/// their snapshot reads at, then by the order they were registered in.
#[cfg(feature = "transactional")]
#[derive(Clone, Default)]
pub(super) struct TransactionRegistry(Arc<Mutex<BTreeMap<(u64, u64), OpenTransaction>>>);

/// When a live transaction was opened and, if captured, where.
#[cfg(feature = "transactional")]
struct OpenTransaction {
    opened_at: Instant,
    backtrace: Option<Arc<Backtrace>>,
}

/// The live transaction reading at the lowest sequence number.
#[cfg(feature = "transactional")]
struct OldestTransaction {
    seqno: u64,
    age: Duration,
    backtrace: Option<String>,
}

#[cfg(feature = "transactional")]
impl TransactionRegistry {
    /// Registers a transaction whose snapshot reads at `seqno`. It stays
    /// registered until the returned ticket is dropped.
    pub(super) fn register(&self, seqno: u64) -> TransactionTicket {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let key = (seqno, NEXT_ID.fetch_add(1, Ordering::Relaxed));
        let transaction = OpenTransaction {
            opened_at: Instant::now(),
            backtrace: CAPTURE_BACKTRACES.then(|| Arc::new(Backtrace::force_capture())),
        };
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, transaction);
        TransactionTicket {
            key,
            registry: self.clone(),
        }
    }

    /// The live transaction reading at the lowest sequence number, and so
    /// holding the GC watermark back the furthest; the first registered
    /// among several at that number.
    fn oldest(&self) -> Option<OldestTransaction> {
        let (seqno, opened_at, backtrace) = {
            let open = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            let (&(seqno, _), transaction) = open.first_key_value()?;
            (seqno, transaction.opened_at, transaction.backtrace.clone())
        };
        // Formatted after the lock is released: resolving a backtrace's
        // symbols can take a while.
        Some(OldestTransaction {
            seqno,
            age: opened_at.elapsed(),
            backtrace: backtrace.map(|backtrace| backtrace.to_string()),
        })
    }
}

/// Keeps one transaction in its [`TransactionRegistry`] until dropped.
#[cfg(feature = "transactional")]
pub(super) struct TransactionTicket {
    key: (u64, u64),
    registry: TransactionRegistry,
}

#[cfg(feature = "transactional")]
impl Drop for TransactionTicket {
    fn drop(&mut self) {
        self.registry
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.key);
    }
}

/// Collects the statistics [`FjallDB::stats`] returns.
pub(super) fn collect(db: &FjallDB) -> io::Result<FjallStats> {
    // Copied out before the database guard is taken, so the two locks are
    // never held together.
    let deleted_tables = db
        .deleted_tables
        .read()
        .map_err(|_| lock_poisoned())?
        .clone();

    let stats = {
        let guard = db.inner()?;
        // Every live transaction holds one clone of the handle: the same
        // count `try_recover_from_poison` refuses on.
        let live_transactions = guard
            .0
            .as_ref()
            .map_or(0, |handle| Arc::strong_count(handle) - 1);
        let database = guard.inner();
        let snapshot_tracker = &database.supervisor.snapshot_tracker;

        // The map's guard drops at the end of this statement, before any
        // keyspace is inspected: every keyspace lookup takes this lock for
        // writing, and a second read of it queued behind such a writer would
        // never be granted.
        let keyspaces: Vec<Keyspace> = database
            .supervisor
            .keyspaces
            .read()
            .map_err(|_| lock_poisoned())?
            .values()
            .cloned()
            .collect();

        let mut keyspace_stats: Vec<KeyspaceStats> = keyspaces
            .iter()
            .map(|keyspace| collect_keyspace(keyspace, &deleted_tables))
            .collect();
        keyspace_stats.sort_by(|a, b| a.name.cmp(&b.name));

        FjallStats {
            path: db.path.to_path_buf(),
            live_transactions,
            open_snapshots: snapshot_tracker.open_snapshots(),
            tracked_snapshot_seqnos: snapshot_tracker.len(),
            seqno: database.seqno(),
            visible_seqno: database.visible_seqno(),
            gc_watermark: snapshot_tracker.get_seqno_safe_to_gc(),
            write_buffer_bytes: database.write_buffer_size(),
            cache_bytes: database.cache_size(),
            cache_capacity_bytes: database.cache_capacity(),
            journal_count: database.journal_count(),
            outstanding_flushes: database.outstanding_flushes(),
            active_compactions: database.active_compactions(),
            compactions_completed: database.compactions_completed(),
            max_memtable_bytes: db.max_memtable_size,
            oldest_transaction_seqno: None,
            oldest_transaction_age: None,
            oldest_transaction_backtrace: None,
            keyspaces: keyspace_stats,
        }
    };

    // Read once the database guard is released, as formatting a captured
    // backtrace can take a while.
    #[cfg(feature = "transactional")]
    let stats = match db.transactions.oldest() {
        Some(oldest) => FjallStats {
            oldest_transaction_seqno: Some(oldest.seqno),
            oldest_transaction_age: Some(oldest.age),
            oldest_transaction_backtrace: oldest.backtrace,
            ..stats
        },
        None => stats,
    };

    Ok(stats)
}

/// Collects the statistics of one keyspace.
fn collect_keyspace(keyspace: &Keyspace, deleted_tables: &HashSet<String>) -> KeyspaceStats {
    let tree = &keyspace.tree;
    let active_memtable = tree.active_memtable();
    KeyspaceStats {
        name: keyspace.name().to_string(),
        deleted: deleted_tables.contains(keyspace.name().as_ref()),
        active_memtable_bytes: active_memtable.size(),
        active_memtable_items: active_memtable.len(),
        sealed_memtables: tree.sealed_memtable_count(),
        retained_versions: tree.version_free_list_len(),
        tables: tree.table_count(),
        l0_runs: tree.l0_run_count(),
        disk_bytes: tree.disk_space(),
        pinned_filter_bytes: tree.pinned_filter_size(),
        pinned_index_bytes: tree.pinned_block_index_size(),
        highest_memtable_seqno: tree.get_highest_memtable_seqno(),
        highest_persisted_seqno: tree.get_highest_persisted_seqno(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KeyValueDB;
    use crate::fjall::{FjallConfig, META_DELETED_KEYSPACE};

    fn open_temporary() -> (tempfile::TempDir, FjallDB) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = FjallDB::open(dir.path()).expect("open");
        (dir, db)
    }

    fn keyspace<'a>(stats: &'a FjallStats, name: &str) -> &'a KeyspaceStats {
        stats
            .keyspaces
            .iter()
            .find(|keyspace| keyspace.name == name)
            .unwrap_or_else(|| panic!("keyspace {name} is not listed"))
    }

    /// The snapshot tracker's entries, its open snapshots and the keyspace
    /// count, read straight from fjall.
    fn fjall_state(db: &FjallDB) -> (usize, usize, usize) {
        let guard = db.inner().expect("inner");
        let snapshot_tracker = &guard.inner().supervisor.snapshot_tracker;
        (
            snapshot_tracker.len(),
            snapshot_tracker.open_snapshots(),
            guard.keyspace_count(),
        )
    }

    #[test]
    fn fresh_database_reports_no_keyspace_snapshot_or_transaction() {
        let (dir, db) = open_temporary();

        let stats = db.stats().expect("stats");

        assert_eq!(stats.path, dir.path());
        assert_eq!(stats.live_transactions, 0);
        assert_eq!(stats.open_snapshots, 0);
        assert_eq!(stats.oldest_transaction_seqno, None);
        assert_eq!(stats.oldest_transaction_age, None);
        assert_eq!(stats.oldest_transaction_backtrace, None);
        assert_eq!(stats.write_buffer_bytes, 0);
        assert_eq!(
            stats.max_memtable_bytes,
            FjallConfig::default().max_memtable_size
        );
        assert!(stats.journal_count >= 1);
        assert!(stats.keyspaces.is_empty());
    }

    #[test]
    fn written_keyspace_reports_its_active_memtable() {
        let (_dir, db) = open_temporary();
        for i in 0..10 {
            db.insert("table", &format!("key{i}"), b"value")
                .expect("insert");
        }

        let stats = db.stats().expect("stats");
        let table = keyspace(&stats, "table");
        assert!(!table.deleted);
        assert_eq!(table.active_memtable_items, 10);
        assert!(table.active_memtable_bytes > 0);
        assert!(stats.write_buffer_bytes >= table.active_memtable_bytes);
        assert!(table.highest_memtable_seqno.is_some());
        assert_eq!(table.tables, 0);
        assert_eq!(table.retained_versions, 0);

        // A soft-deleted table keeps its keyspace, reported as deleted.
        db.delete_table("table").expect("delete_table");
        let stats = db.stats().expect("stats");
        assert!(keyspace(&stats, "table").deleted);
        assert!(!keyspace(&stats, META_DELETED_KEYSPACE).deleted);
    }

    #[cfg(feature = "transactional")]
    #[test]
    fn open_transactions_are_reported_until_dropped() {
        use crate::TransactionalKVDB;

        let (_dir, db) = open_temporary();
        db.insert("table", "key", b"value").expect("insert");

        let snapshot_seqno = db.stats().expect("stats").visible_seqno;
        let before_open = Instant::now();
        let read = TransactionalKVDB::begin_read(&db).expect("begin_read");
        let write = TransactionalKVDB::begin_write(&db).expect("begin_write");

        let stats = db.stats().expect("stats");
        assert_eq!(stats.live_transactions, 2);
        assert_eq!(stats.open_snapshots, 2);
        assert_eq!(stats.oldest_transaction_seqno, Some(snapshot_seqno));
        let age = stats
            .oldest_transaction_age
            .expect("two transactions are open");
        assert!(age <= before_open.elapsed());
        // The read transaction was registered first at the shared sequence
        // number, so its backtrace is the one reported.
        assert_eq!(
            stats.oldest_transaction_backtrace.is_some(),
            *CAPTURE_BACKTRACES
        );
        if let Some(backtrace) = &stats.oldest_transaction_backtrace {
            assert!(backtrace.contains("begin_read"), "{backtrace}");
        }

        drop(read);
        let stats = db.stats().expect("stats");
        assert_eq!(stats.live_transactions, 1);
        assert_eq!(stats.open_snapshots, 1);
        assert_eq!(stats.oldest_transaction_seqno, Some(snapshot_seqno));
        if let Some(backtrace) = &stats.oldest_transaction_backtrace {
            assert!(backtrace.contains("begin_write"), "{backtrace}");
        }

        drop(write);
        let stats = db.stats().expect("stats");
        assert_eq!(stats.live_transactions, 0);
        assert_eq!(stats.open_snapshots, 0);
        assert_eq!(stats.oldest_transaction_seqno, None);
        assert_eq!(stats.oldest_transaction_age, None);
        assert_eq!(stats.oldest_transaction_backtrace, None);
    }

    #[test]
    fn collecting_stats_opens_no_snapshot_and_creates_no_keyspace() {
        let (_dir, db) = open_temporary();

        // A snapshot opened even once leaves a tracker entry behind until
        // the tracker is garbage-collected, which nothing here triggers.
        assert_eq!(fjall_state(&db), (0, 0, 0));
        for _ in 0..3 {
            db.stats().expect("stats");
        }
        assert_eq!(fjall_state(&db), (0, 0, 0));

        // A snapshot taken now would read at a sequence number the tracker
        // has no entry for yet.
        db.insert("table", "key", b"value").expect("insert");
        let before = fjall_state(&db);
        assert_eq!(before.2, 1);
        for _ in 0..3 {
            let stats = db.stats().expect("stats");
            assert_eq!(stats.keyspaces.len(), 1);
        }
        assert_eq!(fjall_state(&db), before);
    }

    #[cfg(feature = "transactional")]
    #[test]
    fn pinned_snapshot_holds_versions_until_released() {
        use crate::TransactionalKVDB;
        use fjall::KeyspaceCreateOptions;

        let (_dir, db) = open_temporary();
        // FIFO compaction under an unreachable size limit never rewrites a
        // table, so only the flushes below add versions. Each flush of a
        // keyspace holds keys beyond the previous one's, which FIFO needs.
        let (pinned, other) = {
            let guard = db.inner().expect("inner");
            let no_compaction = || {
                KeyspaceCreateOptions::default()
                    .compaction_strategy(Arc::new(fjall::compaction::Fifo::new(u64::MAX, None)))
            };
            (
                guard.keyspace("pinned", no_compaction).expect("keyspace"),
                guard.keyspace("other", no_compaction).expect("keyspace"),
            )
        };
        db.insert("pinned", "k0", b"value").expect("insert");
        db.insert("other", "k0", b"value").expect("insert");

        let pin_seqno = db.stats().expect("stats").visible_seqno;
        let pin = TransactionalKVDB::begin_read(&db).expect("begin_read");
        db.insert("pinned", "k1", b"value").expect("insert");
        pinned.inner().rotate_memtable_and_wait().expect("flush");

        let stats = db.stats().expect("stats");
        let retained = keyspace(&stats, "pinned").retained_versions;
        assert!(retained > 0);
        assert_eq!(keyspace(&stats, "pinned").tables, 1);
        assert_eq!(stats.oldest_transaction_seqno, Some(pin_seqno));
        assert_eq!(stats.gc_watermark, pin_seqno - 1);

        // While the pin is held, a rotation elsewhere releases nothing.
        db.insert("other", "k1", b"value").expect("insert");
        other.inner().rotate_memtable_and_wait().expect("flush");
        let stats = db.stats().expect("stats");
        assert_eq!(keyspace(&stats, "pinned").retained_versions, retained);
        assert_eq!(stats.gc_watermark, pin_seqno - 1);

        // Once it is dropped, the next rotation anywhere releases them.
        drop(pin);
        db.insert("other", "k2", b"value").expect("insert");
        other.inner().rotate_memtable_and_wait().expect("flush");
        let stats = db.stats().expect("stats");
        assert_eq!(keyspace(&stats, "pinned").retained_versions, 0);
        assert_eq!(stats.oldest_transaction_seqno, None);
        assert!(stats.gc_watermark >= pin_seqno);
    }
}
