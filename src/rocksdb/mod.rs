//! [RocksDB](https://docs.rs/rust-rocksdb)-backed key-value store.
//!
//! Tables are mapped to RocksDB **column families**. The `default` column
//! family is reserved by RocksDB itself and cannot be used as a table name.

use std::{
    io,
    path::Path,
    sync::{Arc, RwLock},
};

use rust_rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompactionStyle, DBCompressionType,
    DBRecoveryMode, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
    WriteOptions,
};

use crate::KeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

/// Reserved column family name that RocksDB creates automatically.
const DEFAULT_CF: &str = "default";

/// Multi-threaded RocksDB column-family type alias.
type Rocks = DBWithThreadMode<MultiThreaded>;

/// Compression algorithm for RocksDB data blocks.
///
/// Re-exported from `rust-rocksdb` for convenience so callers do not need to
/// depend on the upstream crate directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Snappy,
    Zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
}

impl From<Compression> for DBCompressionType {
    fn from(c: Compression) -> Self {
        match c {
            Compression::None => DBCompressionType::None,
            Compression::Snappy => DBCompressionType::Snappy,
            Compression::Zlib => DBCompressionType::Zlib,
            Compression::Bz2 => DBCompressionType::Bz2,
            Compression::Lz4 => DBCompressionType::Lz4,
            Compression::Lz4hc => DBCompressionType::Lz4hc,
            Compression::Zstd => DBCompressionType::Zstd,
        }
    }
}

/// Compaction style.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStyle {
    /// Level-based compaction (default).  Good for general workloads.
    Level,
    /// Universal (size-tiered) compaction.  Good for write-heavy workloads.
    Universal,
    /// FIFO compaction.  Drops oldest data when the size limit is reached.
    Fifo,
}

impl From<CompactionStyle> for DBCompactionStyle {
    fn from(s: CompactionStyle) -> Self {
        match s {
            CompactionStyle::Level => DBCompactionStyle::Level,
            CompactionStyle::Universal => DBCompactionStyle::Universal,
            CompactionStyle::Fifo => DBCompactionStyle::Fifo,
        }
    }
}

/// WAL recovery mode after a crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecoveryMode {
    /// Tolerate incomplete records at the tail of the WAL.
    TolerateCorruptedTailRecords,
    /// Fail if any corruption is detected.
    AbsoluteConsistency,
    /// Recover to the last consistent point in time.
    PointInTime,
    /// Skip any corrupted record and continue.
    SkipAnyCorruptedRecord,
}

impl From<WalRecoveryMode> for DBRecoveryMode {
    fn from(m: WalRecoveryMode) -> Self {
        match m {
            WalRecoveryMode::TolerateCorruptedTailRecords => {
                DBRecoveryMode::TolerateCorruptedTailRecords
            }
            WalRecoveryMode::AbsoluteConsistency => DBRecoveryMode::AbsoluteConsistency,
            WalRecoveryMode::PointInTime => DBRecoveryMode::PointInTime,
            WalRecoveryMode::SkipAnyCorruptedRecord => DBRecoveryMode::SkipAnyCorruptedRecord,
        }
    }
}

/// Configuration for a [`RocksDB`] instance.
///
/// Use [`Default::default()`] for sensible defaults tuned for write-heavy
/// workloads.  All sizes are in **bytes** unless noted otherwise.
///
/// Fields set to `None` use RocksDB upstream defaults.
#[derive(Debug, Clone)]
pub struct RocksDBConfig {
    // ── Write buffers / memtable ────────────────────────────────────
    /// Size of a single memtable (write buffer).
    ///
    /// Default: **128 MiB**.
    pub write_buffer_size: usize,

    /// Maximum number of memtables (active + immutable) before writes stall.
    ///
    /// Default: **6**.
    pub max_write_buffer_number: i32,

    /// Minimum immutable memtables to merge before flushing to L0.
    ///
    /// Default: **2**.
    pub min_write_buffer_number_to_merge: i32,

    /// Global cap across **all** column families.  `None` = no limit.
    ///
    /// Default: **None**.
    pub db_write_buffer_size: Option<usize>,

    /// Allow concurrent writes into the memtable.
    ///
    /// Default: **None** (upstream default, typically `true`).
    pub allow_concurrent_memtable_write: Option<bool>,

    /// Pipeline WAL write and memtable insert.
    ///
    /// Default: **None** (upstream default).
    pub enable_pipelined_write: Option<bool>,

    /// Adaptive yield for write threads.
    ///
    /// Default: **None** (upstream default).
    pub enable_write_thread_adaptive_yield: Option<bool>,

    /// Allow unordered writes for higher throughput.  Disables snapshot
    /// isolation guarantees.
    ///
    /// Default: **false**.
    pub unordered_write: bool,

    // ── Compression ─────────────────────────────────────────────────
    /// Compression algorithm for SST data blocks.
    ///
    /// Default: [`Compression::Lz4`].
    pub compression: Compression,

    /// Per-level compression overrides.  `None` = uniform compression.
    ///
    /// Default: **None**.
    pub compression_per_level: Option<Vec<Compression>>,

    /// Compression for the bottommost level (often Zstd for best ratio).
    ///
    /// Default: **None** (same as `compression`).
    pub bottommost_compression: Option<Compression>,

    /// Compression for WAL entries.
    ///
    /// Default: **None** (no WAL compression).
    pub wal_compression: Option<Compression>,

    // ── Parallelism ─────────────────────────────────────────────────
    /// Total background threads for flushes + compaction.
    /// `None` means auto-detect via `std::thread::available_parallelism()`.
    ///
    /// Default: **None** (auto).
    pub parallelism: Option<i32>,

    /// Maximum background jobs (flushes + compactions).
    ///
    /// Default: **None** (upstream default, typically 2).
    pub max_background_jobs: Option<i32>,

    /// Number of sub-compactions within a single compaction job.
    ///
    /// Default: **None** (upstream default, typically 1).
    pub max_subcompactions: Option<u32>,

    // ── Compaction ──────────────────────────────────────────────────
    /// Compaction strategy.
    ///
    /// Default: [`CompactionStyle::Level`].
    pub compaction_style: CompactionStyle,

    /// Number of L0 files that trigger a compaction into L1.
    ///
    /// Default: **None** (upstream default, typically 4).
    pub level_zero_file_num_compaction_trigger: Option<i32>,

    /// Number of L0 files that start slowing down writes.
    ///
    /// Default: **None** (upstream default, typically 20).
    pub level_zero_slowdown_writes_trigger: Option<i32>,

    /// Number of L0 files that completely stop writes.
    ///
    /// Default: **None** (upstream default, typically 36).
    pub level_zero_stop_writes_trigger: Option<i32>,

    /// Maximum total size of level-1.
    ///
    /// Default: **None** (upstream default, typically 256 MiB).
    pub max_bytes_for_level_base: Option<u64>,

    /// Size multiplier between adjacent levels.
    ///
    /// Default: **None** (upstream default, typically 10.0).
    pub max_bytes_for_level_multiplier: Option<f64>,

    /// Target SST file size at the base level.
    ///
    /// Default: **None** (upstream default, typically 64 MiB).
    pub target_file_size_base: Option<u64>,

    /// Dynamic level sizing for minimizing space and write amplification.
    ///
    /// Default: **false**.
    pub level_compaction_dynamic_level_bytes: bool,

    /// Number of levels in the LSM tree.
    ///
    /// Default: **None** (upstream default, typically 7).
    pub num_levels: Option<i32>,

    /// Disable automatic compactions entirely.
    ///
    /// Default: **false**.
    pub disable_auto_compactions: bool,

    /// Trigger a compaction for files older than this many seconds.
    ///
    /// Default: **None** (disabled).
    pub periodic_compaction_seconds: Option<u64>,

    /// Maximum bytes consumed by a single compaction job.
    ///
    /// Default: **None** (upstream default).
    pub max_compaction_bytes: Option<u64>,

    /// OS readahead during compaction I/O.
    ///
    /// Default: **None** (upstream default, 0).
    pub compaction_readahead_size: Option<usize>,

    /// Soft threshold (bytes) of pending compaction before writes slow down.
    ///
    /// Default: **None** (upstream default).
    pub soft_pending_compaction_bytes_limit: Option<usize>,

    /// Hard threshold (bytes) of pending compaction before writes stop.
    ///
    /// Default: **None** (upstream default).
    pub hard_pending_compaction_bytes_limit: Option<usize>,

    // ── Durability / WAL ────────────────────────────────────────────
    /// Use `fsync` instead of `fdatasync` for WAL and SST writes.
    ///
    /// **Durability:** `true` is safer (persists metadata), `false` is faster.
    ///
    /// Default: **false** (`fdatasync`).
    pub use_fsync: bool,

    /// Sync the WAL on every write.  When `false` (default), an application
    /// crash may lose the most recent writes that are still in the OS buffer.
    ///
    /// **Durability:** `true` = every write is durable after `put` returns;
    /// `false` = writes are batched in the OS page cache.
    ///
    /// Default: **false**.
    pub sync_writes: bool,

    /// Disable the WAL entirely.  Writes are **not** crash-safe.
    ///
    /// **Durability:** setting this to `true` means all data since the last
    /// flush/compaction is lost on crash.
    ///
    /// Default: **false**.
    pub disable_wal: bool,

    /// Do not auto-flush the WAL after each `WriteBatch`.  The caller must
    /// trigger flushes manually.
    ///
    /// Default: **false**.
    pub manual_wal_flush: bool,

    /// Periodically sync WAL data to disk (bytes between syncs).
    ///
    /// Default: **None** (upstream default, 0 = disabled).
    pub wal_bytes_per_sync: Option<u64>,

    /// Periodically sync SST data to disk (bytes between syncs).
    ///
    /// Default: **None** (upstream default, 0 = disabled).
    pub bytes_per_sync: Option<u64>,

    /// Global size limit for all WAL files.  Oldest WALs are archived
    /// or deleted when exceeded.
    ///
    /// Default: **None** (upstream default, 0 = unlimited).
    pub max_total_wal_size: Option<u64>,

    /// WAL recovery behaviour after a crash.
    ///
    /// Default: **None** (upstream default, `PointInTime`).
    pub wal_recovery_mode: Option<WalRecoveryMode>,

    /// Atomically flush all column families together.  Useful when
    /// cross-CF consistency is required.
    ///
    /// Default: **false**.
    pub atomic_flush: bool,

    // ── Block-based table / cache ───────────────────────────────────
    /// Block cache capacity (LRU).  `None` = RocksDB default (8 MiB).
    ///
    /// Default: **None**.
    pub block_cache_size: Option<usize>,

    /// Data block size inside SST files.
    ///
    /// Default: **None** (upstream default, ~4 KiB).
    pub block_size: Option<usize>,

    /// Bloom filter bits per key.  `None` disables bloom filters.
    /// Typical value: 10.0 (~1 % FPR).
    ///
    /// Default: **None** (no bloom filter).
    pub bloom_filter_bits_per_key: Option<f64>,

    /// Place index and filter blocks in the block cache instead of
    /// keeping them always in memory.
    ///
    /// Default: **None** (upstream default, `false`).
    pub cache_index_and_filter_blocks: Option<bool>,

    /// Pin L0 filter and index blocks in the block cache so they are
    /// never evicted.
    ///
    /// Default: **None** (upstream default, `false`).
    pub pin_l0_filter_and_index_blocks_in_cache: Option<bool>,

    /// SST format version.  Higher versions enable newer features.
    ///
    /// Default: **None** (upstream default).
    pub format_version: Option<i32>,

    /// Enable whole-key filtering in block-based tables.
    ///
    /// Default: **None** (upstream default, `true`).
    pub whole_key_filtering: Option<bool>,

    /// Optimize bloom filters for workloads where most lookups are hits.
    ///
    /// Default: **false**.
    pub optimize_filters_for_hits: bool,

    // ── I/O ─────────────────────────────────────────────────────────
    /// Maximum number of open file descriptors.  `-1` = unlimited.
    ///
    /// Default: **None** (upstream default, -1).
    pub max_open_files: Option<i32>,

    /// Use O_DIRECT for user reads (bypasses OS page cache).
    ///
    /// Default: **false**.
    pub use_direct_reads: bool,

    /// Use O_DIRECT for flushes and compaction (bypasses OS page cache).
    ///
    /// Default: **false**.
    pub use_direct_io_for_flush_and_compaction: bool,

    /// Hint to the OS to use random access pattern on open.
    ///
    /// Default: **None** (upstream default, `true`).
    pub advise_random_on_open: Option<bool>,

    /// Use mmap for reads.
    ///
    /// Default: **false**.
    pub allow_mmap_reads: bool,

    /// Use mmap for writes.
    ///
    /// Default: **false**.
    pub allow_mmap_writes: bool,

    /// OS write buffer size for writable files.
    ///
    /// Default: **None** (upstream default).
    pub writable_file_max_buffer_size: Option<u64>,

    // ── BlobDB (large-value separation) ─────────────────────────────
    /// Enable BlobDB for large values.
    ///
    /// Default: **false**.
    pub enable_blob_files: bool,

    /// Minimum value size for BlobDB separation.
    ///
    /// Default: **None** (upstream default).
    pub min_blob_size: Option<u64>,

    /// Target blob file size.
    ///
    /// Default: **None** (upstream default).
    pub blob_file_size: Option<u64>,

    /// Compression for blob files.
    ///
    /// Default: **None** (no compression).
    pub blob_compression: Option<Compression>,

    /// Enable garbage collection for blob files.
    ///
    /// Default: **None** (upstream default).
    pub enable_blob_gc: Option<bool>,

    /// Age cutoff ratio for blob GC (0.0..=1.0).
    ///
    /// Default: **None** (upstream default, 0.25).
    pub blob_gc_age_cutoff: Option<f64>,

    /// Force GC threshold ratio (0.0..=1.0).
    ///
    /// Default: **None** (upstream default, 1.0).
    pub blob_gc_force_threshold: Option<f64>,

    // ── Rate limiting ───────────────────────────────────────────────
    /// Rate limit for background I/O (bytes/sec).  `None` = no limit.
    ///
    /// Default: **None**.
    pub rate_bytes_per_sec: Option<i64>,

    /// Use auto-tuned rate limiter.  Only effective when
    /// `rate_bytes_per_sec` is set.
    ///
    /// Default: **false**.
    pub rate_limiter_auto_tuned: bool,

    // ── Miscellaneous ───────────────────────────────────────────────
    /// Create the database if it does not exist.
    ///
    /// Default: **true**.
    pub create_if_missing: bool,

    /// Create missing column families when opening the database.
    ///
    /// Default: **true**.
    pub create_missing_column_families: bool,

    /// Enable extra consistency checks.
    ///
    /// Default: **None** (upstream default, `true`).
    pub paranoid_checks: Option<bool>,

    /// Enable RocksDB internal statistics collection.
    ///
    /// Default: **false**.
    pub enable_statistics: bool,

    /// Dump statistics to the info log every N seconds.
    ///
    /// Default: **None** (upstream default, 600).
    pub stats_dump_period_sec: Option<u32>,

    /// Skip loading table stats on open for faster startup.
    ///
    /// Default: **false**.
    pub skip_stats_update_on_db_open: bool,

    /// Skip SST file size verification on open for faster startup.
    ///
    /// Default: **false**.
    pub skip_checking_sst_file_sizes_on_db_open: bool,

    /// Interval for deleting obsolete files (microseconds).
    ///
    /// Default: **None** (upstream default).
    pub delete_obsolete_files_period_micros: Option<u64>,

    /// TTL for data in seconds.  Expired data is removed on compaction.
    ///
    /// Default: **None** (disabled).
    pub ttl: Option<u64>,
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        Self {
            // Write buffers
            write_buffer_size: 128 * 1024 * 1024,
            max_write_buffer_number: 6,
            min_write_buffer_number_to_merge: 2,
            db_write_buffer_size: None,
            allow_concurrent_memtable_write: None,
            enable_pipelined_write: None,
            enable_write_thread_adaptive_yield: None,
            unordered_write: false,
            // Compression
            compression: Compression::Lz4,
            compression_per_level: None,
            bottommost_compression: None,
            wal_compression: None,
            // Parallelism
            parallelism: None,
            max_background_jobs: None,
            max_subcompactions: None,
            // Compaction
            compaction_style: CompactionStyle::Level,
            level_zero_file_num_compaction_trigger: None,
            level_zero_slowdown_writes_trigger: None,
            level_zero_stop_writes_trigger: None,
            max_bytes_for_level_base: None,
            max_bytes_for_level_multiplier: None,
            target_file_size_base: None,
            level_compaction_dynamic_level_bytes: false,
            num_levels: None,
            disable_auto_compactions: false,
            periodic_compaction_seconds: None,
            max_compaction_bytes: None,
            compaction_readahead_size: None,
            soft_pending_compaction_bytes_limit: None,
            hard_pending_compaction_bytes_limit: None,
            // Durability / WAL
            use_fsync: false,
            sync_writes: false,
            disable_wal: false,
            manual_wal_flush: false,
            wal_bytes_per_sync: None,
            bytes_per_sync: None,
            max_total_wal_size: None,
            wal_recovery_mode: None,
            atomic_flush: false,
            // Block cache / table
            block_cache_size: None,
            block_size: None,
            bloom_filter_bits_per_key: None,
            cache_index_and_filter_blocks: None,
            pin_l0_filter_and_index_blocks_in_cache: None,
            format_version: None,
            whole_key_filtering: None,
            optimize_filters_for_hits: false,
            // I/O
            max_open_files: None,
            use_direct_reads: false,
            use_direct_io_for_flush_and_compaction: false,
            advise_random_on_open: None,
            allow_mmap_reads: false,
            allow_mmap_writes: false,
            writable_file_max_buffer_size: None,
            // BlobDB
            enable_blob_files: false,
            min_blob_size: None,
            blob_file_size: None,
            blob_compression: None,
            enable_blob_gc: None,
            blob_gc_age_cutoff: None,
            blob_gc_force_threshold: None,
            // Rate limiting
            rate_bytes_per_sec: None,
            rate_limiter_auto_tuned: false,
            // Misc
            create_if_missing: true,
            create_missing_column_families: true,
            paranoid_checks: None,
            enable_statistics: false,
            stats_dump_period_sec: None,
            skip_stats_update_on_db_open: false,
            skip_checking_sst_file_sizes_on_db_open: false,
            delete_obsolete_files_period_micros: None,
            ttl: None,
        }
    }
}

impl RocksDBConfig {
    /// Sets the write buffer (memtable) size.
    #[must_use]
    pub fn write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_size = bytes;
        self
    }

    /// Sets the max number of write buffers.
    #[must_use]
    pub fn max_write_buffer_number(mut self, n: i32) -> Self {
        self.max_write_buffer_number = n;
        self
    }

    /// Sets the compression algorithm.
    #[must_use]
    pub fn compression(mut self, c: Compression) -> Self {
        self.compression = c;
        self
    }

    /// Sets the background parallelism level.
    #[must_use]
    pub fn parallelism(mut self, n: i32) -> Self {
        self.parallelism = Some(n);
        self
    }

    /// Sets the compaction style.
    #[must_use]
    pub fn compaction_style(mut self, s: CompactionStyle) -> Self {
        self.compaction_style = s;
        self
    }

    /// Enables or disables `fsync` (vs `fdatasync`).
    #[must_use]
    pub fn use_fsync(mut self, flag: bool) -> Self {
        self.use_fsync = flag;
        self
    }

    /// Enables synchronous writes (WAL fsync on every write).
    #[must_use]
    pub fn sync_writes(mut self, flag: bool) -> Self {
        self.sync_writes = flag;
        self
    }

    /// Disables the WAL entirely.
    #[must_use]
    pub fn disable_wal(mut self, flag: bool) -> Self {
        self.disable_wal = flag;
        self
    }

    /// Sets the WAL recovery mode.
    #[must_use]
    pub fn wal_recovery_mode(mut self, mode: WalRecoveryMode) -> Self {
        self.wal_recovery_mode = Some(mode);
        self
    }

    /// Sets the block cache size.
    #[must_use]
    pub fn block_cache_size(mut self, bytes: usize) -> Self {
        self.block_cache_size = Some(bytes);
        self
    }

    /// Sets the bloom filter bits per key.
    #[must_use]
    pub fn bloom_filter_bits_per_key(mut self, bits: f64) -> Self {
        self.bloom_filter_bits_per_key = Some(bits);
        self
    }

    /// Sets the maximum open file descriptors.
    #[must_use]
    pub fn max_open_files(mut self, n: i32) -> Self {
        self.max_open_files = Some(n);
        self
    }

    /// Sets the rate limiter (bytes/sec).
    #[must_use]
    pub fn rate_bytes_per_sec(mut self, rate: i64) -> Self {
        self.rate_bytes_per_sec = Some(rate);
        self
    }
}

/// Key-value database backed by [RocksDB](https://docs.rs/rust-rocksdb).
///
/// Each table corresponds to a RocksDB column family. The database is opened
/// with configurable compression, compaction parallelism, and write buffers.
/// The handle is wrapped in an `Arc` for cheap cloning.
#[derive(Clone)]
pub struct RocksDB {
    inner: Arc<RwLock<Arc<Rocks>>>,
    path: String,
    opts: Arc<Options>,
    write_opts: Option<Arc<WriteOptions>>,
}

#[cfg(feature = "tokio")]
crate::impl_async_kvdb_via_spawn_blocking!(RocksDB);

const DEFAULT_CPU_CORES: usize = 4;

impl RocksDB {
    fn lock_poisoned() -> io::Error {
        io::Error::other("RocksDB lock poisoned")
    }

    fn build_database(path: &str, opts: &Options) -> io::Result<Rocks> {
        let cfs = Rocks::list_cf(opts, path).unwrap_or_default();
        let descriptors = cfs
            .iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf, opts.clone()))
            .collect::<Vec<_>>();
        Rocks::open_cf_descriptors(opts, path, descriptors).map_err(io::Error::other)
    }

    /// Attempt to recover from an unrecoverable database error by
    /// dropping the current handle and re-opening from disk.
    pub fn try_recover_from_error(&self) -> io::Result<()> {
        let mut guard = self.inner.write().map_err(|_| Self::lock_poisoned())?;
        let new_db = Self::build_database(&self.path, &self.opts)?;
        *guard = Arc::new(new_db);
        Ok(())
    }

    fn inner(&self) -> io::Result<Arc<Rocks>> {
        let guard = self.inner.read().map_err(|_| Self::lock_poisoned())?;
        Ok(Arc::clone(&guard))
    }

    /// Opens (or creates) a RocksDB database at the given filesystem `path`
    /// using [`RocksDBConfig::default()`].
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_config(path, RocksDBConfig::default())
    }

    /// Opens (or creates) a RocksDB database at `path` with custom
    /// [`RocksDBConfig`].
    pub fn open_with_config(path: &Path, config: RocksDBConfig) -> io::Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        let mut opts = Options::default();

        // Parallelism
        opts.increase_parallelism(
            config.parallelism.unwrap_or(
                std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or(1)
                    .min(DEFAULT_CPU_CORES) as i32,
            ),
        );
        if let Some(n) = config.max_background_jobs {
            opts.set_max_background_jobs(n);
        }
        if let Some(n) = config.max_subcompactions {
            opts.set_max_subcompactions(n);
        }

        // Write buffers / memtable
        opts.set_write_buffer_size(config.write_buffer_size);
        opts.set_max_write_buffer_number(config.max_write_buffer_number);
        opts.set_min_write_buffer_number_to_merge(config.min_write_buffer_number_to_merge);
        if let Some(size) = config.db_write_buffer_size {
            opts.set_db_write_buffer_size(size);
        }
        if let Some(v) = config.allow_concurrent_memtable_write {
            opts.set_allow_concurrent_memtable_write(v);
        }
        if let Some(v) = config.enable_pipelined_write {
            opts.set_enable_pipelined_write(v);
        }
        if let Some(v) = config.enable_write_thread_adaptive_yield {
            opts.set_enable_write_thread_adaptive_yield(v);
        }
        if config.unordered_write {
            opts.set_unordered_write(true);
        }

        // Compression
        opts.set_compression_type(config.compression.into());
        if let Some(ref levels) = config.compression_per_level {
            let types: Vec<DBCompressionType> = levels.iter().map(|c| (*c).into()).collect();
            opts.set_compression_per_level(&types);
        }
        if let Some(c) = config.bottommost_compression {
            opts.set_bottommost_compression_type(c.into());
        }
        if let Some(c) = config.wal_compression {
            opts.set_wal_compression_type(c.into());
        }

        // Compaction
        opts.set_compaction_style(config.compaction_style.into());
        if let Some(n) = config.level_zero_file_num_compaction_trigger {
            opts.set_level_zero_file_num_compaction_trigger(n);
        }
        if let Some(n) = config.level_zero_slowdown_writes_trigger {
            opts.set_level_zero_slowdown_writes_trigger(n);
        }
        if let Some(n) = config.level_zero_stop_writes_trigger {
            opts.set_level_zero_stop_writes_trigger(n);
        }
        if let Some(n) = config.max_bytes_for_level_base {
            opts.set_max_bytes_for_level_base(n);
        }
        if let Some(n) = config.max_bytes_for_level_multiplier {
            opts.set_max_bytes_for_level_multiplier(n);
        }
        if let Some(n) = config.target_file_size_base {
            opts.set_target_file_size_base(n);
        }
        if config.level_compaction_dynamic_level_bytes {
            opts.set_level_compaction_dynamic_level_bytes(true);
        }
        if let Some(n) = config.num_levels {
            opts.set_num_levels(n);
        }
        if config.disable_auto_compactions {
            opts.set_disable_auto_compactions(true);
        }
        if let Some(n) = config.periodic_compaction_seconds {
            opts.set_periodic_compaction_seconds(n);
        }
        if let Some(n) = config.max_compaction_bytes {
            opts.set_max_compaction_bytes(n);
        }
        if let Some(n) = config.compaction_readahead_size {
            opts.set_compaction_readahead_size(n);
        }
        if let Some(n) = config.soft_pending_compaction_bytes_limit {
            opts.set_soft_pending_compaction_bytes_limit(n);
        }
        if let Some(n) = config.hard_pending_compaction_bytes_limit {
            opts.set_hard_pending_compaction_bytes_limit(n);
        }

        // Durability / WAL
        opts.set_use_fsync(config.use_fsync);
        if config.manual_wal_flush {
            opts.set_manual_wal_flush(true);
        }
        if let Some(n) = config.wal_bytes_per_sync {
            opts.set_wal_bytes_per_sync(n);
        }
        if let Some(n) = config.bytes_per_sync {
            opts.set_bytes_per_sync(n);
        }
        if let Some(n) = config.max_total_wal_size {
            opts.set_max_total_wal_size(n);
        }
        if let Some(m) = config.wal_recovery_mode {
            opts.set_wal_recovery_mode(m.into());
        }
        if config.atomic_flush {
            opts.set_atomic_flush(true);
        }

        // Block-based table options
        let mut block_opts = BlockBasedOptions::default();
        let mut block_opts_modified = false;
        if let Some(size) = config.block_cache_size {
            block_opts.set_block_cache(&Cache::new_lru_cache(size));
            block_opts_modified = true;
        }
        if let Some(size) = config.block_size {
            block_opts.set_block_size(size);
            block_opts_modified = true;
        }
        if let Some(bits) = config.bloom_filter_bits_per_key {
            block_opts.set_bloom_filter(bits, false);
            block_opts_modified = true;
        }
        if let Some(v) = config.cache_index_and_filter_blocks {
            block_opts.set_cache_index_and_filter_blocks(v);
            block_opts_modified = true;
        }
        if let Some(v) = config.pin_l0_filter_and_index_blocks_in_cache {
            block_opts.set_pin_l0_filter_and_index_blocks_in_cache(v);
            block_opts_modified = true;
        }
        if let Some(v) = config.format_version {
            block_opts.set_format_version(v);
            block_opts_modified = true;
        }
        if let Some(v) = config.whole_key_filtering {
            block_opts.set_whole_key_filtering(v);
            block_opts_modified = true;
        }
        if block_opts_modified {
            opts.set_block_based_table_factory(&block_opts);
        }
        if config.optimize_filters_for_hits {
            opts.set_optimize_filters_for_hits(true);
        }

        // I/O
        if let Some(n) = config.max_open_files {
            opts.set_max_open_files(n);
        }
        if config.use_direct_reads {
            opts.set_use_direct_reads(true);
        }
        if config.use_direct_io_for_flush_and_compaction {
            opts.set_use_direct_io_for_flush_and_compaction(true);
        }
        if let Some(v) = config.advise_random_on_open {
            opts.set_advise_random_on_open(v);
        }
        if config.allow_mmap_reads {
            opts.set_allow_mmap_reads(true);
        }
        if config.allow_mmap_writes {
            opts.set_allow_mmap_writes(true);
        }
        if let Some(n) = config.writable_file_max_buffer_size {
            opts.set_writable_file_max_buffer_size(n);
        }

        // BlobDB
        if config.enable_blob_files {
            opts.set_enable_blob_files(true);
        }
        if let Some(n) = config.min_blob_size {
            opts.set_min_blob_size(n);
        }
        if let Some(n) = config.blob_file_size {
            opts.set_blob_file_size(n);
        }
        if let Some(c) = config.blob_compression {
            opts.set_blob_compression_type(c.into());
        }
        if let Some(v) = config.enable_blob_gc {
            opts.set_enable_blob_gc(v);
        }
        if let Some(v) = config.blob_gc_age_cutoff {
            opts.set_blob_gc_age_cutoff(v);
        }
        if let Some(v) = config.blob_gc_force_threshold {
            opts.set_blob_gc_force_threshold(v);
        }

        // Rate limiter
        if let Some(rate) = config.rate_bytes_per_sec {
            if config.rate_limiter_auto_tuned {
                opts.set_auto_tuned_ratelimiter(rate, 100_000, 10);
            } else {
                opts.set_ratelimiter(rate, 100_000, 10);
            }
        }

        // Misc
        opts.create_if_missing(config.create_if_missing);
        opts.create_missing_column_families(config.create_missing_column_families);
        if let Some(v) = config.paranoid_checks {
            opts.set_paranoid_checks(v);
        }
        if config.enable_statistics {
            opts.enable_statistics();
        }
        if let Some(n) = config.stats_dump_period_sec {
            opts.set_stats_dump_period_sec(n);
        }
        if config.skip_stats_update_on_db_open {
            opts.set_skip_stats_update_on_db_open(true);
        }
        if config.skip_checking_sst_file_sizes_on_db_open {
            #[allow(deprecated)]
            opts.set_skip_checking_sst_file_sizes_on_db_open(true);
        }
        if let Some(n) = config.delete_obsolete_files_period_micros {
            opts.set_delete_obsolete_files_period_micros(n);
        }
        if let Some(n) = config.ttl {
            opts.set_ttl(n);
        }

        let write_opts = if config.sync_writes || config.disable_wal {
            let mut wo = WriteOptions::default();
            if config.sync_writes {
                wo.set_sync(true);
            }
            if config.disable_wal {
                wo.disable_wal(true);
            }
            Some(wo)
        } else {
            None
        };

        let cfs = Rocks::list_cf(&opts, path).unwrap_or_default();

        let descriptors = cfs
            .iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf, opts.clone()))
            .collect::<Vec<_>>();

        let db = Rocks::open_cf_descriptors(&opts, path, descriptors).map_err(io::Error::other)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(db))),
            path: path_str,
            opts: Arc::new(opts),
            write_opts: write_opts.map(Arc::new),
        })
    }
}

impl KeyValueDB for RocksDB {
    /// Note: the get-then-put pattern is not atomic. Under concurrent access,
    /// the returned "old value" may be stale. Use the transactional API
    /// (`TransactionalKVDB`) for atomic read-modify-write operations.
    fn insert(&self, table: &str, key: &str, value: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let inner = self.inner()?;
        let db = &*inner;

        // Create cf if not exists (race-safe: retry handle on create failure)
        let cf = if let Some(cf) = db.cf_handle(table) {
            cf
        } else {
            match db.create_cf(table, &self.opts) {
                Ok(()) => {}
                Err(_) if db.cf_handle(table).is_some() => {
                    // Another thread created it concurrently — that's fine
                }
                Err(e) => return Err(io::Error::other(e)),
            }
            db.cf_handle(table).ok_or(io::Error::other(
                "Failed to get column family after creation",
            ))?
        };

        let key_bytes = key.as_bytes();
        let old = db.get_cf(&cf, key_bytes).map_err(io::Error::other)?;

        if let Some(ref wo) = self.write_opts {
            db.put_cf_opt(&cf, key_bytes, value, wo)
                .map_err(io::Error::other)?;
        } else {
            db.put_cf(&cf, key_bytes, value).map_err(io::Error::other)?;
        }

        Ok(old)
    }

    fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let inner = self.inner()?;
        let db = &*inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        db.get_cf(&cf, key.as_bytes())
            .map_err(io::Error::other)?
            .map(Ok)
            .transpose()
    }

    /// Note: the get-then-delete pattern is not atomic. Under concurrent access,
    /// the returned "old value" may be stale. Use the transactional API
    /// (`TransactionalKVDB`) for atomic read-modify-write operations.
    fn remove(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let inner = self.inner()?;
        let db = &*inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(None),
        };

        let key_bytes = key.as_bytes();
        let old = db
            .get_cf(&cf, key_bytes)
            .map_err(io::Error::other)?
            .map(|v| v.to_vec());

        if old.is_some() {
            if let Some(ref wo) = self.write_opts {
                db.delete_cf_opt(&cf, key_bytes, wo)
                    .map_err(io::Error::other)?;
            } else {
                db.delete_cf(&cf, key_bytes).map_err(io::Error::other)?;
            }
        }
        Ok(old)
    }

    fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let inner = self.inner()?;
        let db = &*inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let mut result = Vec::new();
        let iter = db.iterator_cf(&cf, IteratorMode::Start);
        for item in iter {
            let (k, v) = item.map_err(io::Error::other)?;
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut names = Rocks::list_cf(&self.opts, &self.path)
            .map_err(io::Error::other)?
            .into_iter()
            .filter(|n| n != DEFAULT_CF)
            .collect::<Vec<String>>();

        names.sort();
        Ok(names)
    }

    fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        let inner = self.inner()?;
        let db = &*inner;

        let cf = match db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };

        let prefix_b = prefix.as_bytes();
        let iter = db.iterator_cf(&cf, IteratorMode::From(prefix_b, Direction::Forward));

        let mut result = Vec::new();
        for item in iter {
            let (k, v) = item.map_err(io::Error::other)?;
            if !k.starts_with(prefix_b) {
                break;
            }
            let k_str = String::from_utf8(k.to_vec()).map_err(io::Error::other)?;
            result.push((k_str, v.to_vec()));
        }
        Ok(result)
    }

    fn contains_table(&self, table: &str) -> Result<bool, io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use default column family as table",
            ));
        }

        Ok(self.inner()?.cf_handle(table).is_some())
    }

    fn delete_table(&self, table: &str) -> Result<(), io::Error> {
        if table == DEFAULT_CF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot delete default column family",
            ));
        }

        let inner = self.inner()?;
        let db = &*inner;

        if db.cf_handle(table).is_some() {
            db.drop_cf(table).map_err(io::Error::other)?;
        }

        Ok(())
    }

    fn clear(&self) -> Result<(), io::Error> {
        let current_tables = Rocks::list_cf(&self.opts, &self.path)
            .map_err(io::Error::other)?
            .into_iter()
            .filter(|n| n != DEFAULT_CF)
            .collect::<Vec<String>>();

        let inner = self.inner()?;
        let db = &*inner;

        for table_name in current_tables {
            if db.cf_handle(&table_name).is_some() {
                db.drop_cf(&table_name).map_err(io::Error::other)?;
            }
        }

        Ok(())
    }
}
