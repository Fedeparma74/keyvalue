//! [SQLite](https://docs.rs/turso)-backed key-value store (async-only).
//!
//! Each table is a SQLite table with columns `(key TEXT PRIMARY KEY, value BLOB)`.
//! The database uses WAL journal mode for better concurrent read performance.
//!
//! **Note:** This backend only implements [`AsyncKeyValueDB`](crate::AsyncKeyValueDB)
//! because the underlying `turso` driver is async.

use std::{
    io,
    path::Path,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use turso::{Builder, Connection, Database, params};

use crate::AsyncKeyValueDB;
#[cfg(feature = "transactional")]
mod transactional;
#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

/// SQLite journal mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// Write-Ahead Logging — allows concurrent readers while writing.
    Wal,
    /// Classic rollback journal (SQLite default).
    Delete,
    /// Truncate journal file to zero length instead of deleting.
    Truncate,
    /// In-memory journal — faster but no crash recovery.
    Memory,
    /// Disable journaling entirely (no atomicity guarantees).
    Off,
}

impl JournalMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::Delete => "DELETE",
            Self::Truncate => "TRUNCATE",
            Self::Memory => "MEMORY",
            Self::Off => "OFF",
        }
    }
}

/// SQLite synchronous mode, controlling how aggressively data is flushed.
///
/// **Durability:** higher modes are safer but slower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronousMode {
    /// No syncs at all.  A crash can corrupt the database.
    Off,
    /// Sync at the most critical moments.  A crash during a WAL checkpoint
    /// may corrupt the database but an application crash cannot.
    Normal,
    /// Sync after every transaction commit.  Crash-safe.
    Full,
    /// Extra syncs beyond `Full` for additional paranoia.
    Extra,
}

impl SynchronousMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
            Self::Extra => "EXTRA",
        }
    }
}

/// SQLite locking mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockingMode {
    /// Normal locking.  Other processes can access the database.
    Normal,
    /// Exclusive locking.  Faster, but blocks all other connections.
    Exclusive,
}

impl LockingMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Exclusive => "EXCLUSIVE",
        }
    }
}

/// Where SQLite stores temporary tables and indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempStore {
    /// Use the compile-time default.
    Default,
    /// Store temporaries in a file.
    File,
    /// Store temporaries in memory.
    Memory,
}

impl TempStore {
    fn as_i32(self) -> i32 {
        match self {
            Self::Default => 0,
            Self::File => 1,
            Self::Memory => 2,
        }
    }
}

/// SQLite auto-vacuum mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoVacuum {
    /// No auto-vacuum (default).  Database never shrinks.
    None,
    /// Full auto-vacuum.  Free pages are reclaimed after every transaction.
    Full,
    /// Incremental auto-vacuum.  Free pages are reclaimed in batches.
    Incremental,
}

impl AutoVacuum {
    fn as_i32(self) -> i32 {
        match self {
            Self::None => 0,
            Self::Full => 1,
            Self::Incremental => 2,
        }
    }
}

/// Configuration for a [`SqliteDB`] instance.
///
/// Use [`Default::default()`] for sensible defaults (WAL mode, synchronous
/// NORMAL).  Integer PRAGMA values use SQLite conventions (e.g. negative
/// `cache_size` means KiB).
#[derive(Debug, Clone)]
pub struct SqliteConfig {
    /// SQLite journal mode.
    ///
    /// Default: [`JournalMode::Wal`].
    pub journal_mode: JournalMode,

    /// Synchronous mode controlling fsync behaviour.
    ///
    /// **Durability:** `Full` or `Extra` guarantees every committed
    /// transaction survives a power loss.  `Normal` may lose the last
    /// transaction after a sudden power failure in WAL mode.  `Off` is
    /// fastest but a crash can corrupt the database.
    ///
    /// Default: [`SynchronousMode::Normal`].
    pub synchronous: SynchronousMode,

    /// Page cache size.  Negative values are in KiB (e.g. `-2000` = 2000 KiB).
    /// Positive values are in pages.
    ///
    /// Default: **None** (SQLite default, typically `-2000`).
    pub cache_size: Option<i64>,

    /// Busy timeout in milliseconds.  How long SQLite waits for a lock
    /// before returning `SQLITE_BUSY`.
    ///
    /// Default: **5000** (5 seconds).
    pub busy_timeout: u64,

    /// Enforce foreign key constraints.
    ///
    /// Default: **false** (SQLite default).
    pub foreign_keys: bool,

    /// Maximum memory-mapped I/O size in bytes.  `0` disables mmap.
    ///
    /// **Note:** not supported by all turso/libsql builds.  Setting this on
    /// an unsupported build will return an error at open time.
    ///
    /// Default: **None** (SQLite default, 0 = disabled).
    pub mmap_size: Option<i64>,

    /// Database page size in bytes.  Must be a power of two between 512
    /// and 65536.  Can only be changed **before** any tables are created.
    ///
    /// Default: **None** (SQLite default, typically 4096).
    pub page_size: Option<u32>,

    /// Locking mode.
    ///
    /// **Note:** not supported by all turso/libsql builds.  Setting this on
    /// an unsupported build will return an error at open time.
    ///
    /// Default: **None** (SQLite default, [`LockingMode::Normal`]).
    pub locking_mode: Option<LockingMode>,

    /// Where temporary tables and indices are stored.
    ///
    /// Default: **None** (SQLite default, [`TempStore::Default`]).
    pub temp_store: Option<TempStore>,

    /// Number of WAL pages before an automatic checkpoint.
    /// `0` disables automatic checkpoints.
    ///
    /// **Note:** not supported by all turso/libsql builds.  Setting this on
    /// an unsupported build will return an error at open time.
    ///
    /// Default: **None** (SQLite default, typically 1000).
    pub wal_autocheckpoint: Option<i32>,

    /// Auto-vacuum mode.  Can only be changed **before** any tables are
    /// created (or by running `VACUUM`).
    ///
    /// Default: **None** (SQLite default, [`AutoVacuum::None`]).
    pub auto_vacuum: Option<AutoVacuum>,

    /// Maximum WAL journal size in bytes.  `-1` means no limit.
    ///
    /// **Note:** not supported by all turso/libsql builds.  Setting this on
    /// an unsupported build will return an error at open time.
    ///
    /// Default: **None** (SQLite default, `-1`).
    pub journal_size_limit: Option<i64>,

    /// Overwrite deleted data with zeros for security.
    ///
    /// **Note:** not supported by all turso/libsql builds.  Setting this on
    /// an unsupported build will return an error at open time.
    ///
    /// Default: **None** (disabled).
    pub secure_delete: Option<bool>,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            journal_mode: JournalMode::Wal,
            synchronous: SynchronousMode::Normal,
            cache_size: None,
            busy_timeout: 5000,
            foreign_keys: false,
            mmap_size: None,
            page_size: None,
            locking_mode: None,
            temp_store: None,
            wal_autocheckpoint: None,
            auto_vacuum: None,
            journal_size_limit: None,
            secure_delete: None,
        }
    }
}

impl SqliteConfig {
    /// Sets the journal mode.
    #[must_use]
    pub fn journal_mode(mut self, mode: JournalMode) -> Self {
        self.journal_mode = mode;
        self
    }

    /// Sets the synchronous mode.
    #[must_use]
    pub fn synchronous(mut self, mode: SynchronousMode) -> Self {
        self.synchronous = mode;
        self
    }

    /// Sets the page cache size.
    #[must_use]
    pub fn cache_size(mut self, pages: i64) -> Self {
        self.cache_size = Some(pages);
        self
    }

    /// Sets the busy timeout in milliseconds.
    #[must_use]
    pub fn busy_timeout(mut self, ms: u64) -> Self {
        self.busy_timeout = ms;
        self
    }

    /// Enables or disables foreign key enforcement.
    #[must_use]
    pub fn foreign_keys(mut self, flag: bool) -> Self {
        self.foreign_keys = flag;
        self
    }

    /// Sets the mmap size in bytes.
    #[must_use]
    pub fn mmap_size(mut self, bytes: i64) -> Self {
        self.mmap_size = Some(bytes);
        self
    }

    /// Sets the database page size.
    #[must_use]
    pub fn page_size(mut self, bytes: u32) -> Self {
        self.page_size = Some(bytes);
        self
    }

    /// Sets the locking mode.
    #[must_use]
    pub fn locking_mode(mut self, mode: LockingMode) -> Self {
        self.locking_mode = Some(mode);
        self
    }

    /// Sets the temp-store location.
    #[must_use]
    pub fn temp_store(mut self, store: TempStore) -> Self {
        self.temp_store = Some(store);
        self
    }

    /// Sets the WAL autocheckpoint interval (pages).
    #[must_use]
    pub fn wal_autocheckpoint(mut self, pages: i32) -> Self {
        self.wal_autocheckpoint = Some(pages);
        self
    }

    /// Sets the auto-vacuum mode.
    #[must_use]
    pub fn auto_vacuum(mut self, mode: AutoVacuum) -> Self {
        self.auto_vacuum = Some(mode);
        self
    }

    /// Sets the journal size limit in bytes.
    #[must_use]
    pub fn journal_size_limit(mut self, bytes: i64) -> Self {
        self.journal_size_limit = Some(bytes);
        self
    }

    /// Enables or disables secure delete.
    #[must_use]
    pub fn secure_delete(mut self, flag: bool) -> Self {
        self.secure_delete = Some(flag);
        self
    }
}

/// SQLite-backed async key-value database.
///
/// Wraps a `turso::Connection` and `turso::Database`. The connection is shared
/// via `Arc` so the struct can be passed across async tasks.
pub struct SqliteDB {
    conn: RwLock<Arc<Connection>>,
    db: Arc<Database>,
    path: String,
    config: SqliteConfig,
}

impl std::fmt::Debug for SqliteDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteDB")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// Validates that a table name is a safe SQL identifier.
/// Rejects empty names, names containing double-quote characters (to prevent SQL injection
/// when used inside `"quoted_identifier"` syntax), and reserved SQLite-internal names.
/// Validates that a table name is safe to use in SQL statements.
///
/// Rejects empty names, names containing double-quote characters (which
/// would break identifier quoting), and names starting with `sqlite_`
/// (which are reserved by SQLite internals).
pub(crate) fn validate_table_name(table: &str) -> Result<(), io::Error> {
    if table.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "table name must not be empty",
        ));
    }
    if table.contains('"') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "table name must not contain double-quote characters",
        ));
    }
    if table.starts_with("sqlite_") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "table names starting with 'sqlite_' are reserved",
        ));
    }
    Ok(())
}

/// Escapes `%`, `_` and `\` in a LIKE pattern so the literal string is matched.
/// Escapes special characters for `LIKE` pattern matching.
fn escape_like(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' | '%' | '_' => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// Returns `true` if a table with the given name exists in the database.
pub(crate) async fn table_exists(conn: &Connection, table: &str) -> Result<bool, io::Error> {
    let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    let mut stmt = conn.prepare(sql).await.map_err(io::Error::other)?;
    let mut rows = stmt.query([table]).await.map_err(io::Error::other)?;
    Ok(rows.next().await.map_err(io::Error::other)?.is_some())
}

/// Creates the table if it does not already exist, after validating the name.
pub(crate) async fn ensure_table(conn: &Connection, table: &str) -> Result<(), io::Error> {
    validate_table_name(table)?;
    if !table_exists(conn, table).await? {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (key TEXT PRIMARY KEY, value BLOB)",
            table
        );
        conn.execute(&sql, ()).await.map_err(io::Error::other)?;
    }
    Ok(())
}

impl SqliteDB {
    fn lock_poisoned() -> io::Error {
        io::Error::other("SqliteDB lock poisoned")
    }

    fn conn(&self) -> io::Result<Arc<Connection>> {
        Ok(self.conn.read().map_err(|_| Self::lock_poisoned())?.clone())
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    async fn apply_pragmas(conn: &Connection, config: &SqliteConfig) -> io::Result<()> {
        async fn pragma(conn: &Connection, sql: &str) -> io::Result<()> {
            let mut stmt = conn.prepare(sql).await.map_err(io::Error::other)?;
            stmt.query(()).await.map_err(io::Error::other)?;
            Ok(())
        }

        if let Some(ps) = config.page_size {
            pragma(conn, &format!("PRAGMA page_size={ps}")).await?;
        }

        if let Some(av) = config.auto_vacuum {
            pragma(conn, &format!("PRAGMA auto_vacuum={}", av.as_i32())).await?;
        }

        pragma(
            conn,
            &format!("PRAGMA journal_mode={}", config.journal_mode.as_str()),
        )
        .await?;

        pragma(
            conn,
            &format!("PRAGMA synchronous={}", config.synchronous.as_str()),
        )
        .await?;

        pragma(
            conn,
            &format!("PRAGMA busy_timeout={}", config.busy_timeout),
        )
        .await?;

        pragma(
            conn,
            &format!(
                "PRAGMA foreign_keys={}",
                if config.foreign_keys { "ON" } else { "OFF" }
            ),
        )
        .await?;

        if let Some(sd) = config.secure_delete {
            pragma(
                conn,
                &format!("PRAGMA secure_delete={}", if sd { "ON" } else { "OFF" }),
            )
            .await?;
        }

        if let Some(cs) = config.cache_size {
            pragma(conn, &format!("PRAGMA cache_size={cs}")).await?;
        }

        if let Some(ms) = config.mmap_size {
            pragma(conn, &format!("PRAGMA mmap_size={ms}")).await?;
        }

        if let Some(lm) = config.locking_mode {
            pragma(conn, &format!("PRAGMA locking_mode={}", lm.as_str())).await?;
        }

        if let Some(ts) = config.temp_store {
            pragma(conn, &format!("PRAGMA temp_store={}", ts.as_i32())).await?;
        }

        if let Some(wac) = config.wal_autocheckpoint {
            pragma(conn, &format!("PRAGMA wal_autocheckpoint={wac}")).await?;
        }

        if let Some(jsl) = config.journal_size_limit {
            pragma(conn, &format!("PRAGMA journal_size_limit={jsl}")).await?;
        }

        Ok(())
    }

    /// Attempt to recover from an unrecoverable database error by
    /// creating a new connection and re-applying pragmas.
    pub async fn try_recover_from_error(&self) -> io::Result<()> {
        let new_conn = self.db.connect().map_err(io::Error::other)?;
        Self::apply_pragmas(&new_conn, &self.config).await?;
        let mut guard = self.conn.write().map_err(|_| Self::lock_poisoned())?;
        *guard = Arc::new(new_conn);
        Ok(())
    }

    /// Opens (or creates) a SQLite database at the given filesystem `path`
    /// using [`SqliteConfig::default()`].
    pub async fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_config(path, SqliteConfig::default()).await
    }

    /// Opens (or creates) a SQLite database at `path` with custom
    /// [`SqliteConfig`].
    pub async fn open_with_config(path: &Path, config: SqliteConfig) -> io::Result<Self> {
        let path_str = path
            .to_str()
            .ok_or(io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"))?
            .to_string();
        let inner = Builder::new_local(&path_str)
            .build()
            .await
            .map_err(io::Error::other)?;
        let conn = inner.connect().map_err(io::Error::other)?;

        Self::apply_pragmas(&conn, &config).await?;

        Ok(Self {
            conn: RwLock::new(Arc::new(conn)),
            db: Arc::new(inner),
            path: path_str,
            config,
        })
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncKeyValueDB for SqliteDB {
    async fn insert(
        &self,
        table: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            ensure_table(&conn, table).await?;
            let old = self.get(table, key).await?;
            let sql = format!(
                "INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?, ?)",
                table
            );
            conn.execute(&sql, params![key, value])
                .await
                .map_err(io::Error::other)?;
            Ok(old)
        }
        .await;
        match result {
            Ok(old) => {
                conn.execute("COMMIT", ()).await.map_err(io::Error::other)?;
                Ok(old)
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    async fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        if !table_exists(&conn, table).await? {
            return Ok(None);
        }
        let sql = format!("SELECT value FROM \"{}\" WHERE key = ?", table);
        let mut stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query([key]).await.map_err(io::Error::other)?;
        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let blob: Vec<u8> = row.get(0).map_err(io::Error::other)?;
            Ok(Some(blob))
        } else {
            Ok(None)
        }
    }

    async fn remove(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            if !table_exists(&conn, table).await? {
                return Ok(None);
            }
            let old = self.get(table, key).await?;
            let sql = format!("DELETE FROM \"{}\" WHERE key = ?", table);
            conn.execute(&sql, [key]).await.map_err(io::Error::other)?;
            Ok(old)
        }
        .await;
        match result {
            Ok(old) => {
                conn.execute("COMMIT", ()).await.map_err(io::Error::other)?;
                Ok(old)
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    async fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key, value FROM \"{}\"", table);
        let mut stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let key: String = row.get(0).map_err(io::Error::other)?;
            let val: Vec<u8> = row.get(1).map_err(io::Error::other)?;
            result.push((key, val));
        }
        Ok(result)
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let conn = self.conn()?;
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'";
        let mut stmt = conn.prepare(sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let name: String = row.get(0).map_err(io::Error::other)?;
            out.push(name);
        }
        Ok(out)
    }

    async fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let escaped = escape_like(prefix);
        let like_pattern = format!("{}%", escaped);
        let sql = format!(
            "SELECT key, value FROM \"{}\" WHERE key LIKE ? ESCAPE '\\'",
            table
        );
        let mut stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt
            .query([like_pattern.as_str()])
            .await
            .map_err(io::Error::other)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let key: String = row.get(0).map_err(io::Error::other)?;
            let val: Vec<u8> = row.get(1).map_err(io::Error::other)?;
            result.push((key, val));
        }
        Ok(result)
    }

    async fn contains_table(&self, table: &str) -> Result<bool, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        table_exists(&conn, table).await
    }

    async fn contains_key(&self, table: &str, key: &str) -> Result<bool, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        if !table_exists(&conn, table).await? {
            return Ok(false);
        }
        let sql = format!("SELECT EXISTS (SELECT 1 FROM \"{}\" WHERE key = ?)", table);
        let mut stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query([key]).await.map_err(io::Error::other)?;
        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let exists: i64 = row.get(0).map_err(io::Error::other)?;
            Ok(exists > 0)
        } else {
            Ok(false)
        }
    }

    async fn keys(&self, table: &str) -> Result<Vec<String>, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key FROM \"{}\"", table);
        let mut stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let key: String = row.get(0).map_err(io::Error::other)?;
            keys.push(key);
        }
        Ok(keys)
    }

    async fn values(&self, table: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT value FROM \"{}\"", table);
        let mut stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut values = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let val: Vec<u8> = row.get(0).map_err(io::Error::other)?;
            values.push(val);
        }
        Ok(values)
    }

    async fn delete_table(&self, table: &str) -> Result<(), io::Error> {
        validate_table_name(table)?;
        let conn = self.conn()?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            if table_exists(&conn, table).await? {
                let sql = format!("DROP TABLE \"{}\"", table);
                conn.execute(&sql, ()).await.map_err(io::Error::other)?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                conn.execute("COMMIT", ()).await.map_err(io::Error::other)?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    async fn clear(&self) -> Result<(), io::Error> {
        let conn = self.conn()?;
        conn.execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            let tables = self.table_names().await?;
            for t in tables {
                let sql = format!("DROP TABLE \"{}\"", t);
                conn.execute(&sql, ()).await.map_err(io::Error::other)?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                conn.execute("COMMIT", ()).await.map_err(io::Error::other)?;
                Ok(())
            }
            Err(e) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }
}
