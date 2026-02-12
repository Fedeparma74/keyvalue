//! [SQLite](https://docs.rs/turso)-backed key-value store (async-only).
//!
//! Each table is a SQLite table with columns `(key TEXT PRIMARY KEY, value BLOB)`.
//! The database uses WAL journal mode for better concurrent read performance.
//!
//! **Note:** This backend only implements [`AsyncKeyValueDB`](crate::AsyncKeyValueDB)
//! because the underlying `turso` driver is async.

use std::{io, path::Path, sync::Arc};

use async_trait::async_trait;
use turso::{Builder, Connection, Database, params};

use crate::AsyncKeyValueDB;
#[cfg(feature = "transactional")]
mod transactional;
#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

/// SQLite-backed async key-value database.
///
/// Wraps a `turso::Connection` and `turso::Database`. The connection is shared
/// via `Arc` so the struct can be passed across async tasks.
#[derive(Debug)]
pub struct SqliteDB {
    conn: Arc<Connection>,
    db: Arc<Database>,
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
    /// Opens (or creates) a SQLite database at the given filesystem `path`.
    ///
    /// The connection is configured with `PRAGMA journal_mode=WAL` for
    /// improved concurrent read throughput.
    pub async fn open(path: &Path) -> io::Result<Self> {
        let inner = Builder::new_local(
            path.to_str()
                .ok_or(io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"))?,
        )
        .build()
        .await
        .map_err(io::Error::other)?;
        let conn = inner.connect().map_err(io::Error::other)?;

        // Enable WAL mode for concurrent read/write access
        let mut wal_stmt = conn
            .prepare("PRAGMA journal_mode=WAL")
            .await
            .map_err(io::Error::other)?;
        wal_stmt.query(()).await.map_err(io::Error::other)?;

        Ok(Self {
            db: Arc::new(inner),
            conn: Arc::new(conn),
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
        self.conn
            .execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            ensure_table(&self.conn, table).await?;
            let old = self.get(table, key).await?;
            let sql = format!(
                "INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?, ?)",
                table
            );
            self.conn
                .execute(&sql, params![key, value])
                .await
                .map_err(io::Error::other)?;
            Ok(old)
        }
        .await;
        match result {
            Ok(old) => {
                self.conn
                    .execute("COMMIT", ())
                    .await
                    .map_err(io::Error::other)?;
                Ok(old)
            }
            Err(e) => {
                let _ = self.conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    async fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
        if !table_exists(&self.conn, table).await? {
            return Ok(None);
        }
        let sql = format!("SELECT value FROM \"{}\" WHERE key = ?", table);
        let mut stmt = self.conn.prepare(&sql).await.map_err(io::Error::other)?;
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
        self.conn
            .execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            if !table_exists(&self.conn, table).await? {
                return Ok(None);
            }
            let old = self.get(table, key).await?;
            let sql = format!("DELETE FROM \"{}\" WHERE key = ?", table);
            self.conn
                .execute(&sql, [key])
                .await
                .map_err(io::Error::other)?;
            Ok(old)
        }
        .await;
        match result {
            Ok(old) => {
                self.conn
                    .execute("COMMIT", ())
                    .await
                    .map_err(io::Error::other)?;
                Ok(old)
            }
            Err(e) => {
                let _ = self.conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    async fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        validate_table_name(table)?;
        if !table_exists(&self.conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key, value FROM \"{}\"", table);
        let mut stmt = self.conn.prepare(&sql).await.map_err(io::Error::other)?;
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
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'";
        let mut stmt = self.conn.prepare(sql).await.map_err(io::Error::other)?;
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
        if !table_exists(&self.conn, table).await? {
            return Ok(Vec::new());
        }
        let escaped = escape_like(prefix);
        let like_pattern = format!("{}%", escaped);
        let sql = format!(
            "SELECT key, value FROM \"{}\" WHERE key LIKE ? ESCAPE '\\'",
            table
        );
        let mut stmt = self.conn.prepare(&sql).await.map_err(io::Error::other)?;
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
        table_exists(&self.conn, table).await
    }

    async fn contains_key(&self, table: &str, key: &str) -> Result<bool, io::Error> {
        validate_table_name(table)?;
        if !table_exists(&self.conn, table).await? {
            return Ok(false);
        }
        let sql = format!("SELECT EXISTS (SELECT 1 FROM \"{}\" WHERE key = ?)", table);
        let mut stmt = self.conn.prepare(&sql).await.map_err(io::Error::other)?;
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
        if !table_exists(&self.conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key FROM \"{}\"", table);
        let mut stmt = self.conn.prepare(&sql).await.map_err(io::Error::other)?;
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
        if !table_exists(&self.conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT value FROM \"{}\"", table);
        let mut stmt = self.conn.prepare(&sql).await.map_err(io::Error::other)?;
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
        self.conn
            .execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            if table_exists(&self.conn, table).await? {
                let sql = format!("DROP TABLE \"{}\"", table);
                self.conn
                    .execute(&sql, ())
                    .await
                    .map_err(io::Error::other)?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                self.conn
                    .execute("COMMIT", ())
                    .await
                    .map_err(io::Error::other)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }

    async fn clear(&self) -> Result<(), io::Error> {
        self.conn
            .execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        let result = async {
            let tables = self.table_names().await?;
            for t in tables {
                let sql = format!("DROP TABLE \"{}\"", t);
                self.conn
                    .execute(&sql, ())
                    .await
                    .map_err(io::Error::other)?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                self.conn
                    .execute("COMMIT", ())
                    .await
                    .map_err(io::Error::other)?;
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute("ROLLBACK", ()).await;
                Err(e)
            }
        }
    }
}
