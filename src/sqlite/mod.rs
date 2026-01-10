use std::{io, path::Path};

use async_trait::async_trait;
use libsql::{Builder, Connection, Database, TransactionBehavior, params};

use crate::AsyncKeyValueDB;

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use self::transactional::{ReadTransaction, WriteTransaction};

#[derive(Debug)]
pub struct SqliteDB {
    inner: Database,
}

async fn table_exists(conn: &Connection, table: &str) -> Result<bool, io::Error> {
    let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    let stmt = conn.prepare(sql).await.map_err(io::Error::other)?;
    let mut rows = stmt.query([table]).await.map_err(io::Error::other)?;
    Ok(rows.next().await.map_err(io::Error::other)?.is_some())
}

async fn ensure_table(conn: &Connection, table: &str) -> Result<(), io::Error> {
    if !table_exists(conn, table).await? {
        let sql = format!(
            "CREATE TABLE \"{}\" (key TEXT PRIMARY KEY, value BLOB)",
            table
        );
        conn.execute(&sql, ()).await.map_err(io::Error::other)?;
    }
    Ok(())
}

impl SqliteDB {
    pub async fn open(path: &Path) -> io::Result<Self> {
        let inner = Builder::new_local(path)
            .build()
            .await
            .map_err(io::Error::other)?;

        let conn = inner.connect().map_err(io::Error::other)?;

        // Set WAL mode
        let mut rows = conn
            .query("PRAGMA journal_mode = WAL", ())
            .await
            .map_err(io::Error::other)?;

        // consume the result to confirm it worked
        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let mode: String = row.get(0).map_err(io::Error::other)?;
            if mode != "wal" {
                return Err(io::Error::other(format!(
                    "Failed to set WAL mode, got: {}",
                    mode
                )));
            }
        }

        // Set synchronous = NORMAL
        let mut rows = conn
            .query("PRAGMA synchronous = NORMAL", ())
            .await
            .map_err(io::Error::other)?;

        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let value: i64 = row.get(0).map_err(io::Error::other)?;
            if value != 1 {
                // NORMAL == 1 in SQLite
                return Err(io::Error::other(format!(
                    "Failed to set synchronous = NORMAL, got: {}",
                    value
                )));
            }
        }

        Ok(Self { inner })
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
        let conn = self.inner.connect().map_err(io::Error::other)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(io::Error::other)?;
        ensure_table(&tx, table).await?;
        let old = self.get(table, key).await?; // Reuse get for old value; it's read-only and safe
        let sql = format!(
            "INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?, ?)",
            table
        );
        tx.execute(&sql, params![key, value])
            .await
            .map_err(io::Error::other)?;
        tx.commit().await.map_err(io::Error::other)?;
        Ok(old)
    }

    async fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        if !table_exists(&conn, table).await? {
            return Ok(None);
        }
        let sql = format!("SELECT value FROM \"{}\" WHERE key = ?", table);
        let stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query([key]).await.map_err(io::Error::other)?;
        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let blob: Vec<u8> = row.get(0).map_err(io::Error::other)?;
            Ok(Some(blob))
        } else {
            Ok(None)
        }
    }

    async fn remove(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(io::Error::other)?;
        if !table_exists(&tx, table).await? {
            tx.commit().await.map_err(io::Error::other)?; // Commit even if no-op
            return Ok(None);
        }
        let old = self.get(table, key).await?; // Reuse get for old value
        let sql = format!("DELETE FROM \"{}\" WHERE key = ?", table);
        tx.execute(&sql, [key]).await.map_err(io::Error::other)?;
        tx.commit().await.map_err(io::Error::other)?;
        Ok(old)
    }

    async fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key, value FROM \"{}\"", table);
        let stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
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
        let conn = self.inner.connect().map_err(io::Error::other)?;
        let sql = "SELECT name FROM sqlite_master WHERE type='table'";
        let stmt = conn.prepare(sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let name: String = row.get(0).map_err(io::Error::other)?;
            out.push(name);
        }
        Ok(out)
    }

    // Overrides for efficiency

    async fn iter_from_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT key, value FROM \"{}\" WHERE key LIKE ? || '%'",
            table
        );
        let stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query([prefix]).await.map_err(io::Error::other)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let key: String = row.get(0).map_err(io::Error::other)?;
            let val: Vec<u8> = row.get(1).map_err(io::Error::other)?;
            result.push((key, val));
        }
        Ok(result)
    }

    async fn contains_table(&self, table: &str) -> Result<bool, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        table_exists(&conn, table).await
    }

    async fn contains_key(&self, table: &str, key: &str) -> Result<bool, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        if !table_exists(&conn, table).await? {
            return Ok(false);
        }
        let sql = format!("SELECT EXISTS (SELECT 1 FROM \"{}\" WHERE key = ?)", table);
        let stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query([key]).await.map_err(io::Error::other)?;
        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let exists: i64 = row.get(0).map_err(io::Error::other)?;
            Ok(exists > 0)
        } else {
            Ok(false)
        }
    }

    async fn keys(&self, table: &str) -> Result<Vec<String>, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key FROM \"{}\"", table);
        let stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let key: String = row.get(0).map_err(io::Error::other)?;
            keys.push(key);
        }
        Ok(keys)
    }

    async fn values(&self, table: &str) -> Result<Vec<Vec<u8>>, io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        if !table_exists(&conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT value FROM \"{}\"", table);
        let stmt = conn.prepare(&sql).await.map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut values = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let val: Vec<u8> = row.get(0).map_err(io::Error::other)?;
            values.push(val);
        }
        Ok(values)
    }

    async fn delete_table(&self, table: &str) -> Result<(), io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(io::Error::other)?;
        if table_exists(&tx, table).await? {
            let sql = format!("DROP TABLE \"{}\"", table);
            tx.execute(&sql, ()).await.map_err(io::Error::other)?;
        }
        tx.commit().await.map_err(io::Error::other)?;
        Ok(())
    }

    async fn clear(&self) -> Result<(), io::Error> {
        let conn = self.inner.connect().map_err(io::Error::other)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .map_err(io::Error::other)?;
        let tables = self.table_names().await?; // Reuse table_names; it's read-only
        for t in tables {
            let sql = format!("DROP TABLE \"{}\"", t);
            tx.execute(&sql, ()).await.map_err(io::Error::other)?;
        }
        tx.commit().await.map_err(io::Error::other)?;
        Ok(())
    }
}
