use std::{io, sync::Arc};

use async_trait::async_trait;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};
use turso::Connection;

use crate::{
    AsyncKVReadTransaction, AsyncKVWriteTransaction, AsyncTransactionalKVDB,
    sqlite::{ensure_table, table_exists},
};

use super::SqliteDB;

pub struct ReadTransaction<'a> {
    conn: Arc<Connection>,
    _read_guard: RwLockReadGuard<'a, ()>,
}
pub struct WriteTransaction<'a> {
    conn: Arc<Connection>,
    _write_guard: RwLockWriteGuard<'a, ()>,
}
#[async_trait(?Send)]
impl<'a> AsyncKVReadTransaction<'a> for ReadTransaction<'a> {
    async fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if !table_exists(&self.conn, table).await? {
            return Ok(None);
        }
        let sql = format!("SELECT value FROM \"{}\" WHERE key = ?", table);
        let mut stmt = self
            .conn
            .prepare(sql.as_str())
            .await
            .map_err(io::Error::other)?;
        let mut rows = stmt.query([key]).await.map_err(io::Error::other)?;
        if let Some(row) = rows.next().await.unwrap_or(None) {
            let blob: Vec<u8> = row.get(0).unwrap_or_default();
            Ok(Some(blob))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if !table_exists(&self.conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key, value FROM \"{}\"", table);
        let mut stmt = self
            .conn
            .prepare(sql.as_str())
            .await
            .map_err(io::Error::other)?;
        let mut result = Vec::new();
        let mut stream = stmt.query(()).await.map_err(io::Error::other)?;
        while let Some(row) = stream.next().await.unwrap_or(None) {
            let key: String = row.get(0).unwrap_or_default();
            let val: Vec<u8> = row.get(1).unwrap_or_default();
            result.push((key, val));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .await
            .map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap_or(None) {
            let name: String = row.get(0).unwrap_or_default();
            out.push(name);
        }
        Ok(out)
    }
}

#[async_trait(?Send)]
impl<'a> AsyncKVReadTransaction<'a> for WriteTransaction<'a> {
    async fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if !table_exists(&self.conn, table).await? {
            return Ok(None);
        }
        let sql = format!("SELECT value FROM \"{}\" WHERE key = ?", table);
        let mut stmt = self
            .conn
            .prepare(sql.as_str())
            .await
            .map_err(io::Error::other)?;
        let mut rows = stmt.query([key]).await.map_err(io::Error::other)?;
        if let Some(row) = rows.next().await.unwrap_or(None) {
            let blob: Vec<u8> = row.get(0).unwrap_or_default();
            Ok(Some(blob))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        if !table_exists(&self.conn, table).await? {
            return Ok(Vec::new());
        }
        let sql = format!("SELECT key, value FROM \"{}\"", table);
        let mut stmt = self
            .conn
            .prepare(sql.as_str())
            .await
            .map_err(io::Error::other)?;
        let mut result = Vec::new();
        let mut stream = stmt.query(()).await.map_err(io::Error::other)?;
        while let Some(row) = stream.next().await.unwrap_or(None) {
            let key: String = row.get(0).unwrap_or_default();
            let val: Vec<u8> = row.get(1).unwrap_or_default();
            result.push((key, val));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut stmt = self
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .await
            .map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.unwrap_or(None) {
            let name: String = row.get(0).unwrap_or_default();
            out.push(name);
        }
        Ok(out)
    }
}

#[async_trait(?Send)]
impl<'a> AsyncKVWriteTransaction<'a> for WriteTransaction<'a> {
    async fn insert(
        &mut self,
        table: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        ensure_table(&self.conn, table).await?;
        let old = self.get(table, key).await?;
        let insert_sql = format!(
            "INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?, ?)",
            table
        );
        self.conn
            .execute(insert_sql.as_str(), (key, value))
            .await
            .map_err(io::Error::other)?;
        Ok(old)
    }
    async fn remove(&mut self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        if !table_exists(&self.conn, table).await? {
            return Ok(None);
        }
        let old = self.get(table, key).await?;
        let sql = format!("DELETE FROM \"{}\" WHERE key = ?", table);
        self.conn
            .execute(sql.as_str(), [key])
            .await
            .map_err(io::Error::other)?;
        Ok(old)
    }
    async fn delete_table(&mut self, table: &str) -> Result<(), io::Error> {
        if !table_exists(&self.conn, table).await? {
            return Ok(());
        }
        let sql = format!("DROP TABLE \"{}\"", table);
        self.conn
            .execute(sql.as_str(), ())
            .await
            .map_err(io::Error::other)?;
        Ok(())
    }
    async fn clear(&mut self) -> Result<(), io::Error> {
        let tables = self.table_names().await?;
        for table in tables {
            self.delete_table(&table).await?;
        }
        Ok(())
    }
    async fn commit(self) -> Result<(), io::Error> {
        self.conn
            .execute("COMMIT", ())
            .await
            .map_err(io::Error::other)?;
        Ok(())
    }
    async fn abort(self) -> Result<(), io::Error> {
        self.conn
            .execute("ROLLBACK", ())
            .await
            .map_err(io::Error::other)?;
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncTransactionalKVDB for SqliteDB {
    type ReadTransaction<'a> = ReadTransaction<'a>;
    type WriteTransaction<'a> = WriteTransaction<'a>;
    async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        Ok(ReadTransaction {
            conn: self.conn.clone(),
            _read_guard: self.rw_lock.read().await,
        })
    }
    async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        self.conn
            .execute("BEGIN CONCURRENT", ())
            .await
            .map_err(io::Error::other)?;
        Ok(WriteTransaction {
            conn: self.conn.clone(),
            _write_guard: self.rw_lock.write().await,
        })
    }
}
