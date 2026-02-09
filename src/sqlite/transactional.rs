use std::{io, sync::Arc};

use async_trait::async_trait;
use turso::Connection;

use crate::{
    AsyncKVReadTransaction, AsyncKVWriteTransaction, AsyncTransactionalKVDB,
    sqlite::{ensure_table, table_exists, validate_table_name},
};

use super::SqliteDB;

pub struct ReadTransaction {
    conn: Connection,
}
pub struct WriteTransaction {
    conn: Arc<Connection>,
}
#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a> AsyncKVReadTransaction<'a> for ReadTransaction {
    async fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
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
        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let blob: Vec<u8> = row.get(0).map_err(io::Error::other)?;
            Ok(Some(blob))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        validate_table_name(table)?;
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
        while let Some(row) = stream.next().await.map_err(io::Error::other)? {
            let key: String = row.get(0).map_err(io::Error::other)?;
            let val: Vec<u8> = row.get(1).map_err(io::Error::other)?;
            result.push((key, val));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            .await
            .map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let name: String = row.get(0).map_err(io::Error::other)?;
            out.push(name);
        }
        Ok(out)
    }
}

impl ReadTransaction {
    /// End the read transaction, releasing the snapshot.
    pub async fn end(self) -> Result<(), io::Error> {
        self.conn
            .execute("COMMIT", ())
            .await
            .map_err(io::Error::other)?;
        Ok(())
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a> AsyncKVReadTransaction<'a> for WriteTransaction {
    async fn get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
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
        if let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let blob: Vec<u8> = row.get(0).map_err(io::Error::other)?;
            Ok(Some(blob))
        } else {
            Ok(None)
        }
    }
    async fn iter(&self, table: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        validate_table_name(table)?;
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
        while let Some(row) = stream.next().await.map_err(io::Error::other)? {
            let key: String = row.get(0).map_err(io::Error::other)?;
            let val: Vec<u8> = row.get(1).map_err(io::Error::other)?;
            result.push((key, val));
        }
        Ok(result)
    }
    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            .await
            .map_err(io::Error::other)?;
        let mut rows = stmt.query(()).await.map_err(io::Error::other)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(io::Error::other)? {
            let name: String = row.get(0).map_err(io::Error::other)?;
            out.push(name);
        }
        Ok(out)
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl<'a> AsyncKVWriteTransaction<'a> for WriteTransaction {
    async fn insert(
        &mut self,
        table: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        validate_table_name(table)?;
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
        validate_table_name(table)?;
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
        validate_table_name(table)?;
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
    type ReadTransaction<'a> = ReadTransaction;
    type WriteTransaction<'a> = WriteTransaction;
    async fn begin_read(&self) -> Result<Self::ReadTransaction<'_>, io::Error> {
        // Open a separate connection for read isolation.
        // With WAL mode, BEGIN DEFERRED gives us a snapshot as of the first read.
        let read_conn = self.db.connect().map_err(io::Error::other)?;
        read_conn
            .execute("BEGIN DEFERRED", ())
            .await
            .map_err(io::Error::other)?;
        Ok(ReadTransaction { conn: read_conn })
    }
    async fn begin_write(&self) -> Result<Self::WriteTransaction<'_>, io::Error> {
        self.conn
            .execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(io::Error::other)?;
        Ok(WriteTransaction {
            conn: self.conn.clone(),
        })
    }
}
