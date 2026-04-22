//! AWS S3 (or S3-compatible) key-value store (async-only).
//!
//! Tables are represented as **key prefixes** within a single S3 bucket:
//! an entry with table `"users"` and key `"alice"` is stored as the S3
//! object `users/alice`. This means that neither table names nor keys may
//! contain the `/` character.
//!
//! This backend is suitable for durable, cloud-native storage but has
//! higher latency per operation compared to local embedded databases.

use std::{collections::HashSet, io};

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
pub use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, operation::get_object::GetObjectError, primitives::ByteStream};

use crate::AsyncKeyValueDB;

mod client;

use self::client::{HttpClientImpl, SleepImpl, TimeSourceImpl};

/// Configuration for the AWS S3 backend.
#[derive(Debug, Clone)]
pub struct AwsS3Config {
    /// S3 endpoint URL (e.g. `"http://localhost:9000"` for MinIO).
    pub endpoint_url: String,
    /// AWS region name.
    pub region: String,
    /// Access key / secret key pair.
    pub credentials: Credentials,
    /// Name of the bucket to use (created if absent).
    pub bucket_name: String,
}

impl AwsS3Config {
    /// Creates a new S3 configuration.
    pub fn new(
        endpoint_url: impl Into<String>,
        region: impl Into<String>,
        credentials: Credentials,
        bucket_name: impl Into<String>,
    ) -> Self {
        Self {
            endpoint_url: endpoint_url.into(),
            region: region.into(),
            credentials,
            bucket_name: bucket_name.into(),
        }
    }
}

/// Async key-value database backed by AWS S3 (or any S3-compatible service).
///
/// Created via [`AwsS3DB::open`], which takes endpoint, region, credentials,
/// and a bucket name. The bucket is created automatically if it doesn't exist.
#[derive(Debug)]
pub struct AwsS3DB {
    client: Client,
    bucket_name: String,
}

impl AwsS3DB {
    /// Connects to the S3-compatible service and ensures the bucket exists.
    pub async fn open_with_config(config: AwsS3Config) -> io::Result<Self> {
        Self::open(
            &config.endpoint_url,
            &config.region,
            config.credentials,
            &config.bucket_name,
        )
        .await
    }

    /// Connects to the S3-compatible service and ensures the bucket exists.
    pub async fn open(
        endpoint_url: &str,
        region: &str,
        credentials: Credentials,
        bucket_name: &str,
    ) -> io::Result<Self> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .sleep_impl(SleepImpl)
            .region(Region::new(region.to_string()))
            .time_source(TimeSourceImpl)
            .endpoint_url(endpoint_url)
            .credentials_provider(credentials)
            .http_client(HttpClientImpl)
            .load()
            .await;

        let client = Client::new(&config);

        let buckets = client
            .list_buckets()
            .send()
            .await
            .map_err(|e| io::Error::other(format!("{:?}", e)))?
            .buckets
            .unwrap_or_default();

        if !buckets
            .iter()
            .any(|bucket| bucket.name().unwrap_or_default() == bucket_name)
        {
            client
                .create_bucket()
                .bucket(bucket_name)
                .send()
                .await
                .map_err(|e| io::Error::other(format!("Failed to create bucket: {:?}", e)))?;
        }

        Ok(Self {
            client,
            bucket_name: bucket_name.to_string(),
        })
    }
}

/// Validates that a name does not contain `/`, which would break the `table/key` S3 key format.
fn validate_name(kind: &str, name: &str) -> Result<(), io::Error> {
    if name.contains('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{kind} must not contain '/'"),
        ));
    }
    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{kind} must not be empty"),
        ));
    }
    Ok(())
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncKeyValueDB for AwsS3DB {
    /// Note: the get-then-put pattern is not atomic. Under concurrent access,
    /// the returned "old value" may be stale due to inherent S3 eventual consistency.
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        validate_name("table name", table_name)?;
        validate_name("key", key)?;
        let old_value = self.get(table_name, key).await?;

        let table_key = format!("{}/{}", table_name, key);

        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&table_key)
            .body(ByteStream::from(value.to_vec()))
            .send()
            .await
            .map_err(|e| io::Error::other(format!("{:?}", e)))?;

        Ok(old_value)
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        validate_name("table name", table_name)?;
        validate_name("key", key)?;
        let table_key = format!("{}/{}", table_name, key);

        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&table_key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                if let Some(GetObjectError::NoSuchKey(_)) = e.as_service_error() {
                    return Ok(None);
                } else {
                    return Err(io::Error::other(format!("{:?}", e)));
                }
            }
        };

        let data = output.body.collect().await.map_err(io::Error::other)?;

        Ok(Some(data.to_vec()))
    }

    /// Note: the get-then-delete pattern is not atomic. Under concurrent access,
    /// the returned "old value" may be stale due to inherent S3 eventual consistency.
    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        validate_name("table name", table_name)?;
        validate_name("key", key)?;
        let old_value = self.get(table_name, key).await?;

        if old_value.is_some() {
            let table_key = format!("{}/{}", table_name, key);

            self.client
                .delete_object()
                .bucket(&self.bucket_name)
                .key(&table_key)
                .send()
                .await
                .map_err(|e| io::Error::other(format!("{:?}", e)))?;
        }

        Ok(old_value)
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        validate_name("table name", table_name)?;
        let prefix = format!("{}/", table_name);

        let mut keys_and_values = Vec::new();

        let mut continuation_token = None;

        loop {
            let list_objects = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .prefix(&prefix);

            let list_objects = if let Some(token) = continuation_token {
                list_objects.continuation_token(token)
            } else {
                list_objects
            };

            let output = list_objects
                .send()
                .await
                .map_err(|e| io::Error::other(format!("{:?}", e)))?;

            for object in output.contents.unwrap_or_default() {
                let key = object.key.unwrap_or_default();

                let key = if let Some(key) = key.strip_prefix(&prefix) {
                    key
                } else {
                    continue;
                };

                if let Some(data) = self.get(table_name, key).await? {
                    keys_and_values.push((key.to_string(), data));
                }
            }

            if let Some(token) = output.next_continuation_token {
                continuation_token = Some(token);
            } else {
                break;
            }
        }

        Ok(keys_and_values)
    }

    async fn table_names(&self) -> Result<Vec<String>, io::Error> {
        let mut table_names = HashSet::new();

        let mut continuation_token = None;

        loop {
            let list_objects = self.client.list_objects_v2().bucket(&self.bucket_name);

            let list_objects = if let Some(token) = continuation_token {
                list_objects.continuation_token(token)
            } else {
                list_objects
            };

            let output = list_objects
                .send()
                .await
                .map_err(|e| io::Error::other(format!("{:?}", e)))?;

            for object in output.contents.unwrap_or_default() {
                if let Some(table_name) = object
                    .key
                    .unwrap_or_default()
                    .split_once('/')
                    .map(|(table_name, _)| table_name)
                {
                    table_names.insert(table_name.to_string());
                }
            }

            if let Some(token) = output.next_continuation_token {
                continuation_token = Some(token);
            } else {
                break;
            }
        }

        let mut result: Vec<String> = table_names.into_iter().collect();
        result.sort();
        Ok(result)
    }

    async fn iter_range(
        &self,
        table_name: &str,
        range: crate::KeyRange,
    ) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        validate_name("table name", table_name)?;

        // Reverse direction is not natively supported by S3's ListObjectsV2.
        // Fall back to the default in-memory implementation for that path.
        if range.direction == crate::Direction::Reverse {
            let items = AsyncKeyValueDB::iter(self, table_name).await?;
            return Ok(crate::apply_range_in_memory(items, &range));
        }

        let table_prefix = format!("{}/", table_name);

        // S3 `prefix` combines the table prefix with an optional user prefix.
        let list_prefix = match &range.prefix {
            Some(p) => format!("{}{}", table_prefix, p),
            None => table_prefix.clone(),
        };

        // `start_after` strictly skips the provided key.  We use it for
        // Excluded lower bounds; for Included lower bounds we still pass
        // the predecessor + fetch the exact key separately when the bound
        // key itself falls inside the prefix (handled below by including
        // it unconditionally after the scan).
        let start_after: Option<String> = match &range.lower {
            crate::Bound::Unbounded => None,
            crate::Bound::Included(k) => {
                // Emulate inclusivity by asking S3 to skip the byte *before* k:
                // start_after accepts any string, so we subtract a zero byte.
                // If no predecessor is representable, fall back to None and
                // rely on post-filtering.
                let full = format!("{}{}", table_prefix, k);
                let mut bytes = full.into_bytes();
                if let Some(last) = bytes.last_mut()
                    && *last > 0
                {
                    *last -= 1;
                    bytes.push(0xFF);
                }
                String::from_utf8(bytes).ok()
            }
            crate::Bound::Excluded(k) => Some(format!("{}{}", table_prefix, k)),
        };

        let limit = range.limit.unwrap_or(usize::MAX);
        let mut result: Vec<(String, Vec<u8>)> = Vec::new();
        let mut continuation_token: Option<String> = None;

        'outer: loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .prefix(&list_prefix);
            if let Some(sa) = &start_after {
                req = req.start_after(sa);
            }
            if limit != usize::MAX {
                // Cap per-page size; remaining will be fetched if needed.
                let remaining = limit.saturating_sub(result.len());
                let page = remaining.min(1000) as i32;
                if page > 0 {
                    req = req.max_keys(page);
                }
            }
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let output = req
                .send()
                .await
                .map_err(|e| io::Error::other(format!("{:?}", e)))?;

            for object in output.contents.unwrap_or_default() {
                let full_key = object.key.unwrap_or_default();
                let Some(key_in_table) = full_key.strip_prefix(&table_prefix) else {
                    continue;
                };
                let key_owned = key_in_table.to_string();
                if range.is_beyond_far_end(&key_owned) {
                    break 'outer;
                }
                if !range.contains(&key_owned) {
                    continue;
                }
                if let Some(data) = self.get(table_name, &key_owned).await? {
                    result.push((key_owned, data));
                    if result.len() >= limit {
                        break 'outer;
                    }
                }
            }

            match output.next_continuation_token {
                Some(token) => continuation_token = Some(token),
                None => break,
            }
        }

        Ok(result)
    }
}
