use std::{collections::HashSet, io};

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
pub use aws_credential_types::Credentials;
use aws_sdk_s3::{operation::get_object::GetObjectError, primitives::ByteStream, Client};

use crate::AsyncKeyValueDB;

mod client;

use self::client::{HttpClientImpl, SleepImpl, TimeSourceImpl};

#[derive(Debug)]
pub struct AwsS3DB {
    client: Client,
    bucket_name: String,
}

impl AwsS3DB {
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
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?
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
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to create bucket: {:?}", e),
                    )
                })?;
        }

        Ok(Self {
            client,
            bucket_name: bucket_name.to_string(),
        })
    }
}

#[cfg_attr(all(not(target_arch = "wasm32"), feature = "std"), async_trait)]
#[cfg_attr(any(target_arch = "wasm32", not(feature = "std")), async_trait(?Send))]
impl AsyncKeyValueDB for AwsS3DB {
    async fn insert(
        &self,
        table_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, io::Error> {
        let old_value = self.get(table_name, key).await?;

        let table_key = format!("{}/{}", table_name, key);

        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&table_key)
            .body(ByteStream::from(value.to_vec()))
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

        Ok(old_value)
    }

    async fn get(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
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
                    return Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e)));
                }
            }
        };

        let data = output
            .body
            .collect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(Some(data.to_vec()))
    }

    async fn remove(&self, table_name: &str, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
        let old_value = self.get(table_name, key).await?;

        let table_key = format!("{}/{}", table_name, key);

        self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(&table_key)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

        Ok(old_value)
    }

    async fn iter(&self, table_name: &str) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
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
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

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
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

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

        Ok(table_names.into_iter().collect())
    }
}
