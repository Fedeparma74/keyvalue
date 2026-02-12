//! Versioned key-value storage.
//!
//! This module extends the base key-value abstraction with per-entry version
//! tracking. Every stored value is wrapped in a [`VersionedObject`] that pairs
//! the raw bytes with a monotonically increasing `u64` version number.
//!
//! A versioned "delete" sets the value to `None` while preserving (and
//! incrementing) the version, which is useful for conflict detection and
//! synchronisation protocols. For permanent removal, use
//! [`remove()`](crate::VersionedKeyValueDB::remove) or
//! [`delete_table(_, true)`](crate::VersionedKeyValueDB::delete_table).
//!
//! Encoding helpers [`encode`] and [`decode`] serialise `VersionedObject` to
//! a compact binary format suitable for storage in any `KeyValueDB` backend.

use crate::io;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[cfg(feature = "async")]
mod async_kvdb;
mod kvdb;

#[cfg(feature = "async")]
pub use async_kvdb::*;
pub use kvdb::*;

/// A value paired with its version number.
///
/// When `value` is `None` the entry is a *tombstone*: it has been logically
/// deleted but the version is preserved for synchronisation purposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedObject {
    pub value: Option<Vec<u8>>,
    pub version: u64,
}

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use transactional::*;

/// Encodes a [`VersionedObject`] into a compact binary representation.
///
/// Format: `[tag:1][version:8][len:4][value:len]`
///   - `tag = 0` — tombstone (`None` value), no length/value fields.
///   - `tag = 1` — live entry, followed by a little-endian `u32` length and
///     the raw value bytes.
pub fn encode(obj: &VersionedObject) -> Result<Vec<u8>, io::Error> {
    let value_len = obj.value.as_ref().map(|v| v.len()).unwrap_or(0);

    let mut buf = Vec::with_capacity(
        1 + 8
            + if obj.value.is_some() {
                4 + value_len
            } else {
                0
            },
    );

    match &obj.value {
        None => {
            buf.push(0);
            buf.extend_from_slice(&obj.version.to_le_bytes());
        }
        Some(v) => {
            let len: u32 = v.len().try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "value too large to encode (exceeds u32::MAX)",
                )
            })?;
            buf.push(1);
            buf.extend_from_slice(&obj.version.to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(v);
        }
    }

    Ok(buf)
}

/// Decodes bytes produced by [`encode`] back into a [`VersionedObject`].
///
/// Returns an error if the buffer is too small, contains an invalid tag,
/// or has trailing bytes.
pub fn decode(bytes: &[u8]) -> io::Result<VersionedObject> {
    let mut cursor = bytes;

    // value_tag + version
    if cursor.len() < 1 + 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "buffer too small",
        ));
    }

    let value_tag = cursor[0];
    cursor = &cursor[1..];

    let version = u64::from_le_bytes(
        cursor[..8]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid version bytes"))?,
    );
    cursor = &cursor[8..];

    let value = match value_tag {
        0 => {
            if !cursor.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "trailing bytes after versioned object (None variant)",
                ));
            }
            None
        }
        1 => {
            if cursor.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "missing value length",
                ));
            }

            let len =
                u32::from_le_bytes(cursor[..4].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid length bytes")
                })?) as usize;

            cursor = &cursor[4..];

            if cursor.len() < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated value",
                ));
            }

            let val = cursor[..len].to_vec();
            cursor = &cursor[len..];

            if !cursor.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "trailing bytes after versioned object (Some variant)",
                ));
            }

            Some(val)
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid value tag",
            ));
        }
    };

    Ok(VersionedObject { version, value })
}

#[test]
fn roundtrip_some() {
    let obj = VersionedObject {
        version: 42,
        value: Some(b"hello".to_vec()),
    };

    let encoded = encode(&obj).unwrap();
    let decoded = decode(&encoded).unwrap();

    assert_eq!(obj, decoded);
}

#[test]
fn roundtrip_none() {
    let obj = VersionedObject {
        version: 7,
        value: None,
    };

    let encoded = encode(&obj).unwrap();
    let decoded = decode(&encoded).unwrap();

    assert_eq!(obj, decoded);
}

#[test]
fn reject_garbage() {
    assert!(decode(&[1, 2, 3]).is_err());
}

#[test]
fn reject_trailing_bytes() {
    let obj = VersionedObject {
        version: 1,
        value: Some(b"hi".to_vec()),
    };
    let mut encoded = encode(&obj).unwrap();
    encoded.push(0xFF);
    assert!(decode(&encoded).is_err());
}
