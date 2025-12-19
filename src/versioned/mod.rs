use std::io;

#[cfg(feature = "async")]
mod async_kvdb;
mod kvdb;

#[cfg(feature = "async")]
pub use async_kvdb::*;
pub use kvdb::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedObject {
    pub value: Option<Vec<u8>>,
    pub version: u64,
}

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use transactional::*;

pub fn encode(obj: &VersionedObject) -> Vec<u8> {
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
            buf.push(1);
            buf.extend_from_slice(&obj.version.to_le_bytes());
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
    }

    buf
}

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
        0 => None,
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

            Some(cursor[..len].to_vec())
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

    let encoded = encode(&obj);
    let decoded = decode(&encoded).unwrap();

    assert_eq!(obj, decoded);
}

#[test]
fn roundtrip_none() {
    let obj = VersionedObject {
        version: 7,
        value: None,
    };

    let encoded = encode(&obj);
    let decoded = decode(&encoded).unwrap();

    assert_eq!(obj, decoded);
}

#[test]
fn reject_garbage() {
    assert!(decode(&[1, 2, 3]).is_err());
}
