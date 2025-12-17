use bincode::{Decode, Encode};

#[cfg(feature = "async")]
mod async_kvdb;
mod kvdb;

#[cfg(feature = "async")]
pub use async_kvdb::*;
pub use kvdb::*;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct VersionedObject {
    pub value: Vec<u8>,
    pub version: u64,
}

#[cfg(feature = "transactional")]
mod transactional;

#[cfg(feature = "transactional")]
pub use transactional::*;
