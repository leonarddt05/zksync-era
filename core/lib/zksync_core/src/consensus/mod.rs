//! Consensus-related functionality.
mod payload;
mod proto;
mod storage;

pub(crate) use self::payload::Payload;
pub(crate) use self::storage::sync_block_to_consensus_block;
