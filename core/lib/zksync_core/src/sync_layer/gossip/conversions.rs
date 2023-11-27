//! Conversion logic between server and consensus types.

use anyhow::Context as _;

use zksync_consensus_roles::validator::{BlockHeader, BlockNumber, FinalBlock};
use zksync_types::{
    api::en::SyncBlock, block::ConsensusBlockFields, MiniblockNumber, ProtocolVersionId,
};

use crate::{consensus, sync_layer::fetcher::FetchedBlock};

impl FetchedBlock {
    pub(super) fn from_gossip_block(
        block: &FinalBlock,
        last_in_batch: bool,
    ) -> anyhow::Result<Self> {
        let number = u32::try_from(block.header.number.0)
            .context("Integer overflow converting block number")?;
        let payload = consensus::Payload::decode(&block.payload)
            .context("Failed deserializing block payload")?;

        Ok(Self {
            number: MiniblockNumber(number),
            l1_batch_number: payload.l1_batch_number,
            last_in_batch,
            protocol_version: ProtocolVersionId::latest(), // FIXME
            timestamp: payload.timestamp,
            hash: payload.hash,
            l1_gas_price: payload.l1_gas_price,
            l2_fair_gas_price: payload.l2_fair_gas_price,
            virtual_blocks: payload.virtual_blocks,
            operator_address: payload.operator_address,
            transactions: payload.transactions,
            consensus: Some(ConsensusBlockFields {
                parent: block.header.parent,
                justification: block.justification.clone(),
            }),
        })
    }
}
