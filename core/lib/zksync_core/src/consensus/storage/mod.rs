//! Storage implementation based on DAL.
use anyhow::Context as _;
use async_trait::async_trait;

use std::ops;

use zksync_concurrency::{
    ctx,
    sync::{self, watch, Mutex},
    time,
};
use zksync_consensus_roles::validator;
use zksync_consensus_storage::{BlockStore, StorageError, StorageResult, WriteBlockStore};
use zksync_dal::{ConnectionPool, StorageProcessor};
use zksync_types::{api::en::SyncBlock, block::ConsensusBlockFields, Address, MiniblockNumber};

use crate::consensus;
use crate::sync_layer::{
    fetcher::{FetchedBlock, FetcherCursor},
    sync_action::{ActionQueueSender, SyncAction},
};

pub(super) fn sync_block_to_consensus_block(
    mut block: SyncBlock,
) -> anyhow::Result<validator::FinalBlock> {
    let number = validator::BlockNumber(block.number.0.into());
    let consensus = block.consensus.take().context("Missing consensus fields")?;
    let payload: consensus::Payload = block.try_into()?;
    let payload = payload.encode();
    Ok(validator::FinalBlock {
        header: BlockHeader {
            parent: consensus.parent,
            number,
            payload: payload.hash(),
        },
        payload,
        justification: consensus.justification,
    })
}

async fn storage(ctx: &ctx::Ctx, pool: &ConnectionPool) -> StorageResult<StorageProcessor<'_>> {
    ctx.wait(pool.access_storage_tagged("sync_layer"))
        .await?
        .context("Failed to connect to Postgres")
}

async fn transaction_begin(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'_>,
) -> anyhow::Result<StorageProcessor<'_>> {
    Ok(ctx.wait(storage.transaction_begin()).await??)
}

async fn commit(ctx: &ctx::Ctx, txn: StorageProcessor<'_>) -> anyhow::Result<()> {
    Ok(ctx.wait(txn.commit()).await??)
}

async fn fetch_block(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'_>,
    number: validator::BlockNumber,
) -> StorageResult<Option<validator::FinalBlock>> {
    let number = MiniblockNumber(number.0.try_into()?);
    let Some(block) = ctx
        .wrap(
            storage
                .sync_dal()
                .sync_block(number, Address::default(), true),
        )
        .await??
    else {
        return Ok(None);
    };
    if block.consensus.is_none() {
        return Ok(None);
    }
    Ok(Some(sync_block_to_consensus_block(block)?))
}

async fn verify_payload(
    ctx: &ctx,
    storage: &mut StorageProcessor<'_>,
    block_number: validator::BlockNumber,
    payload: &validator::Payload,
) -> anyhow::Result<()> {
    let n = MiniblockNumber(block.header.number.0.try_into()?);
    let sync_block = ctx
        .wait(txn.sync_dal().sync_block(n, Address::default(), true))
        .await??;
    let payload: consensus::Payload = sync_block.try_into()?;
    anyhow::ensure!(payload == block.payload);
    Ok(())
}

async fn put_block(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'_>,
    block: &validator::FinalBlock,
) -> anyhow::Result<()> {
    let n = MiniblockNumber(block.header.number.0.try_into()?);
    let mut txn = transaction_begin(ctx, storage).await?;

    // We require the block to be already stored in Postgres when we set the consensus field.
    let sync_block = ctx
        .wait(txn.sync_dal().sync_block(n, Address::default(), true))
        .await??;
    let payload: consensus::Payload = sync_block.try_into()?;
    anyhow::ensure!(payload == block.payload);

    let want = &ConsensusBlockFields {
        parent: block.header.parent,
        justification: block.justification.clone(),
    };
    // Early exit if consensus field is already set to the expected value.
    if Some(want) == sync_block.consensus.as_ref() {
        return Ok(());
    }
    ctx.wait(txn.blocks_dal().set_miniblock_consensus_fields(n, want))
        .await?
        .context("set_miniblock_consensus_fields()")?;
    commit(ctx, txn).await?;
    Ok(())
}

async fn find_head_number(
    ctx: &ctx::Ctx,
    storage: &StorageProcessor<'_>,
) -> anyhow::Result<validator::BlockNumber> {
    ctx.wrap(
        storage
            .blocks_dal()
            .get_last_miniblock_number_with_consensus_fields(),
    )
    .await??;
}

async fn find_head_forward(
    ctx: &ctx::Ctx,
    storage: &StorageProcessor<'_>,
    start_at: validator::BlockNumber,
) -> StorageResult<Option<validator::FinalBlock>> {
    let Some(mut block) = fetch_block(ctx, storage, start_at).await? else {
        return Ok(None);
    };
    while let Some(next) = fetch_block(ctx, storage, block.header.number.next()).await? {
        block = next;
    }
    Ok(Some(block))
}

/// Postgres-based [`BlockStore`] implementation, which
/// considers blocks as stored iff they have consensus field set.
#[derive(Debug)]
pub(super) struct SignedBlockStore {
    genesis: validator::BlockNumber,
    // TODO(gprusak): wrap in a mutex.
    head: watch::Sender<validator::BlockNumber>,
    pool: ConnectionPool,
}

impl SignedBlockStore {
    /// Creates a new storage handle. `pool` should have multiple connections to work efficiently.
    pub async fn new(
        ctx: &ctx::Ctx,
        pool: ConnectionPool,
        genesis: &validator::FinalBlock,
    ) -> anyhow::Result<Self> {
        // Ensure that genesis block has consensus field set in postgres.
        let storage = &mut storage(ctx, pool).await?;
        put_block(ctx, storage, genesis).await?;

        // Find the last miniblock with consensus field set (aka head).
        // We assume here that all blocks in range (genesis,head) also have consensus field set.
        // WARNING: genesis should NEVER be moved to an earlier block.
        let head = find_head_number(ctx, storage).await?;
        Ok(Self {
            genesis: genesis.header.number,
            head: watch::channel(head).0,
            pool,
        })
    }
}

impl WriteBlockStore for SignedBlockStore {
    /// Verify that `payload` is a correct proposal for the block `block_number`.
    async fn verify_payload(
        &self,
        ctx: &ctx::Ctx,
        block_number: validator::BlockNumber,
        payload: &validator::Payload,
    ) -> anyhow::Result<()> {
        Ok(verify_payload(
            ctx,
            &mut storage(ctx, &self.pool).await?,
            block_number,
            payload,
        )
        .await?)
    }
    /// Puts a block into this storage.
    async fn put_block(&self, ctx: &ctx::Ctx, block: &validator::FinalBlock) -> StorageResult<()> {
        Ok(put_block(ctx, &mut storage(ctx, &self.pool).await?, block).await?)
    }
}

#[async_trait]
impl BlockStore for SignedBlockStore {
    async fn head_block(&self, ctx: &ctx::Ctx) -> StorageResult<validator::FinalBlock> {
        let head = *self.head.borrow();
        find_head_forward(ctx, head).await
    }

    async fn first_block(&self, ctx: &ctx::Ctx) -> StorageResult<validator::FinalBlock> {
        let mut storage = self.storage().await?;
        fetch_block(ctx, &mut storage, self.genesis)
            .await
            .context("Genesis miniblock not present in Postgres")
    }

    async fn last_contiguous_block_number(
        &self,
        ctx: &ctx::Ctx,
    ) -> StorageResult<validator::BlockNumber> {
        Ok(self.head_block(ctx).await?.header.number)
    }

    async fn block(
        &self,
        ctx: &ctx::Ctx,
        number: validator::BlockNumber,
    ) -> StorageResult<Option<validator::FinalBlock>> {
        fetch_block(ctx, &mut self.storage().await?, number).await
    }

    async fn missing_block_numbers(
        &self,
        _ctx: &ctx::Ctx,
        _range: ops::Range<validator::BlockNumber>,
    ) -> StorageResult<Vec<validator::BlockNumber>> {
        Ok(vec![]) // The storage never has missing blocks by construction
    }

    fn subscribe_to_block_writes(&self) -> watch::Receiver<validator::BlockNumber> {
        self.head.subscribe()
    }
}
