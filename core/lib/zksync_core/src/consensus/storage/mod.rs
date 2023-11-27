//! Storage implementation based on DAL.
#![allow(unused)]
use crate::consensus;
use anyhow::Context as _;
use std::ops;
use zksync_concurrency::{ctx, sync};
use zksync_consensus_roles::validator;
use zksync_consensus_storage::{BlockStore, StorageResult, WriteBlockStore};
use zksync_dal::{ConnectionPool, StorageProcessor};
use zksync_types::{api::en::SyncBlock, block::ConsensusBlockFields, Address, MiniblockNumber};

pub(crate) fn sync_block_to_consensus_block(
    mut block: SyncBlock,
) -> anyhow::Result<validator::FinalBlock> {
    let number = validator::BlockNumber(block.number.0.into());
    let consensus = block.consensus.take().context("Missing consensus fields")?;
    let payload: consensus::Payload = block.try_into()?;
    let payload = payload.encode();
    Ok(validator::FinalBlock {
        header: validator::BlockHeader {
            parent: consensus.parent,
            number,
            payload: payload.hash(),
        },
        payload,
        justification: consensus.justification,
    })
}

async fn storage<'a>(
    ctx: &ctx::Ctx,
    pool: &'a ConnectionPool,
) -> anyhow::Result<StorageProcessor<'a>> {
    Ok(ctx.wait(pool.access_storage_tagged("sync_layer")).await??)
}

async fn start_transaction<'a, 'b, 'c: 'b>(
    ctx: &ctx::Ctx,
    storage: &'c mut StorageProcessor<'a>,
) -> anyhow::Result<StorageProcessor<'b>> {
    Ok(ctx.wait(storage.start_transaction()).await??)
}

async fn commit<'a>(ctx: &ctx::Ctx, txn: StorageProcessor<'a>) -> anyhow::Result<()> {
    Ok(ctx.wait(txn.commit()).await??)
}

async fn fetch_block<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    number: validator::BlockNumber,
) -> anyhow::Result<Option<validator::FinalBlock>> {
    let number = MiniblockNumber(number.0.try_into()?);
    let Some(block) = ctx
        .wait(
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

async fn verify_payload<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    block_number: validator::BlockNumber,
    payload: &validator::Payload,
) -> anyhow::Result<()> {
    let n = MiniblockNumber(block_number.0.try_into()?);
    let sync_block = ctx
        .wait(storage.sync_dal().sync_block(n, Address::default(), true))
        .await??
        .context("unexpected payload")?;
    let got: consensus::Payload = sync_block.try_into()?;
    anyhow::ensure!(payload == &got.encode());
    Ok(())
}

async fn put_block<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    block: &validator::FinalBlock,
) -> anyhow::Result<()> {
    let n = MiniblockNumber(block.header.number.0.try_into()?);
    let mut txn = start_transaction(ctx, storage).await?;

    // We require the block to be already stored in Postgres when we set the consensus field.
    let sync_block = ctx
        .wait(txn.sync_dal().sync_block(n, Address::default(), true))
        .await??
        .context("unexpected payload")?;
    let want = &ConsensusBlockFields {
        parent: block.header.parent,
        justification: block.justification.clone(),
    };

    // Early exit if consensus field is already set to the expected value.
    if Some(want) == sync_block.consensus.as_ref() {
        return Ok(());
    }

    // Verify that the payload matches the storage.
    let payload: consensus::Payload = sync_block.try_into()?;
    anyhow::ensure!(payload.encode() == block.payload);

    ctx.wait(txn.blocks_dal().set_miniblock_consensus_fields(n, want))
        .await?
        .context("set_miniblock_consensus_fields()")?;
    commit(ctx, txn).await?;
    Ok(())
}

async fn find_head_number<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
) -> anyhow::Result<validator::BlockNumber> {
    let head = ctx
        .wait(
            storage
                .blocks_dal()
                .get_last_miniblock_number_with_consensus_fields(),
        )
        .await??
        .context("head not found")?;
    Ok(validator::BlockNumber(head.0.into()))
}

async fn find_head_forward<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    start_at: validator::BlockNumber,
) -> anyhow::Result<Option<validator::FinalBlock>> {
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
    head: sync::watch::Sender<validator::BlockNumber>,
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
        let head = {
            let mut storage = storage(ctx, &pool).await?;

            put_block(ctx, &mut storage, genesis).await?;

            // Find the last miniblock with consensus field set (aka head).
            // We assume here that all blocks in range (genesis,head) also have consensus field set.
            // WARNING: genesis should NEVER be moved to an earlier block.
            find_head_number(ctx, &mut storage).await?
        };
        Ok(Self {
            genesis: genesis.header.number,
            head: sync::watch::channel(head).0,
            pool,
        })
    }
}

#[async_trait::async_trait]
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

#[async_trait::async_trait]
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

    fn subscribe_to_block_writes(&self) -> sync::watch::Receiver<validator::BlockNumber> {
        self.head.subscribe()
    }
}
