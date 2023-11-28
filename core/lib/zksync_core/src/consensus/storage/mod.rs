//! Storage implementation based on DAL.
use crate::consensus;
use crate::consensus::wrap_error::WrapError;
use anyhow::Context as _;
use std::ops;
use zksync_concurrency::{ctx, sync, time};
use zksync_consensus_bft::PayloadSource;
use zksync_consensus_roles::validator;
use zksync_consensus_storage::{
    BlockStore, ReplicaState, ReplicaStateStore, StorageError, StorageResult, WriteBlockStore,
};
use zksync_dal::{ConnectionPool, StorageProcessor};
use zksync_types::{api::en::SyncBlock, block::ConsensusBlockFields, Address, MiniblockNumber};

impl WrapError for StorageError {
    fn with_wrap<C: std::fmt::Display + Send + Sync + 'static, F: FnOnce() -> C>(
        self,
        f: F,
    ) -> Self {
        match self {
            Self::Database(err) => Self::Database(err.with_wrap(f)),
            err => err,
        }
    }
}

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
) -> StorageResult<StorageProcessor<'a>> {
    ctx.wait(pool.access_storage_tagged("sync_layer"))
        .await?
        .map_err(StorageError::Database)
}

async fn start_transaction<'a, 'b, 'c: 'b>(
    ctx: &ctx::Ctx,
    storage: &'c mut StorageProcessor<'a>,
) -> Result<StorageProcessor<'b>, StorageError> {
    ctx.wait(storage.start_transaction())
        .await?
        .map_err(|err| StorageError::Database(err.into()))
}

async fn commit<'a>(ctx: &ctx::Ctx, txn: StorageProcessor<'a>) -> StorageResult<()> {
    ctx.wait(txn.commit())
        .await?
        .map_err(|err| StorageError::Database(err.into()))
}

async fn fetch_block<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    number: validator::BlockNumber,
) -> StorageResult<Option<validator::FinalBlock>> {
    let number = MiniblockNumber(
        number
            .0
            .try_into()
            .context("MiniblockNumber")
            .map_err(StorageError::Database)?,
    );
    let Some(block) = ctx
        .wait(
            storage
                .sync_dal()
                .sync_block(number, Address::default(), true),
        )
        .await?
        .context("sync_block()")
        .map_err(StorageError::Database)?
    else {
        return Ok(None);
    };
    if block.consensus.is_none() {
        return Ok(None);
    }
    Ok(Some(
        sync_block_to_consensus_block(block)
            .context("sync_block_to_consensus_block()")
            .map_err(StorageError::Database)?,
    ))
}

async fn fetch_payload<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    block_number: validator::BlockNumber,
) -> StorageResult<Option<consensus::Payload>> {
    let n = MiniblockNumber(
        block_number
            .0
            .try_into()
            .context("MiniblockNumber")
            .map_err(StorageError::Database)?,
    );
    let Some(sync_block) = ctx
        .wait(storage.sync_dal().sync_block(n, Address::default(), true))
        .await?
        .context("sync_block()")
        .map_err(StorageError::Database)?
    else {
        return Ok(None);
    };
    Ok(Some(sync_block.try_into().map_err(StorageError::Database)?))
}

async fn put_block<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    block: &validator::FinalBlock,
) -> StorageResult<()> {
    let n = MiniblockNumber(
        block
            .header
            .number
            .0
            .try_into()
            .context("MiniblockNumber")
            .map_err(StorageError::Database)?,
    );
    let mut txn = start_transaction(ctx, storage)
        .await
        .wrap("start_transaction()")?;

    // We require the block to be already stored in Postgres when we set the consensus field.
    let sync_block = ctx
        .wait(txn.sync_dal().sync_block(n, Address::default(), true))
        .await?
        .context("sync_block()")
        .map_err(StorageError::Database)?
        .context("unknown block")
        .map_err(StorageError::Database)?;
    let want = &ConsensusBlockFields {
        parent: block.header.parent,
        justification: block.justification.clone(),
    };

    // Early exit if consensus field is already set to the expected value.
    if Some(want) == sync_block.consensus.as_ref() {
        return Ok(());
    }

    // Verify that the payload matches the storage.
    let payload: consensus::Payload = sync_block.try_into().map_err(StorageError::Database)?;
    if payload.encode() != block.payload {
        return Err(StorageError::Database(anyhow::anyhow!("payload mismatch")));
    }

    ctx.wait(txn.blocks_dal().set_miniblock_consensus_fields(n, want))
        .await?
        .wrap("set_miniblock_consensus_fields()")
        .map_err(StorageError::Database)?;
    commit(ctx, txn).await.wrap("commit()")?;
    Ok(())
}

async fn find_head_number<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
) -> StorageResult<validator::BlockNumber> {
    let head = ctx
        .wait(
            storage
                .blocks_dal()
                .get_last_miniblock_number_with_consensus_fields(),
        )
        .await?
        .context("get_last_miniblock_number_with_consensus_fields()")
        .map_err(StorageError::Database)?
        .context("head not found")
        .map_err(StorageError::Database)?;
    Ok(validator::BlockNumber(head.0.into()))
}

async fn find_head_forward<'a>(
    ctx: &ctx::Ctx,
    storage: &mut StorageProcessor<'a>,
    start_at: validator::BlockNumber,
) -> StorageResult<validator::FinalBlock> {
    let mut block = fetch_block(ctx, storage, start_at)
        .await
        .wrap("fetch_block()")?
        .context("head not found")
        .map_err(StorageError::Database)?;
    while let Some(next) = fetch_block(ctx, storage, block.header.number.next())
        .await
        .wrap("fetch_block()")?
    {
        block = next;
    }
    Ok(block)
}

/// Postgres-based [`BlockStore`] implementation, which
/// considers blocks as stored iff they have consensus field set.
#[derive(Debug)]
pub(super) struct SignedBlockStore {
    genesis: validator::BlockNumber,
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
            let mut storage = storage(ctx, &pool).await.wrap("storage()")?;

            put_block(ctx, &mut storage, genesis)
                .await
                .wrap("put_block()")?;

            // Find the last miniblock with consensus field set (aka head).
            // We assume here that all blocks in range (genesis,head) also have consensus field set.
            // WARNING: genesis should NEVER be moved to an earlier block.
            find_head_number(ctx, &mut storage)
                .await
                .wrap("find_head_number()")?
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
        let storage = &mut storage(ctx, &self.pool).await.context("storage()")?;
        let want = fetch_payload(ctx, storage, block_number)
            .await
            .wrap("fetch_payload()")?
            .context("unknown block")?;
        anyhow::ensure!(payload == &want.encode());
        Ok(())
    }

    /// Puts a block into this storage.
    async fn put_block(&self, ctx: &ctx::Ctx, block: &validator::FinalBlock) -> StorageResult<()> {
        let storage = &mut storage(ctx, &self.pool).await.wrap("storage()")?;
        put_block(ctx, storage, block).await.wrap("put_block()")
    }
}

#[async_trait::async_trait]
impl BlockStore for SignedBlockStore {
    async fn head_block(&self, ctx: &ctx::Ctx) -> StorageResult<validator::FinalBlock> {
        let storage = &mut storage(ctx, &self.pool).await.wrap("storage()")?;
        let head = *self.head.borrow();
        find_head_forward(ctx, storage, head)
            .await
            .wrap("fing_head_forward()")
    }

    async fn first_block(&self, ctx: &ctx::Ctx) -> StorageResult<validator::FinalBlock> {
        let storage = &mut storage(ctx, &self.pool).await.wrap("storage()")?;
        fetch_block(ctx, storage, self.genesis)
            .await
            .wrap("fetch_block()")?
            .context("Genesis miniblock not present in Postgres")
            .map_err(StorageError::Database)
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
        let storage = &mut storage(ctx, &self.pool).await.wrap("storage()")?;
        fetch_block(ctx, storage, number)
            .await
            .wrap("fetch_block()")
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

#[async_trait::async_trait]
impl ReplicaStateStore for SignedBlockStore {
    async fn replica_state(&self, ctx: &ctx::Ctx) -> StorageResult<Option<ReplicaState>> {
        let storage = &mut storage(ctx, &self.pool).await.wrap("storage")?;
        ctx.wait(storage.consensus_dal().replica_state())
            .await?
            .context("replica_state()")
            .map_err(StorageError::Database)
    }

    async fn put_replica_state(
        &self,
        ctx: &ctx::Ctx,
        replica_state: &ReplicaState,
    ) -> StorageResult<()> {
        let storage = &mut storage(ctx, &self.pool).await.wrap("storage")?;
        ctx.wait(storage.consensus_dal().put_replica_state(replica_state))
            .await?
            .context("replica_state()")
            .map_err(StorageError::Database)
    }
}

#[async_trait::async_trait]
impl PayloadSource for SignedBlockStore {
    async fn propose(
        &self,
        ctx: &ctx::Ctx,
        block_number: validator::BlockNumber,
    ) -> anyhow::Result<validator::Payload> {
        const POLL_INTERVAL: time::Duration = time::Duration::milliseconds(50);
        let storage = &mut storage(ctx, &self.pool).await?;
        loop {
            if let Some(payload) = fetch_payload(ctx, storage, block_number)
                .await
                .context("fetch_payload()")?
            {
                return Ok(payload.encode());
            }
            ctx.sleep(POLL_INTERVAL).await?;
        }
    }
}

impl SignedBlockStore {
    pub async fn run_background_tasks(&self, ctx: &ctx::Ctx) -> anyhow::Result<()> {
        const POLL_INTERVAL: time::Duration = time::Duration::milliseconds(50);
        let mut head = *self.head.borrow();
        let storage = &mut storage(ctx, &self.pool).await?;
        loop {
            head = match find_head_forward(ctx, storage, head)
                .await
                .wrap("find_head_forward()")
            {
                Ok(block) => block.header.number,
                Err(StorageError::Canceled(_)) => return Ok(()),
                Err(StorageError::Database(err)) => return Err(err),
            };
            self.head.send_if_modified(|x| {
                if *x >= head {
                    return false;
                }
                *x = head;
                true
            });
        }
    }
}
