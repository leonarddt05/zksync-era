//! Consensus-related functionality.
use anyhow::Context as _;
use zksync_concurrency::{ctx, time, ctx::channel};
use zksync_consensus_roles::validator;
use zksync_consensus_executor::{ExecutorConfig,ConsensusConfig};
use zksync_types::block::ConsensusBlockFields;
use zksync_types::{Address, MiniblockNumber};

mod payload;
mod proto;

pub(crate) use self::payload::Payload;
/*
struct Config {
    executor: ExecutorConfig,
    consensus: ConsensusConfig,
    
    node_key: node::SecretKey,
    validator_key: validator::SecretKey,
}

pub async fn run(cfg: Config, node_key: node::SecretKey, pool: zksync_dal::ConnectionPool) -> anyhow::Result<()> {
    let storage = PostgresState(pool);

    let mut executor = Executor::new(cfg, node_key, storage.clone()).context("Executor::new()")?;
    executor.set_validator(
        cfg.consensus,
        cfg.validator_key,
        storage.clone(),
    ).context("Executor::set_validator()")?;
}

#[derive(Clone)]
struct PostgresState(zksync_dal::ConnectionPool);

impl ReadBlock

async fn fetch_payload(
    storage: &mut zksync_dal::StorageProcessor<'_>,
    block_number: validator::BlockNumber,
) -> anyhow::Result<Option<validator::Payload>> {
    let block_number = MiniblockNumber(block_number.0.try_into().context("MiniblockNumber")?);
    Ok(storage
        .sync_dal()
        .sync_block(block_number, Address::default(), true)
        .await?
        // Unwrap is ok, because try_from fails only if transactions are missing,
        // and sync_block() is expected to fetch transactions.
        .map(|b| Payload::try_from(b).unwrap().encode()))
}

#[async_trait::async_trait]
impl BlockChainState for PostgresState {
    async fn propose(
        &self,
        ctx: &ctx::Ctx,
        block_number: validator::BlockNumber,
    ) -> anyhow::Result<validator::Payload> {
        const POLL_INTERVAL: time::Duration = time::Duration::milliseconds(50);
        let storage = &mut self
            .0
            .access_storage_tagged("consensus")
            .await
            .context("access_storage_tagged()")?;
        loop {
            if let Some(payload) = fetch_payload(storage, block_number).await? {
                return Ok(payload);
            }
            ctx.sleep(POLL_INTERVAL).await?;
        }
    }

    async fn verify(
        &self,
        _ctx: &ctx::Ctx,
        block_number: validator::BlockNumber,
        payload: &validator::Payload,
    ) -> anyhow::Result<()> {
        let conn = &mut self
            .0
            .access_storage_tagged("consensus")
            .await
            .context("access_storage_tagged()")?;
        anyhow::ensure!(
            &fetch_payload(conn, block_number)
                .await?
                .context("unexpected payload")?
                == payload
        );
        Ok(())
    }

    async fn apply(&self, _ctx: &ctx::Ctx, block: &validator::FinalBlock) -> anyhow::Result<()> {
        let storage = &mut self
            .0
            .access_storage_tagged("consensus")
            .await
            .context("access_storage_tagged()")?;
        let mut txn = storage
            .start_transaction()
            .await
            .context("start_transaction")?;
        anyhow::ensure!(
            &fetch_payload(&mut txn, block.header.number)
                .await?
                .context("unexpected payload")?
                == &block.payload
        );
        let block_number = MiniblockNumber(
            block
                .header
                .number
                .0
                .try_into()
                .context("MiniblockNumber")?,
        );
        txn.blocks_dal()
            .set_miniblock_consensus_fields(
                block_number,
                &ConsensusBlockFields {
                    parent: block.header.parent,
                    justification: block.justification.clone(),
                },
            )
            .await
            .context("set_miniblock_consensus_fields()")?;
        txn.commit().await.context("commit()")
    }
}
*/
