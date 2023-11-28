//! Consensus-related functionality.
#![allow(unused)]
use std::sync::Arc;
use zksync_concurrency::{ctx, scope};
use zksync_consensus_executor::{ConsensusConfig, Executor, ExecutorConfig};
use zksync_consensus_roles::{node, validator};
use zksync_dal::ConnectionPool;

mod payload;
mod proto;
mod storage;
mod wrap_error;

pub(crate) use self::payload::Payload;
pub(crate) use self::storage::sync_block_to_consensus_block;

pub struct Config {
    pub executor: ExecutorConfig,
    pub consensus: ConsensusConfig,
    pub node_key: node::SecretKey,
    pub validator_key: validator::SecretKey,
}

impl Config {
    pub async fn run(self, ctx: &ctx::Ctx, pool: ConnectionPool) -> anyhow::Result<()> {
        assert_eq!(
            self.executor.validators,
            validator::ValidatorSet::new(vec![self.validator_key.public()]).unwrap(),
            "currently only consensus with just 1 validator is supported"
        );
        let store = Arc::new(
            storage::SignedBlockStore::new(ctx, pool, &self.executor.genesis_block).await?,
        );
        let mut executor = Executor::new(ctx, self.executor, self.node_key, store.clone()).await?;
        executor.set_validator(
            self.consensus,
            self.validator_key,
            store.clone(),
            store.clone(),
        );
        scope::run!(&ctx, |ctx, s| async {
            s.spawn_bg(store.run_background_tasks(ctx));
            executor.run(ctx).await
        })
        .await
    }
}
