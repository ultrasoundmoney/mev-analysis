mod postgres;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::{chain_store::Block, timestamp_service::TaggedTx};

pub use postgres::PostgresCensorshipDB;

pub struct BlockProductionData {
    pub block_number: i64,
    pub builder_pubkey: Option<String>,
    pub proposer_pubkey: Option<String>,
    pub relay_pubkeys: Vec<String>,
}

#[async_trait]
pub trait CensorshipDB {
    async fn persist_chain_data(&self, blocks: Vec<Block>, txs: Vec<TaggedTx>) -> Result<()>;
    async fn get_block_checkpoint(&self) -> Result<Option<DateTime<Utc>>>;
}
