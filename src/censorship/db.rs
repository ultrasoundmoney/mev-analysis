mod postgres;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::{
    archive_node::TxLowBalanceCheck, chain::Block, mempool::TaggedTx, relay::DeliveredPayload,
};

pub use postgres::PostgresCensorshipDB;

#[async_trait]
pub trait CensorshipDB {
    // checkpoint for onchain data
    async fn get_chain_checkpoint(&self) -> Result<Option<DateTime<Utc>>>;
    // slot_number checkpoint for offchain (relay) data
    async fn get_block_production_checkpoint(&self) -> Result<Option<i64>>;
    async fn get_tx_low_balance_checks(&self) -> Result<Vec<TxLowBalanceCheck>>;
    #[allow(clippy::ptr_arg)]
    async fn update_tx_low_balance_status(&self, tx_hash: &String, status: &bool) -> Result<()>;
    async fn put_chain_data(&self, blocks: Vec<Block>, txs: Vec<TaggedTx>) -> Result<()>;
    // this method is idempotent for a given set of relays
    async fn upsert_delivered_payloads(&self, payloads: Vec<DeliveredPayload>) -> Result<()>;
    async fn populate_tx_metadata(&self) -> Result<i64>;
    async fn refresh_matviews(&self) -> Result<()>;
}
