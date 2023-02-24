mod bigquery;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct Block {
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: DateTime<Utc>,
    pub fee_recipient: String,
    pub extra_data: String,
    pub tx_count: i64,
    pub gas_limit: i64,
    pub gas_used: i64,
    pub base_fee_per_gas: i64,
}

#[derive(Clone, Debug)]
pub struct Tx {
    pub tx_hash: String,
    pub tx_index: i64,
    pub block_number: i64,
    pub base_fee: Option<i64>,
    pub max_prio_fee: Option<i64>,
    pub address_trace: Vec<String>,
}

#[async_trait]
pub trait ChainStore {
    // start is our previous checkpoint, so start = exclusive, end = inclusive
    async fn fetch_blocks(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Block>>;
    async fn fetch_txs(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Tx>>;
}
