mod bigquery;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct Block {
    pub base_fee_per_gas: i64,
    pub block_hash: String,
    pub block_number: i64,
    pub extra_data: Option<String>,
    pub fee_recipient: String,
    pub gas_limit: i64,
    pub gas_used: i64,
    pub logs_bloom: String,
    pub parent_hash: String,
    pub receipts_root: String,
    pub sha3_uncles: String,
    pub size: i64,
    pub state_root: String,
    pub timestamp: DateTime<Utc>,
    pub transaction_count: i64,
    pub transactions_root: String,
}

#[derive(Clone, Debug)]
pub struct Tx {
    pub address_trace: Vec<String>,
    pub block_number: i64,
    pub block_timestamp: DateTime<Utc>,
    pub from_address: String,
    pub gas: i64,
    pub gas_price: i64,
    pub input: Option<String>,
    pub max_fee_per_gas: Option<i64>,
    pub max_priority_fee_per_gas: Option<i64>,
    pub nonce: i64,
    pub prev_nonce_timestamp: Option<DateTime<Utc>>,
    pub receipt_contract_address: Option<String>,
    pub receipt_cumulative_gas_used: i64,
    pub receipt_effective_gas_price: Option<i64>,
    pub receipt_gas_used: i64,
    pub receipt_status: i64,
    pub to_address: Option<String>,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub transaction_type: i64,
    pub value: String,
}

#[async_trait]
pub trait ChainStore {
    // start is our previous checkpoint, so start = exclusive, end = inclusive
    async fn fetch_blocks(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Block>>;
    async fn fetch_txs(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Tx>>;
}
