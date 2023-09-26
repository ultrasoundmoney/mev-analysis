use super::util::hex_to_option;
use super::{Block, ChainStore, Tx};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::{Itertools, PutBack};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Lines};
use std::sync::RwLock;
use tracing::debug;

/*
 Getting chain data from BigQuery is quite slow due to having to page through
 10MB responses. That's fine for real-time updating, but not so much for backfilling.
 This module implements `ChainStore` by reading from ndjson files exported from BigQuery.
*/

pub struct ChainStoreFile {
    block_streams: RwLock<Vec<PutBack<Lines<BufReader<File>>>>>,
    tx_streams: RwLock<Vec<PutBack<Lines<BufReader<File>>>>>,
}

impl ChainStoreFile {
    #[allow(dead_code)]
    pub fn new(blocks_path: &str, txs_path: &str) -> Result<Self> {
        let block_streams = fs::read_dir(blocks_path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .sorted_by_key(|entry| entry.path())
            .rev()
            .map(|entry| {
                let file = File::open(entry.path()).unwrap();
                itertools::put_back(BufReader::new(file).lines())
            })
            .collect_vec();

        let tx_streams = fs::read_dir(txs_path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .sorted_by_key(|entry| entry.path())
            .rev()
            .map(|entry| {
                let file = File::open(entry.path()).unwrap();
                itertools::put_back(BufReader::new(file).lines())
            })
            .collect_vec();

        debug!(
            "ingesting chain data from {} block files and {} transaction files",
            &block_streams.len(),
            &tx_streams.len()
        );

        Ok(Self {
            block_streams: RwLock::new(block_streams),
            tx_streams: RwLock::new(tx_streams),
        })
    }
}

#[async_trait]
impl ChainStore for ChainStoreFile {
    async fn fetch_blocks(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Block>> {
        assert!(start <= end);
        let mut streams = self.block_streams.write().unwrap();
        deliver_interval_from_streams::<BlockRow, Block>(&mut streams, start, end)
    }

    async fn fetch_txs(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Tx>> {
        assert!(start <= end);
        let mut streams = self.tx_streams.write().unwrap();
        deliver_interval_from_streams::<TxRow, Tx>(&mut streams, start, end)
    }
}

fn deliver_interval_from_streams<R, T>(
    streams: &mut Vec<PutBack<Lines<BufReader<File>>>>,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Result<Vec<T>>
where
    R: DeserializeOwned + Into<T>,
    T: HasTimestamp,
{
    let mut results: Vec<T> = vec![];

    loop {
        let next_line = streams.last_mut().and_then(|s| s.next());

        match next_line {
            Some(Ok(line)) => {
                let row: R = serde_json::from_str(&line)?;
                let thing: T = row.into();
                let timestamp = thing.get_timestamp();

                if timestamp > end {
                    debug!("reached end of interval");
                    streams.last_mut().unwrap().put_back(Ok(line));
                    break;
                }

                if timestamp > start && timestamp <= end {
                    results.push(thing);
                }
            }
            None => {
                streams.pop();
                match streams.last() {
                    Some(_) => {
                        debug!("switching to next stream");
                    }
                    None => {
                        debug!("reached end of streams");
                        break;
                    }
                }
            }
            Some(Err(err)) => {
                return Err(anyhow!("error reading stream: {}", err));
            }
        }
    }
    Ok(results)
}

#[derive(Deserialize, Debug)]
struct BlockRow {
    pub base_fee_per_gas: String,
    pub hash: String,
    pub number: String,
    pub extra_data: String,
    pub miner: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub logs_bloom: String,
    pub parent_hash: String,
    pub receipts_root: String,
    pub sha3_uncles: String,
    pub size: String,
    pub state_root: String,
    pub f0_: DateTime<Utc>,
    pub transaction_count: String,
    pub transactions_root: String,
}

impl From<BlockRow> for Block {
    fn from(val: BlockRow) -> Self {
        Block {
            base_fee_per_gas: val.base_fee_per_gas.parse().unwrap(),
            block_hash: val.hash,
            block_number: val.number.parse().unwrap(),
            extra_data: hex_to_option(val.extra_data),
            fee_recipient: val.miner,
            gas_limit: val.gas_limit.parse().unwrap(),
            gas_used: val.gas_used.parse().unwrap(),
            logs_bloom: val.logs_bloom,
            parent_hash: val.parent_hash,
            receipts_root: val.receipts_root,
            sha3_uncles: val.sha3_uncles,
            size: val.size.parse().unwrap(),
            state_root: val.state_root,
            timestamp: val.f0_,
            transaction_count: val.transaction_count.parse().unwrap(),
            transactions_root: val.transactions_root,
        }
    }
}

#[derive(Deserialize)]
pub struct TxRow {
    pub address_trace: Vec<String>,
    pub block_number: String,
    pub f0_: DateTime<Utc>,
    pub from_address: String,
    pub gas: String,
    pub gas_price: String,
    pub input: String,
    pub max_fee_per_gas: Option<String>,
    pub max_priority_fee_per_gas: Option<String>,
    pub nonce: String,
    pub receipt_contract_address: Option<String>,
    pub receipt_cumulative_gas_used: String,
    pub receipt_effective_gas_price: Option<String>,
    pub receipt_gas_used: String,
    pub receipt_status: String,
    pub to_address: Option<String>,
    pub hash: String,
    pub transaction_index: String,
    pub transaction_type: String,
    // pub value: String,
}

impl From<TxRow> for Tx {
    fn from(val: TxRow) -> Self {
        Tx {
            address_trace: val.address_trace,
            block_number: val.block_number.parse().unwrap(),
            block_timestamp: val.f0_,
            from_address: val.from_address,
            gas: val.gas.parse().unwrap(),
            gas_price: val.gas_price.parse().unwrap(),
            input: hex_to_option(val.input),
            max_fee_per_gas: val.max_fee_per_gas.map(|s| s.parse().unwrap()),
            max_priority_fee_per_gas: val.max_priority_fee_per_gas.map(|s| s.parse().unwrap()),
            nonce: val.nonce.parse().unwrap(),
            receipt_contract_address: val.receipt_contract_address,
            receipt_cumulative_gas_used: val.receipt_cumulative_gas_used.parse().unwrap(),
            receipt_effective_gas_price: val
                .receipt_effective_gas_price
                .map(|s| s.parse().unwrap()),
            receipt_gas_used: val.receipt_gas_used.parse().unwrap(),
            receipt_status: val.receipt_status.parse().unwrap(),
            to_address: val.to_address,
            transaction_hash: val.hash,
            transaction_index: val.transaction_index.parse().unwrap(),
            transaction_type: val.transaction_type.parse().unwrap(),
            value: "lol".to_string(), // value: self.value.parse().unwrap(),
            prev_nonce_timestamp: None,
        }
    }
}

trait HasTimestamp {
    fn get_timestamp(&self) -> &DateTime<Utc>;
}

impl HasTimestamp for Block {
    fn get_timestamp(&self) -> &DateTime<Utc> {
        &self.timestamp
    }
}

impl HasTimestamp for Tx {
    fn get_timestamp(&self) -> &DateTime<Utc> {
        &self.block_timestamp
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, Utc};

    use super::{ChainStore, ChainStoreFile};

    #[tokio::test]
    async fn it_fetches_correct_intervals() {
        let store = ChainStoreFile::new("fixtures/blocks", "fixtures/transactions").unwrap();
        let start: DateTime<Utc> = "2023-02-01T00:30:00Z".parse().unwrap();
        let end: DateTime<Utc> = "2023-02-01T01:00:00Z".parse().unwrap();

        let blocks = store.fetch_blocks(&start, &end).await.unwrap();
        let first = blocks.first().unwrap().timestamp;
        let last = blocks.last().unwrap().timestamp;

        assert!(!blocks.is_empty());
        assert!(first > start && first <= end);
        assert!(last > start && last <= end);
        assert!(first < last);

        let new_start = last;
        let new_end = last + Duration::hours(1);
        let more_blocks = store.fetch_blocks(&new_start, &new_end).await.unwrap();

        let new_first = more_blocks.first().unwrap().timestamp;
        let new_last = more_blocks.last().unwrap().timestamp;

        assert!(!more_blocks.is_empty());
        assert!(new_first > new_start && new_first < new_last);
        assert!(new_last > new_first && new_last <= new_end);

        // blocks get consumed, so reading an interval only works once
        let blocks = store.fetch_blocks(&start, &end).await.unwrap();

        assert!(blocks.is_empty());
    }

    #[tokio::test]
    async fn it_does_not_drop_items() {
        let store = ChainStoreFile::new("fixtures/blocks", "fixtures/transactions").unwrap();
        // start and end are before the first block
        let start: DateTime<Utc> = "2023-01-01T00:00:00Z".parse().unwrap();
        let end = start + Duration::hours(1);

        let blocks = store.fetch_blocks(&start, &end).await.unwrap();

        assert_eq!(blocks.len(), 0);

        let proper_start: DateTime<Utc> = "2023-02-01T00:00:00Z".parse().unwrap();
        let proper_end = proper_start + Duration::hours(1);

        let new_blocks = store
            .fetch_blocks(&proper_start, &proper_end)
            .await
            .unwrap();

        assert!(!new_blocks.is_empty());
        assert_eq!(new_blocks.first().unwrap().block_number, 16530248);
    }

    #[tokio::test]
    async fn it_correctly_handles_multiple_files() {
        let store = ChainStoreFile::new("fixtures/blocks", "fixtures/transactions").unwrap();

        // second file starts here
        let start: DateTime<Utc> = "2023-02-01T12:00:00Z".parse().unwrap();
        let end = start + Duration::hours(12);

        let blocks = store.fetch_blocks(&start, &end).await.unwrap();

        assert!(!blocks.is_empty());

        let first = blocks.first().unwrap();
        let last = blocks.last().unwrap();

        assert_eq!(first.block_number, 16533827);
        assert_eq!(last.block_number, 16537406);
    }
}
