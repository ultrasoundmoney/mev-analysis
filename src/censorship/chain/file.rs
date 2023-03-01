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
use tracing::debug;

/*
 Getting chain data from BigQuery is quite slow due to having to page through
 10MB responses. That's fine for real-time updating, but not so much for backfilling.
 This module implements `ChainStore` by reading from ndjson files exported from BigQuery.
*/

pub struct ChainStoreFile {
    block_streams: Vec<PutBack<Lines<BufReader<File>>>>,
    tx_streams: Vec<PutBack<Lines<BufReader<File>>>>,
}

impl ChainStoreFile {
    #[allow(dead_code)]
    pub fn new(blocks_path: &str, txs_path: &str) -> Result<Self> {
        let block_streams = fs::read_dir(blocks_path)
            .unwrap()
            .into_iter()
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
            .into_iter()
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
            block_streams,
            tx_streams,
        })
    }
}

#[async_trait]
impl ChainStore for ChainStoreFile {
    async fn fetch_blocks(
        &mut self,
        start: &DateTime<Utc>,
        end: &DateTime<Utc>,
    ) -> Result<Vec<Block>> {
        assert!(start <= end);
        deliver_interval_from_streams::<BlockRow, Block>(&mut self.block_streams, start, end)
    }

    async fn fetch_txs(&mut self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Tx>> {
        assert!(start <= end);
        deliver_interval_from_streams::<TxRow, Tx>(&mut self.tx_streams, start, end)
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

impl Into<Block> for BlockRow {
    fn into(self) -> Block {
        Block {
            base_fee_per_gas: self.base_fee_per_gas.parse().unwrap(),
            block_hash: self.hash,
            block_number: self.number.parse().unwrap(),
            extra_data: hex_to_option(self.extra_data),
            fee_recipient: self.miner,
            gas_limit: self.gas_limit.parse().unwrap(),
            gas_used: self.gas_used.parse().unwrap(),
            logs_bloom: self.logs_bloom,
            parent_hash: self.parent_hash,
            receipts_root: self.receipts_root,
            sha3_uncles: self.sha3_uncles,
            size: self.size.parse().unwrap(),
            state_root: self.state_root,
            timestamp: self.f0_,
            transaction_count: self.transaction_count.parse().unwrap(),
            transactions_root: self.transactions_root,
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

impl Into<Tx> for TxRow {
    fn into(self) -> Tx {
        Tx {
            address_trace: self.address_trace,
            block_number: self.block_number.parse().unwrap(),
            block_timestamp: self.f0_,
            from_address: self.from_address,
            gas: self.gas.parse().unwrap(),
            gas_price: self.gas_price.parse().unwrap(),
            input: hex_to_option(self.input),
            max_fee_per_gas: self.max_fee_per_gas.map(|s| s.parse().unwrap()),
            max_priority_fee_per_gas: self.max_priority_fee_per_gas.map(|s| s.parse().unwrap()),
            nonce: self.nonce.parse().unwrap(),
            receipt_contract_address: self.receipt_contract_address,
            receipt_cumulative_gas_used: self.receipt_cumulative_gas_used.parse().unwrap(),
            receipt_effective_gas_price: self
                .receipt_effective_gas_price
                .map(|s| s.parse().unwrap()),
            receipt_gas_used: self.receipt_gas_used.parse().unwrap(),
            receipt_status: self.receipt_status.parse().unwrap(),
            to_address: self.to_address,
            transaction_hash: self.hash,
            transaction_index: self.transaction_index.parse().unwrap(),
            transaction_type: self.transaction_type.parse().unwrap(),
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
        let mut store = ChainStoreFile::new("fixtures/blocks", "fixtures/transactions").unwrap();
        let start: DateTime<Utc> = "2023-02-01T00:30:00Z".parse().unwrap();
        let end: DateTime<Utc> = "2023-02-01T01:00:00Z".parse().unwrap();

        let blocks = store.fetch_blocks(&start, &end).await.unwrap();
        let first = blocks.first().unwrap().timestamp;
        let last = blocks.last().unwrap().timestamp;

        assert!(blocks.len() > 0);
        assert!(first > start && first <= end);
        assert!(last > start && last <= end);
        assert!(first < last);

        let new_start = last;
        let new_end = last + Duration::hours(1);
        let more_blocks = store.fetch_blocks(&new_start, &new_end).await.unwrap();

        let new_first = more_blocks.first().unwrap().timestamp;
        let new_last = more_blocks.last().unwrap().timestamp;

        assert!(more_blocks.len() > 0);
        assert!(new_first > new_start && new_first < new_last);
        assert!(new_last > new_first && new_last <= new_end);

        // blocks get consumed, so reading an interval only works once
        let blocks = store.fetch_blocks(&start, &end).await.unwrap();

        assert!(blocks.len() == 0);
    }

    #[tokio::test]
    async fn it_does_not_drop_items() {
        let mut store = ChainStoreFile::new("fixtures/blocks", "fixtures/transactions").unwrap();
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

        assert!(new_blocks.len() > 0);
        assert_eq!(new_blocks.first().unwrap().block_number, 16530248);
    }

    #[tokio::test]
    async fn it_correctly_handles_multiple_files() {
        let mut store = ChainStoreFile::new("fixtures/blocks", "fixtures/transactions").unwrap();

        // second file starts here
        let start: DateTime<Utc> = "2023-02-01T12:00:00Z".parse().unwrap();
        let end = start + Duration::hours(12);

        let blocks = store.fetch_blocks(&start, &end).await.unwrap();

        assert!(blocks.len() > 0);

        let first = blocks.first().unwrap();
        let last = blocks.last().unwrap();

        assert_eq!(first.block_number, 16533827);
        assert_eq!(last.block_number, 16537406);
    }
}
