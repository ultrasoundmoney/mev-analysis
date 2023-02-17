use super::{Block, ChainStore, Tx};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use gcp_bigquery_client::{model::query_request::QueryRequest, Client};

#[async_trait]
impl ChainStore for Client {
    async fn fetch_blocks(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Block>> {
        let query = format!(
            "
            SELECT * FROM bigquery-public-data.crypto_ethereum.blocks
            WHERE timestamp >= \"{start}\"
              AND timestamp <= \"{end}\"
            ORDER BY timestamp ASC
            ",
            start = start,
            end = end
        );

        let mut res = self
            .job()
            // TODO: read from env
            .query("ultra-sound-relay", QueryRequest::new(&query))
            .await?;

        let mut blocks: Vec<Block> = Vec::new();

        while res.next_row() {
            let ts = res.get_i64_by_name("timestamp")?.unwrap();

            blocks.push(Block {
                block_number: res.get_i64_by_name("number")?.unwrap(),
                block_hash: res.get_string_by_name("hash")?.unwrap(),
                block_timestamp: Utc.timestamp_opt(ts, 0).unwrap(),
                fee_recipient: res.get_string_by_name("miner")?.unwrap(),
                extra_data: res.get_string_by_name("extra_data")?.unwrap(),
                tx_count: res.get_i64_by_name("transaction_count")?.unwrap(),
                gas_limit: res.get_i64_by_name("gas_limit")?.unwrap(),
                gas_used: res.get_i64_by_name("gas_used")?.unwrap(),
                base_fee_per_gas: res.get_i64_by_name("base_fee_per_gas")?.unwrap(),
            });
        }

        Ok(blocks)
    }

    async fn fetch_txs(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Tx>> {
        let query = format!(
            "
            WITH
            txs AS (
                SELECT * FROM bigquery-public-data.crypto_ethereum.transactions
                WHERE block_timestamp >= \"{start}\"
                AND block_timestamp <= \"{end}\"
                ORDER BY
                block_number ASC,
                transaction_index ASC
            ),
            traces AS (
                SELECT
                    transaction_hash,
                    ARRAY_CONCAT(
                        ARRAY_AGG(from_address IGNORE NULLS),
                        ARRAY_AGG(to_address IGNORE NULLS)
                    ) AS addresses
                FROM bigquery-public-data.crypto_ethereum.traces
                WHERE block_timestamp >= \"{start}\"
                AND block_timestamp <= \"{end}\"
                GROUP BY transaction_hash
            )

            SELECT
                txs.hash,
                txs.transaction_index,
                txs.block_number,
                txs.max_fee_per_gas,
                txs.max_priority_fee_per_gas,
                ARRAY(
                  SELECT DISTINCT x FROM UNNEST(traces.addresses) AS x
                ) AS address_trace
            FROM txs INNER JOIN traces ON txs.hash = traces.transaction_hash
            ORDER BY txs.block_timestamp, txs.transaction_index
        ",
            start = start,
            end = end
        );

        let mut res = self
            .job()
            // TODO: read from env
            .query("ultra-sound-relay", QueryRequest::new(&query))
            .await?;

        let mut txs: Vec<Tx> = Vec::new();

        while res.next_row() {
            let address_trace = res
                .get_json_value_by_name("address_trace")?
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                // as_array returns strings nested in an object like { "v": "0x123" }
                .map(|obj| {
                    let map = obj.as_object().unwrap();
                    map.get("v").unwrap().as_str().unwrap().to_owned()
                })
                .collect();

            txs.push(Tx {
                tx_hash: res.get_string_by_name("hash")?.unwrap(),
                tx_index: res.get_i64_by_name("transaction_index")?.unwrap(),
                block_number: res.get_i64_by_name("block_number")?.unwrap(),
                max_fee: res.get_i64_by_name("max_fee_per_gas")?,
                max_prio_fee: res.get_i64_by_name("max_priority_fee_per_gas")?,
                address_trace,
            });
        }

        Ok(txs)
    }
}
