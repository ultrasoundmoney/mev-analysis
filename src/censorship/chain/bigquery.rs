use super::{Block, ChainStore, Tx};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use gcp_bigquery_client::{
    model::{job_configuration_query::JobConfigurationQuery, table_row::TableRow},
    Client,
};
use tokio_stream::StreamExt;

#[async_trait]
impl ChainStore for Client {
    async fn fetch_blocks(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Block>> {
        let query = format!(
            r#"
            SELECT
                blocks.number,
                blocks.hash,
                FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", blocks.timestamp),
                blocks.miner,
                blocks.extra_data,
                blocks.transaction_count,
                blocks.gas_limit,
                blocks.gas_used,
                blocks.base_fee_per_gas
            FROM bigquery-public-data.crypto_ethereum.blocks
            WHERE timestamp > "{start}"
              AND timestamp <= "{end}"
            ORDER BY timestamp ASC
            "#,
            start = start,
            end = end
        );

        let blocks: Vec<Block> = self
            .job()
            .query_all(
                "ultra-sound-relay",
                JobConfigurationQuery {
                    query,
                    use_legacy_sql: Some(false),
                    ..Default::default()
                },
                None,
            )
            .map_err(Into::into)
            .collect::<Result<Vec<_>>>()
            .await?
            .into_iter()
            .flatten()
            .map(parse_block_row)
            .collect();

        Ok(blocks)
    }

    async fn fetch_txs(&self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Tx>> {
        let query = format!(
            r#"
            WITH
            txs AS (
                SELECT * FROM bigquery-public-data.crypto_ethereum.transactions
                WHERE block_timestamp > "{start}"
                AND block_timestamp <= "{end}"
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
                WHERE block_timestamp > "{start}"
                AND block_timestamp <= "{end}"
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
        "#,
            start = start,
            end = end
        );

        let txs: Vec<Tx> = self
            .job()
            .query_all(
                "ultra-sound-relay",
                JobConfigurationQuery {
                    query,
                    use_legacy_sql: Some(false),
                    ..Default::default()
                },
                None,
            )
            .map_err(Into::into)
            .collect::<Result<Vec<_>>>()
            .await?
            .into_iter()
            .flatten()
            .map(parse_tx_row)
            .collect();

        Ok(txs)
    }
}

// bigquery returns everything as strings, so this becomes quite the unwrap fest
fn parse_block_row(row: TableRow) -> Block {
    let columns = row.columns.unwrap();
    Block {
        block_number: columns[0]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),

        block_hash: columns[1]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),

        timestamp: DateTime::parse_from_rfc3339(
            columns[2].clone().value.unwrap().as_str().unwrap(),
        )
        .unwrap()
        .with_timezone(&Utc),

        fee_recipient: columns[3]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),

        extra_data: columns[4]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),

        tx_count: columns[5]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),

        gas_limit: columns[6]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),

        gas_used: columns[7]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),

        base_fee_per_gas: columns[8]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
    }
}

fn parse_tx_row(row: TableRow) -> Tx {
    let columns = row.columns.unwrap();
    Tx {
        tx_hash: columns[0]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        tx_index: columns[1]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        block_number: columns[2]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        base_fee: columns[3]
            .clone()
            .value
            .map(|v| v.as_str().unwrap().parse::<i64>().unwrap()),
        max_prio_fee: columns[4]
            .clone()
            .value
            .map(|v| v.as_str().unwrap().parse::<i64>().unwrap()),
        address_trace: columns[5]
            .clone()
            .value
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            // as_array returns strings nested in an object like { "v": "0x123" }
            .map(|obj| {
                let map = obj.as_object().unwrap();
                map.get("v").unwrap().as_str().unwrap().to_owned()
            })
            .collect(),
    }
}
