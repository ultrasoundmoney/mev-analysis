use super::{util::hex_to_option, Block, ChainStore, Tx};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures::TryStreamExt;
use gcp_bigquery_client::{
    model::{job_configuration_query::JobConfigurationQuery, table_row::TableRow},
    Client,
};
use tokio_stream::StreamExt;

#[async_trait]
impl ChainStore for Client {
    async fn fetch_blocks(
        &mut self,
        start: &DateTime<Utc>,
        end: &DateTime<Utc>,
    ) -> Result<Vec<Block>> {
        assert!(start <= end);
        let query = format!(
            r#"
            SELECT
                blocks.base_fee_per_gas,
                blocks.hash,
                blocks.number,
                blocks.extra_data,
                blocks.miner,
                blocks.gas_limit,
                blocks.gas_used,
                blocks.logs_bloom,
                blocks.parent_hash,
                blocks.receipts_root,
                blocks.sha3_uncles,
                blocks.size,
                blocks.state_root,
                FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", blocks.timestamp),
                blocks.transaction_count,
                blocks.transactions_root
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

    async fn fetch_txs(&mut self, start: &DateTime<Utc>, end: &DateTime<Utc>) -> Result<Vec<Tx>> {
        assert!(start <= end);
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
            ),
            prev AS (
                SELECT
                    block_number,
                    block_timestamp,
                    nonce,
                    from_address
                FROM bigquery-public-data.crypto_ethereum.transactions
                WHERE block_timestamp <= "{end}"
                  AND block_timestamp >= "{lookback}"
            )

            SELECT
                ARRAY(
                  SELECT DISTINCT x FROM UNNEST(traces.addresses) AS x
                ) AS address_trace,
                txs.block_number,
                FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", txs.block_timestamp),
                txs.from_address,
                txs.gas,
                txs.gas_price,
                txs.input,
                txs.max_fee_per_gas,
                txs.max_priority_fee_per_gas,
                txs.nonce,
                txs.receipt_contract_address,
                txs.receipt_cumulative_gas_used,
                txs.receipt_effective_gas_price,
                txs.receipt_gas_used,
                txs.receipt_status,
                txs.to_address,
                txs.hash,
                txs.transaction_index,
                txs.transaction_type,
                txs.value,
                FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", prev.block_timestamp)
            FROM txs INNER JOIN traces ON txs.hash = traces.transaction_hash
            LEFT JOIN prev ON prev.nonce + 1 = txs.nonce AND prev.from_address = txs.from_address
            ORDER BY txs.block_timestamp, txs.transaction_index
        "#,
            start = start,
            end = end,
            lookback = *start - Duration::days(60)
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

fn parse_block_row(row: TableRow) -> Block {
    let columns = row.columns.unwrap();
    Block {
        base_fee_per_gas: columns[0]
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
        block_number: columns[2]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        extra_data: hex_to_option(
            columns[3]
                .clone()
                .value
                .unwrap()
                .as_str()
                .unwrap()
                .to_string(),
        ),
        fee_recipient: columns[4]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        gas_limit: columns[5]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        gas_used: columns[6]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        logs_bloom: columns[7]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        parent_hash: columns[8]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        receipts_root: columns[9]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        sha3_uncles: columns[10]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        size: columns[11]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        state_root: columns[12]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        timestamp: DateTime::parse_from_rfc3339(
            columns[13].clone().value.unwrap().as_str().unwrap(),
        )
        .unwrap()
        .with_timezone(&Utc),
        transaction_count: columns[14]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        transactions_root: columns[15]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
    }
}

fn parse_tx_row(row: TableRow) -> Tx {
    let columns = row.columns.unwrap();
    Tx {
        address_trace: columns[0]
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
        block_number: columns[1]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        block_timestamp: DateTime::parse_from_rfc3339(
            &columns[2]
                .clone()
                .value
                .unwrap()
                .as_str()
                .unwrap()
                .to_string(),
        )
        .unwrap()
        .with_timezone(&Utc),
        from_address: columns[3]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        gas: columns[4]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        gas_price: columns[5]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        input: hex_to_option(
            columns[6]
                .clone()
                .value
                .unwrap()
                .as_str()
                .unwrap()
                .to_string(),
        ),
        max_fee_per_gas: columns[7]
            .clone()
            .value
            .map(|v| v.as_str().unwrap().parse::<i64>().unwrap()),
        max_priority_fee_per_gas: columns[8]
            .clone()
            .value
            .map(|v| v.as_str().unwrap().parse::<i64>().unwrap()),
        nonce: columns[9]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        receipt_contract_address: columns[10]
            .clone()
            .value
            .map(|v| v.as_str().unwrap().to_string()),
        receipt_cumulative_gas_used: columns[11]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        receipt_effective_gas_price: columns[12]
            .clone()
            .value
            .map(|v| v.as_str().unwrap().parse::<i64>().unwrap()),
        receipt_gas_used: columns[13]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        receipt_status: columns[14]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        to_address: columns[15]
            .clone()
            .value
            .map(|v| v.as_str().unwrap().to_string()),
        transaction_hash: columns[16]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        transaction_index: columns[17]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        transaction_type: columns[18]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        value: columns[19]
            .clone()
            .value
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        prev_nonce_timestamp: columns[20].clone().value.map(|v| {
            DateTime::parse_from_rfc3339(v.as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc)
        }),
    }
}
