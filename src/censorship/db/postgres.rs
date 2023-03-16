use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Pool, Postgres, QueryBuilder,
};

use super::CensorshipDB;
use crate::censorship::{
    chain::Block, env::APP_CONFIG, mempool::TaggedTx, relay::DeliveredPayload,
};

pub struct PostgresCensorshipDB {
    pool: Pool<Postgres>,
}

impl PostgresCensorshipDB {
    pub async fn new() -> Result<Self> {
        let connect_opts = PgConnectOptions::from_str(&APP_CONFIG.database_url)?
            // logging the batch inserts makes the sql pretty printer crawl to a halt
            .disable_statement_logging()
            .to_owned();

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect_with(connect_opts)
            .await?;

        Ok(Self { pool })
    }
}

// It's not great but these are used to avoid the bind limit
// when batch inserting rows, and have to be kept up to date
const BIND_LIMIT: usize = 65535;
const BLOCK_NUM_KEYS: usize = 17;
const TX_NUM_KEYS: usize = 23;
const TIMESTAMP_NUM_KEYS: usize = 4;

#[async_trait]
impl CensorshipDB for PostgresCensorshipDB {
    async fn get_chain_checkpoint(&self) -> Result<Option<DateTime<Utc>>> {
        sqlx::query_scalar!("SELECT timestamp FROM blocks ORDER BY timestamp DESC LIMIT 1")
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn get_block_production_checkpoint(&self) -> Result<Option<i64>> {
        sqlx::query_scalar!(
            "
            SELECT slot_number
            FROM block_production
            ORDER BY slot_number ASC
            LIMIT 1
            "
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn upsert_delivered_payloads(&self, payloads: Vec<DeliveredPayload>) -> Result<()> {
        for DeliveredPayload {
            slot_number,
            block_number,
            block_hash,
            builder_pubkey,
            proposer_pubkey,
            relay_id,
            ..
        } in payloads
        {
            // it's possible multiple relays will deliver the same block. in this case, append to array
            sqlx::query!(
                "
                INSERT INTO block_production (slot_number, block_number, block_hash, builder_pubkey, proposer_pubkey, relays)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (slot_number, block_number, block_hash)
                DO UPDATE
                    SET relays = ARRAY (SELECT DISTINCT UNNEST(block_production.relays || $6))
                ",
                slot_number,
                block_number,
                block_hash,
                builder_pubkey,
                proposer_pubkey,
                &vec![relay_id.to_string()]
            )
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn put_chain_data(&self, blocks: Vec<Block>, txs: Vec<TaggedTx>) -> Result<()> {
        let block_chunks = blocks
            .into_iter()
            .chunks(BIND_LIMIT / BLOCK_NUM_KEYS)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect_vec();

        for chunk in block_chunks {
            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                "
                INSERT INTO blocks (
                    base_fee_per_gas,
                    block_hash,
                    block_number,
                    extra_data,
                    fee_recipient,
                    gas_limit,
                    gas_used,
                    logs_bloom,
                    parent_hash,
                    receipts_root,
                    sha3_uncles,
                    size,
                    state_root,
                    timestamp,
                    timestamp_unix,
                    transaction_count,
                    transactions_root
                )
                ",
            );

            query_builder.push_values(chunk, |mut builder, block| {
                builder
                    .push(block.base_fee_per_gas)
                    .push_bind(block.block_hash)
                    .push_bind(block.block_number)
                    .push_bind(block.extra_data)
                    .push_bind(block.fee_recipient)
                    .push_bind(block.gas_limit)
                    .push_bind(block.gas_used)
                    .push_bind(block.logs_bloom)
                    .push_bind(block.parent_hash)
                    .push_bind(block.receipts_root)
                    .push_bind(block.sha3_uncles)
                    .push_bind(block.size)
                    .push_bind(block.state_root)
                    .push_bind(block.timestamp)
                    .push_bind(block.timestamp.timestamp())
                    .push_bind(block.transaction_count)
                    .push_bind(block.transactions_root);
            });

            query_builder.build().execute(&self.pool).await?;
        }

        let tx_chunks = &txs
            .iter()
            .chunks(BIND_LIMIT / TX_NUM_KEYS)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect_vec();

        for chunk in tx_chunks {
            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                "
                INSERT INTO transactions (
                    address_trace,
                    block_number,
                    block_timestamp,
                    block_timestamp_unix,
                    from_address,
                    gas,
                    gas_price,
                    input,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    nonce,
                    receipt_contract_address,
                    receipt_cumulative_gas_used,
                    receipt_effective_gas_price,
                    receipt_gas_used,
                    receipt_status,
                    to_address,
                    transaction_hash,
                    transaction_index,
                    transaction_type,
                    value,
                    prev_nonce_timestamp,
                    prev_nonce_timestamp_unix
                )
                ",
            );

            query_builder.push_values(chunk, |mut builder, TaggedTx { tx, .. }| {
                builder
                    .push_bind(tx.address_trace.clone())
                    .push_bind(tx.block_number)
                    .push_bind(tx.block_timestamp)
                    .push_bind(tx.block_timestamp.timestamp())
                    .push_bind(tx.from_address.clone())
                    .push_bind(tx.gas)
                    .push_bind(tx.gas_price)
                    .push_bind(tx.input.clone())
                    .push_bind(tx.max_fee_per_gas)
                    .push_bind(tx.max_priority_fee_per_gas)
                    .push_bind(tx.nonce)
                    .push_bind(tx.receipt_contract_address.clone())
                    .push_bind(tx.receipt_cumulative_gas_used)
                    .push_bind(tx.receipt_effective_gas_price)
                    .push_bind(tx.receipt_gas_used)
                    .push_bind(tx.receipt_status)
                    .push_bind(tx.to_address.clone())
                    .push_bind(tx.transaction_hash.clone())
                    .push_bind(tx.transaction_index)
                    .push_bind(tx.transaction_type)
                    .push(format_args!("{}::numeric", tx.value.clone()))
                    .push_bind(tx.prev_nonce_timestamp)
                    .push_bind(tx.prev_nonce_timestamp.map(|t| t.timestamp()));
            });

            query_builder.build().execute(&self.pool).await?;
        }

        let timestamp_chunks = txs
            .into_iter()
            .flat_map(|TaggedTx { timestamps, tx }| {
                timestamps
                    .into_iter()
                    .map(move |ts| (tx.transaction_hash.clone(), ts))
            })
            .chunks(BIND_LIMIT / TIMESTAMP_NUM_KEYS)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect_vec();

        for chunk in timestamp_chunks {
            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                "
                INSERT INTO mempool_timestamps (
                    transaction_hash,
                    source_id,
                    timestamp,
                    timestamp_unix
                )
                ",
            );

            query_builder.push_values(chunk, |mut builder, (tx_hash, ts)| {
                builder
                    .push_bind(tx_hash)
                    .push_bind(ts.id.to_string())
                    .push_bind(ts.timestamp)
                    .push_bind(ts.timestamp.timestamp());
            });

            query_builder.build().execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn populate_tx_metadata(&self) -> Result<i64> {
        let insert_count = sqlx::query_file_scalar!("sql/populate_tx_metadata.sql")
            .fetch_one(&self.pool)
            .await?;

        Ok(insert_count.unwrap_or(0))
    }

    async fn refresh_matviews(&self) -> Result<()> {
        let matviews = vec![
            "builders_7d",
            "builders_30d",
            "builder_blocks_7d",
            "builder_blocks_30d",
            "censored_transactions_7d",
            "censored_transactions_30d",
            "inclusion_delay_7d",
            "inclusion_delay_30d",
            "operators_all",
            "top_7d",
            "top_30d",
            "relay_censorship_7d",
            "relay_censorship_30d",
            "censorship_delay_7d",
            "censorship_delay_30d",
        ];

        for matview in matviews {
            sqlx::query(&format!(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY {}",
                matview
            ))
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }
}
