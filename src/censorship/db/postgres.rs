use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, QueryBuilder};

use super::{CensorshipDB, Checkpoint};
use crate::censorship::{chain_store::Block, env::APP_CONFIG, timestamp_service::TaggedTx};

pub struct PostgresCensorshipDB {
    pool: Pool<Postgres>,
}

impl PostgresCensorshipDB {
    pub async fn new() -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&APP_CONFIG.db_connection_str)
            .await?;

        Ok(Self { pool })
    }
}

const BIND_LIMIT: usize = 65535;
const BLOCK_NUM_KEYS: usize = 9;
const TX_NUM_KEYS: usize = 6;

#[async_trait]
impl CensorshipDB for PostgresCensorshipDB {
    async fn get_block_checkpoint(&self) -> Result<Option<DateTime<Utc>>> {
        sqlx::query!("SELECT timestamp FROM blocks ORDER BY timestamp DESC LIMIT 1",)
            .fetch_optional(&self.pool)
            .await
            .map(|res| res.map(|row| row.timestamp))
            .map_err(Into::into)
    }

    async fn persist_chain_data(&self, blocks: Vec<Block>, txs: Vec<TaggedTx>) -> Result<()> {
        let block_chunks: Vec<Vec<Block>> = blocks
            .into_iter()
            .chunks(BIND_LIMIT / BLOCK_NUM_KEYS)
            .into_iter()
            .map(|chunk| chunk.collect())
            .collect();

        for chunk in block_chunks {
            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                "
                INSERT INTO blocks (
                    block_number,
                    block_hash,
                    timestamp,
                    fee_recipient,
                    extra_data,
                    tx_count,
                    gas_limit,
                    gas_used,
                    base_fee_per_gas
                )
                ",
            );

            query_builder.push_values(chunk, |mut builder, block| {
                builder
                    .push_bind(block.block_number)
                    .push_bind(block.block_hash)
                    .push_bind(block.timestamp)
                    .push_bind(block.fee_recipient)
                    .push_bind(block.extra_data)
                    .push_bind(block.tx_count)
                    .push_bind(block.gas_limit)
                    .push_bind(block.gas_used)
                    .push_bind(block.base_fee_per_gas);
            });

            query_builder.build().execute(&self.pool).await?;
        }

        let tx_chunks: Vec<Vec<TaggedTx>> = txs
            .into_iter()
            .chunks(BIND_LIMIT / TX_NUM_KEYS)
            .into_iter()
            .map(|chunk| chunk.collect())
            .collect();

        for chunk in tx_chunks {
            let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
                "
                INSERT INTO txs (
                    tx_hash,
                    tx_index,
                    block_number,
                    max_fee,
                    max_prio_fee,
                    address_trace
                )
                ",
            );

            query_builder.push_values(chunk, |mut builder, TaggedTx { tx, timestamps }| {
                builder
                    .push_bind(tx.tx_hash)
                    .push_bind(tx.tx_index)
                    .push_bind(tx.block_number)
                    .push_bind(tx.max_fee)
                    .push_bind(tx.max_prio_fee)
                    .push_bind(tx.address_trace);
            });

            query_builder.build().execute(&self.pool).await?;
        }

        Ok(())
    }
}
