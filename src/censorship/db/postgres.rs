use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    types::BigDecimal,
    ConnectOptions, Pool, Postgres,
};

use super::CensorshipDB;
use crate::censorship::{env::APP_CONFIG, relay::DeliveredPayload};

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

#[async_trait]
impl CensorshipDB for PostgresCensorshipDB {
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
            value,
            ..
        } in payloads
        {
            // it's possible multiple relays will deliver the same block. in this case, append to array
            sqlx::query!(
                "
                INSERT INTO block_production (slot_number, block_number, block_hash, builder_pubkey, proposer_pubkey, relays, value)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (slot_number, block_number, block_hash)
                DO UPDATE SET
                  relays = ARRAY (SELECT DISTINCT UNNEST(block_production.relays || $6)),
                  value = $7
                ",
                slot_number,
                block_number,
                block_hash,
                builder_pubkey,
                proposer_pubkey,
                &vec![relay_id.to_string()],
                value.parse::<BigDecimal>().expect("failed to parse delivered payload value to BigDecimal"),
            )
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }
}
