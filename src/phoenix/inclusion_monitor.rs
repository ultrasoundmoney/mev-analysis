use anyhow::Result;
use chrono::Duration;
use sqlx::{PgPool, Row};
use tracing::{error, info};

use crate::{beacon_api::BeaconAPI, env::ToNetwork};

use super::env::APP_CONFIG;

#[derive(Debug)]
struct DeliveredPayload {
    slot: i64,
    block_hash: String,
}

struct CheckedPayload {
    slot_number: i64,
    relayed_block_hash: String,
    canonical_block_hash: String,
}

async fn get_last_checked_slot(mev_pool: &PgPool) -> Result<Option<i64>> {
    let slot_number = sqlx::query_scalar!(
        r#"
        SELECT slot_number
        FROM inclusion_monitor
        ORDER BY slot_number DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(mev_pool)
    .await?;

    Ok(slot_number)
}

async fn put_checked_payload(mev_pool: &PgPool, payload: CheckedPayload) -> Result<()> {
    sqlx::query!(
        r#"
            INSERT INTO inclusion_monitor (
                slot_number,
                relayed_block_hash,
                canonical_block_hash
            ) VALUES (
                $1,
                $2,
                $3
            )
            "#,
        payload.slot_number,
        payload.relayed_block_hash,
        payload.canonical_block_hash
    )
    .execute(mev_pool)
    .await?;

    Ok(())
}

async fn get_delivered_payloads(
    relay_pool: &PgPool,
    checkpoint: &Option<i64>,
) -> Result<Vec<DeliveredPayload>> {
    let query = match checkpoint {
        Some(checkpoint) => format!(
            "
            SELECT
                slot,
                block_hash
            FROM {}_payload_delivered
            WHERE slot > {}
            AND inserted_at <= NOW() - '3 minutes'::interval
            ORDER BY slot ASC
            ",
            &APP_CONFIG.env.to_network().to_string(),
            checkpoint
        ),
        // No checkpoint, get last payload
        None => format!(
            "
            SELECT
                slot,
                block_hash
            FROM {}_payload_delivered
            ORDER BY slot DESC
            LIMIT 1
            ",
            &APP_CONFIG.env.to_network().to_string()
        ),
    };

    sqlx::query(&query)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| DeliveredPayload {
                    slot: row.get("slot"),
                    block_hash: row.get("block_hash"),
                })
                .collect()
        })
        .map_err(Into::into)
}

pub async fn start_inclusion_monitor(relay_pool: &PgPool, mev_poool: &PgPool) -> Result<()> {
    let beacon_api = BeaconAPI::new(&APP_CONFIG.consensus_nodes);

    loop {
        let checkpoint = get_last_checked_slot(mev_poool).await?;
        let payloads = get_delivered_payloads(relay_pool, &checkpoint).await?;

        info!(
            "validating block hash for {} delivered payloads",
            payloads.len()
        );

        for payload in payloads {
            let block_hash = beacon_api.get_block_hash(&payload.slot).await?;

            if payload.block_hash == block_hash {
                info!("found matching block hash for slot {}", payload.slot);
            } else {
                error!("block hash mismatch for slot {}", payload.slot);
            }

            put_checked_payload(
                mev_poool,
                CheckedPayload {
                    slot_number: payload.slot,
                    relayed_block_hash: payload.block_hash,
                    canonical_block_hash: block_hash,
                },
            )
            .await?;
        }

        tokio::time::sleep(Duration::minutes(1).to_std()?).await;
    }
}
