use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use reqwest::StatusCode;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tracing::{error, info, warn};

use crate::{beacon_api::BeaconApi, env::ToNetwork, phoenix::alert};

use super::env::APP_CONFIG;

#[derive(Debug)]
struct DeliveredPayload {
    inserted_at: DateTime<Utc>,
    slot: i64,
    block_hash: String,
}

async fn get_checkpoint(mev_pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    sqlx::query_scalar!(
        r#"
        SELECT timestamp
        FROM monitor_checkpoints
        WHERE monitor_id = 'inclusion_monitor'
        LIMIT 1
        "#
    )
    .fetch_optional(mev_pool)
    .await
    .map_err(Into::into)
}

async fn put_checkpoint(mev_pool: &PgPool, checkpoint: &DateTime<Utc>) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO monitor_checkpoints (monitor_id, timestamp)
        VALUES ('inclusion_monitor', $1)
        ON CONFLICT (monitor_id) DO UPDATE SET timestamp = $1
        "#,
        checkpoint
    )
    .execute(mev_pool)
    .await
    .map(|_| ())
    .map_err(Into::into)
}

async fn get_delivered_payloads(
    relay_pool: &PgPool,
    checkpoint: &DateTime<Utc>,
) -> Result<Vec<DeliveredPayload>> {
    let query = format!(
        "
        SELECT
            inserted_at,
            slot,
            block_hash
        FROM {}_payload_delivered
        WHERE inserted_at > $1
        AND inserted_at <= NOW() - '3 minutes'::interval
        ORDER BY inserted_at ASC
        ",
        &APP_CONFIG.env.to_network().to_string(),
    );

    sqlx::query(&query)
        .bind(checkpoint)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| DeliveredPayload {
                    inserted_at: Utc.from_utc_datetime(&row.get("inserted_at")),
                    slot: row.get("slot"),
                    block_hash: row.get("block_hash"),
                })
                .collect()
        })
        .map_err(Into::into)
}

pub async fn start_inclusion_monitor() -> Result<()> {
    let beacon_api = BeaconApi::new(&APP_CONFIG.consensus_nodes);
    let relay_pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::seconds(3).to_std()?)
        .connect(&APP_CONFIG.relay_database_url)
        .await?;
    let mev_pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::seconds(3).to_std()?)
        .connect(&APP_CONFIG.database_url)
        .await?;

    loop {
        let checkpoint = match get_checkpoint(&mev_pool).await? {
            Some(c) => c,
            None => {
                info!("no checkpoint found, initializing");
                let now = Utc::now();
                put_checkpoint(&mev_pool, &now).await?;
                now
            }
        };
        let payloads = get_delivered_payloads(&relay_pool, &checkpoint).await?;

        info!(
            "validating block hash for {} delivered payloads",
            payloads.len()
        );

        for payload in &payloads {
            let block_hash = beacon_api.get_block_hash(&payload.slot).await;

            match block_hash {
                Ok(block_hash) => {
                    if payload.block_hash == block_hash {
                        info!("found matching block hash for slot {}", payload.slot);
                    } else {
                        error!("block hash mismatch for slot {}", payload.slot);
                        alert::send_telegram_alert(&format!(
                            "block hash mismatch for slot {}: relayed {} but found {}",
                            payload.slot, payload.block_hash, block_hash
                        ))
                        .await?;
                    }
                }
                Err(err) => {
                    if err.status() == Some(StatusCode::NOT_FOUND) {
                        warn!("delivered block not found for slot {}", payload.slot);
                        alert::send_telegram_alert(&format!(
                            "delivered block not found for slot {}",
                            payload.slot
                        ))
                        .await?;
                    } else {
                        error!(
                            "error getting block hash for slot {}: {}",
                            payload.slot, err
                        );
                        break;
                    }
                }
            }
        }

        let new_checkpoint = &payloads.last().map(|d| d.inserted_at);

        if let Some(new) = new_checkpoint {
            info!("updating checkpoint to {}", new);
            put_checkpoint(&mev_pool, &new).await?;
        }

        tokio::time::sleep(Duration::minutes(1).to_std()?).await;
    }
}
