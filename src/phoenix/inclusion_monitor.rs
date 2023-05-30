use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};
use reqwest::StatusCode;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tracing::{error, info, warn};

use crate::{
    beacon_api::BeaconApi,
    env::{ToBeaconExplorerUrl, ToNetwork},
};

use super::{
    alert,
    checkpoint::{self, CheckpointId},
    env::APP_CONFIG,
};

#[derive(Debug)]
struct DeliveredPayload {
    inserted_at: DateTime<Utc>,
    slot: i64,
    block_hash: String,
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

async fn insert_missed_slot(
    mev_pool: &PgPool,
    slot_number: &i64,
    relayed: &String,
    canonical: Option<&String>,
) -> Result<()> {
    sqlx::query!(
        r#"
            INSERT INTO missed_slots (slot_number, relayed_block_hash, canonical_block_hash)
            VALUES ($1, $2, $3)
            ON CONFLICT (slot_number) DO UPDATE
            SET relayed_block_hash = $2, canonical_block_hash = $3
            "#,
        slot_number,
        relayed,
        canonical
    )
    .execute(mev_pool)
    .await
    .map(|_| ())
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
        let checkpoint =
            match checkpoint::get_checkpoint(&mev_pool, CheckpointId::Inclusion).await? {
                Some(c) => c,
                None => {
                    info!("no checkpoint found, initializing");
                    let now = Utc::now();
                    checkpoint::put_checkpoint(&mev_pool, CheckpointId::Inclusion, &now).await?;
                    now
                }
            };
        let payloads = get_delivered_payloads(&relay_pool, &checkpoint).await?;

        info!(
            "validating block hash for {} delivered payloads",
            payloads.len()
        );

        let explorer_url = &APP_CONFIG.env.to_beacon_explorer_url();

        for payload in &payloads {
            let block_hash = beacon_api.get_block_hash(&payload.slot).await;

            match block_hash {
                Ok(block_hash) => {
                    if payload.block_hash == block_hash {
                        info!("found matching block hash for slot {}", payload.slot);
                    } else {
                        error!("block hash mismatch for slot {}", payload.slot);

                        insert_missed_slot(
                            &mev_pool,
                            &payload.slot,
                            &payload.block_hash,
                            Some(&block_hash),
                        )
                        .await?;

                        alert::send_telegram_alert(&format!(
                            "block hash mismatch for slot [{slot}]({url}/slot/{slot}): relayed {relayed} but found {found}",
                            slot = payload.slot,
                            url = explorer_url,
                            relayed = payload.block_hash,
                            found = block_hash
                        ))
                        .await?;
                    }
                }
                Err(err) => {
                    if err.status() == Some(StatusCode::NOT_FOUND) {
                        warn!("delivered block not found for slot {}", payload.slot);

                        insert_missed_slot(&mev_pool, &payload.slot, &payload.block_hash, None)
                            .await?;

                        alert::send_telegram_alert(&format!(
                            "delivered block not found for slot [{slot}]({url}/slot/{slot})",
                            slot = payload.slot,
                            url = explorer_url,
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
            checkpoint::put_checkpoint(&mev_pool, CheckpointId::Inclusion, &new).await?;
        }

        tokio::time::sleep(Duration::minutes(1).to_std()?).await;
    }
}
