use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use reqwest::StatusCode;
use sqlx::{PgPool, Row};
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
#[allow(dead_code)]
struct DeliveredPayload {
    inserted_at: DateTime<Utc>,
    slot: i64,
    block_hash: String,
}

async fn get_delivered_payloads(
    relay_pool: &PgPool,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Result<Vec<DeliveredPayload>> {
    let query = format!(
        "
        SELECT
            inserted_at,
            slot,
            block_hash
        FROM {}_payload_delivered
        WHERE inserted_at > $1
        AND inserted_at <= $2
        ORDER BY inserted_at ASC
        ",
        &APP_CONFIG.env.to_network().to_string(),
    );

    sqlx::query(&query)
        .bind(start)
        .bind(end)
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

pub async fn run_inclusion_monitor(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    canonical_horizon: &DateTime<Utc>,
) -> Result<()> {
    let beacon_api = BeaconApi::new(&APP_CONFIG.consensus_nodes);

    let checkpoint = match checkpoint::get_checkpoint(mev_pool, CheckpointId::Inclusion).await? {
        Some(c) => c,
        None => {
            info!("no checkpoint found, initializing");
            let now = Utc::now();
            checkpoint::put_checkpoint(mev_pool, CheckpointId::Inclusion, &now).await?;
            now
        }
    };

    info!(
        "checking inclusions between {} and {}",
        &checkpoint, canonical_horizon
    );

    let payloads = get_delivered_payloads(relay_pool, &checkpoint, canonical_horizon).await?;

    let explorer_url = APP_CONFIG.env.to_beacon_explorer_url();

    for payload in &payloads {
        let block_hash = beacon_api.get_block_hash(&payload.slot).await;

        match block_hash {
            Ok(block_hash) => {
                if payload.block_hash == block_hash {
                    info!("found matching block hash for slot {}", payload.slot);
                } else {
                    error!("block hash mismatch for slot {}", payload.slot);

                    insert_missed_slot(
                        mev_pool,
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

                    insert_missed_slot(mev_pool, &payload.slot, &payload.block_hash, None).await?;

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

    checkpoint::put_checkpoint(mev_pool, CheckpointId::Inclusion, canonical_horizon).await?;

    Ok(())
}
