mod loki_client;

use anyhow::Context;
pub use loki_client::LokiClient;

use chrono::{DateTime, TimeZone, Utc};
use indoc::formatdoc;
use reqwest::StatusCode;
use sqlx::{PgPool, Row};
use tracing::{error, info, warn};

use loki_client::PayloadLogStats;

use crate::{
    beacon_api::BeaconApi,
    env::{ToBeaconExplorerUrl, ToNetwork},
    phoenix::telegram::telegram_escape,
};

use super::{
    alert,
    checkpoint::{self, CheckpointId},
    env::APP_CONFIG,
};

#[derive(Debug)]
struct DeliveredPayload {
    block_hash: String,
    #[allow(dead_code)]
    inserted_at: DateTime<Utc>,
    proposer_pubkey: String,
    slot: i64,
}

async fn get_delivered_payloads(
    relay_pool: &PgPool,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> anyhow::Result<Vec<DeliveredPayload>> {
    let query = format!(
        "
        SELECT
            inserted_at,
            slot,
            block_hash,
            proposer_pubkey
        FROM {}_payload_delivered
        WHERE inserted_at > $1
        AND inserted_at <= $2
        AND publish_ms != 0
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
                    block_hash: row.get("block_hash"),
                    inserted_at: Utc.from_utc_datetime(&row.get("inserted_at")),
                    proposer_pubkey: row.get("proposer_pubkey"),
                    slot: row.get("slot"),
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
) -> anyhow::Result<()> {
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

async fn get_missed_slot_count(
    mev_pool: &PgPool,
    start_slot: &i64,
    end_slot: &i64,
) -> anyhow::Result<i64> {
    sqlx::query_scalar!(
        "SELECT COUNT(*) FROM missed_slots WHERE slot_number > $1 AND slot_number <= $2",
        start_slot,
        end_slot
    )
    .fetch_one(mev_pool)
    .await
    .map(|count| count.unwrap_or(0))
    .map_err(Into::into)
}

#[derive(Default, sqlx::FromRow)]
pub struct ProposerLabelMeta {
    pub grafitti: Option<String>,
    pub label: Option<String>,
    pub lido_operator: Option<String>,
}

async fn proposer_label_meta(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<ProposerLabelMeta> {
    sqlx::query_as::<_, ProposerLabelMeta>(
        "
        SELECT
            label,
            lido_operator,
            last_graffiti AS grafitti
        FROM validators
        WHERE pubkey = $1
        ",
    )
    .bind(proposer_pubkey)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.unwrap_or_default())
    .context("failed to get proposer label meta")
}

async fn proposer_registration_ip(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<Option<String>> {
    sqlx::query(
        "
        SELECT last_registration_ip_address
        FROM validators
        WHERE pubkey = $1
        ",
    )
    .bind(proposer_pubkey)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.and_then(|row| row.get(0)))
    .context("failed to get proposer ip")
}

async fn proposer_payload_request_ip(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<Option<String>> {
    sqlx::query(
        "
        SELECT ip
        FROM payload_requests
        WHERE pubkey = $1
        ",
    )
    .bind(proposer_pubkey)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.and_then(|row| row.get(0)))
    .context("failed to get proposer ip")
}

async fn get_proposer_ip(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<Option<String>> {
    let registration_ip = proposer_registration_ip(pg_pool, proposer_pubkey).await?;
    let payload_request_ip = proposer_payload_request_ip(pg_pool, proposer_pubkey).await?;

    Ok(registration_ip.or(payload_request_ip))
}

#[derive(Default, sqlx::FromRow)]
struct ProposerLocation {
    pub country: Option<String>,
    pub city: Option<String>,
}

async fn proposer_location(pg_pool: &PgPool, ip_address: &str) -> anyhow::Result<ProposerLocation> {
    sqlx::query_as::<_, ProposerLocation>(
        "
        SELECT country, city
        FROM ip_meta
        WHERE ip_address = $1
        ",
    )
    .bind(ip_address)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.unwrap_or_default())
    .context("failed to get proposer location")
}

fn format_delivered_not_found_message(
    log_stats: Option<PayloadLogStats>,
    proposer_meta: ProposerLabelMeta,
    proposer_ip: Option<String>,
    proposer_location: ProposerLocation,
    slot: &i64,
) -> String {
    let explorer_url = APP_CONFIG.env.to_beacon_explorer_url();

    match log_stats {
        Some(stats) => {
            let PayloadLogStats {
                decoded_at_slot_age_ms,
                pre_publish_duration_ms,
                publish_duration_ms,
                request_download_duration_ms,
            } = stats;

            let publish_took_too_long = publish_duration_ms > 1000;
            let request_delivered_late = decoded_at_slot_age_ms >= 3000;
            let safe_to_ignore = request_delivered_late && !publish_took_too_long;

            let operator = {
                let label = proposer_meta.label.as_deref().unwrap_or("-");
                telegram_escape(label)
            };

            let lido_operator = {
                let lido_operator = proposer_meta.lido_operator.as_deref().unwrap_or("-");
                telegram_escape(lido_operator)
            };

            let grafitti = {
                let grafitti = proposer_meta.grafitti.as_deref().unwrap_or("-");
                telegram_escape(grafitti)
            };

            let proposer_ip = {
                let ip = proposer_ip.unwrap_or("-".to_string());
                telegram_escape(&ip)
            };

            let proposer_country = {
                let country = proposer_location.country.unwrap_or("-".to_string());
                telegram_escape(&country)
            };

            let proposer_city = {
                let city = proposer_location.city.unwrap_or("-".to_string());
                telegram_escape(&city)
            };

            formatdoc!(
                "
                delivered block not found for slot

                [beaconcha\\.in/slot/{slot}]({explorer_url}/slot/{slot})

                ```
                decoded_at_slot_age_ms: {decoded_at_slot_age_ms}
                pre_publish_duration_ms: {pre_publish_duration_ms}
                proposer_city: {proposer_city}
                proposer_country: {proposer_country}
                proposer_grafitti: {grafitti}
                proposer_ip: {proposer_ip}
                proposer_label: {operator}
                proposer_lido_operator: {lido_operator}
                publish_duration_ms: {publish_duration_ms}
                request_download_duration_ms: {request_download_duration_ms}
                safe_to_ignore: {safe_to_ignore}
                slot: {slot}
                ```
                ",
            )
        }
        None => {
            error!("no payload log stats found for slot {}", slot);

            formatdoc!(
                "
                delivered block not found for slot

                [beaconcha\\.in/slot/{slot}]({explorer_url}/slot/{slot})

                ```
                log based stats not found, check logs for details
                ```
                ",
            )
        }
    }
}

pub async fn run_inclusion_monitor(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    canonical_horizon: &DateTime<Utc>,
    log_client: &LokiClient,
) -> anyhow::Result<()> {
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

                    let log_stats = log_client.payload_logs(payload.slot).await?;
                    let proposer_meta =
                        proposer_label_meta(mev_pool, &payload.proposer_pubkey).await?;
                    let proposer_ip = get_proposer_ip(mev_pool, &payload.proposer_pubkey).await?;
                    let proposer_location = if let Some(proposer_ip) = &proposer_ip {
                        proposer_location(mev_pool, proposer_ip).await?
                    } else {
                        ProposerLocation::default()
                    };
                    let msg = format_delivered_not_found_message(
                        log_stats,
                        proposer_meta,
                        proposer_ip,
                        proposer_location,
                        &payload.slot,
                    );

                    alert::send_telegram_alert(&msg).await?;
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

    let last_slot_delivered = payloads.last().map(|p| p.slot);

    if let Some(last_slot) = last_slot_delivered {
        let start = last_slot - APP_CONFIG.missed_slots_check_range;
        let end = last_slot;

        let missed_slot_count = get_missed_slot_count(mev_pool, &start, &end).await?;

        if missed_slot_count >= APP_CONFIG.missed_slots_alert_threshold {
            let message = format!(
                "missed {} slots in the last {} slots",
                missed_slot_count, APP_CONFIG.missed_slots_check_range
            );
            warn!("{}", &message);
            alert::send_alert(&message).await?;
        }
    }

    checkpoint::put_checkpoint(mev_pool, CheckpointId::Inclusion, canonical_horizon).await?;

    Ok(())
}
