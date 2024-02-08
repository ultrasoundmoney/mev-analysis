mod loki_client;

use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use indoc::formatdoc;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

pub use loki_client::LokiClient;
use loki_client::PublishedPayloadStats;

use crate::{
    beacon_api::BeaconApi,
    env::{ToBeaconExplorerUrl, ToNetwork},
    phoenix::telegram::{send_telegram_alert, send_telegram_warning, telegram_escape},
};

use self::loki_client::LatePayloadStats;

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

async fn check_is_adjustment_hash(pg_pool: &PgPool, block_hash: &str) -> anyhow::Result<bool> {
    sqlx::query(
        "
        SELECT EXISTS (
            SELECT 1
            FROM turbo_adjustment_trace
            WHERE adjusted_block_hash = $1
        )
        ",
    )
    .bind(block_hash)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.map(|row| row.get(0)).unwrap_or(false))
    .context("failed to check if block hash is adjustment hash")
}

async fn report_missing_payload(
    found_block_hash: Option<String>,
    loki_client: &LokiClient,
    mev_pool: &PgPool,
    payload: &DeliveredPayload,
    relay_pool: &PgPool,
) -> anyhow::Result<()> {
    insert_missed_slot(
        mev_pool,
        &payload.slot,
        &payload.block_hash,
        found_block_hash.as_ref(),
    )
    .await?;

    let explorer_url = APP_CONFIG.env.to_beacon_explorer_url();

    let slot = payload.slot;
    let payload_block_hash = &payload.block_hash;
    let on_chain_block_hash = found_block_hash.unwrap_or_else(|| "-".to_string());

    let mut message = formatdoc!(
        "
        delivered block not found

        beaconcha\\.in: [slot/{slot}]({explorer_url}/slot/{slot})
        slot: `{slot}`
        payload_block_hash: `{payload_block_hash}`
        on_chain_block_hash: `{on_chain_block_hash}`
        "
    );

    // Check if a publish was attempted, if yes, add publish stats.
    match loki_client.published_stats(payload.slot).await? {
        Some(payload_stats) => {
            let PublishedPayloadStats {
                decoded_at_slot_age_ms,
                pre_publish_duration_ms,
                publish_duration_ms,
                request_download_duration_ms,
            } = payload_stats;
            let published_stats_message = formatdoc!(
                "
                publish was attempted

                decoded_at_slot_age_ms: `{decoded_at_slot_age_ms}`
                pre_publish_duration_ms: `{pre_publish_duration_ms}`
                publish_duration_ms: `{publish_duration_ms}`
                request_download_duration_ms: `{request_download_duration_ms}`
                ",
            );
            message.push_str("\n\n");
            message.push_str(&published_stats_message);
        }
        None => {
            message.push_str("\n\n");
            message.push_str("no logs indicating publish was attempted");
        }
    }

    // Add proposer meta.
    let (proposer_meta, proposer_ip, proposer_location) = tokio::try_join!(
        proposer_label_meta(mev_pool, &payload.proposer_pubkey),
        get_proposer_ip(mev_pool, &payload.proposer_pubkey),
        proposer_location(mev_pool, &payload.proposer_pubkey)
    )?;
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
        let ip = proposer_ip.as_deref().unwrap_or("-");
        telegram_escape(ip)
    };

    let proposer_country = {
        let country = proposer_location.country.as_deref().unwrap_or("-");
        telegram_escape(country)
    };

    let proposer_city = {
        let city = proposer_location.city.as_deref().unwrap_or("-");
        telegram_escape(city)
    };
    let proposer_meta_message = formatdoc!(
        "
        proposer meta

        proposer_city: `{proposer_city}`
        proposer_country: `{proposer_country}`
        proposer_grafitti: `{grafitti}`
        proposer_ip: `{proposer_ip}`
        proposer_label: `{operator}`
        proposer_lido_operator: `{lido_operator}`
        ",
    );
    message.push_str("\n\n");
    message.push_str(&proposer_meta_message);

    let is_adjustment_hash = check_is_adjustment_hash(relay_pool, &payload.block_hash).await?;
    message.push_str("\n\n");
    message.push_str(&format!("is_missed_adjustment: `{}`", is_adjustment_hash));

    let publish_errors = loki_client.error_messages(slot).await?;
    // if !publish_errors.is_empty() {
    //     message.push_str("\n\n");
    //     message.push_str("found publish errors\n");
    //     for error in publish_errors.iter() {
    //         let error_message = {
    //             let error_message = formatdoc!(
    //                 "
    //                 ```
    //                 {}
    //                 ```
    //                 ",
    //                 error
    //             );
    //             telegram_escape(&error_message)
    //         };
    //         message.push_str(&error_message);
    //     }
    // }

    let late_call_stats = loki_client.late_call_stats(slot).await?;
    if let Some(late_call_stats) = &late_call_stats {
        let LatePayloadStats {
            decoded_at_slot_age_ms,
            request_download_duration_ms,
        } = late_call_stats;
        let late_call_message = formatdoc!(
            "
            found late call warnings

            decoded_at_slot_age_ms: `{decoded_at_slot_age_ms}`
            request_download_duration_ms: `{request_download_duration_ms}`
            "
        );
        message.push_str("\n\n");
        message.push_str(&late_call_message);
    }

    if publish_errors.is_empty() && late_call_stats.is_some() {
        send_telegram_warning(&message, "HTML").await?;
        send_telegram_warning(&message, "MarkdownV2").await?;
    } else {
        send_telegram_alert(&message).await?;
    }

    Ok(())
}

async fn check_missing_payload(
    beacon_api: &BeaconApi,
    loki_client: &LokiClient,
    mev_pool: &PgPool,
    payload: &DeliveredPayload,
    relay_pool: &PgPool,
) -> anyhow::Result<()> {
    let block_hash = beacon_api.get_block_hash(&payload.slot).await?;

    match block_hash {
        Some(block_hash) => {
            if payload.block_hash == block_hash {
                info!(
                    slot = payload.slot,
                    block_hash = payload.block_hash,
                    "found matching block hash"
                );
                Ok(())
            } else {
                warn!(
                    slot = payload.slot,
                    block_hash_payload = payload.block_hash,
                    block_hash_on_chain = block_hash,
                    "block hash on chain does not match payload"
                );

                report_missing_payload(Some(block_hash), loki_client, mev_pool, payload, relay_pool)
                    .await
            }
        }
        None => {
            warn!("delivered block not found for slot {}", payload.slot);
            report_missing_payload(None, loki_client, mev_pool, payload, relay_pool).await
        }
    }
}

pub async fn run_inclusion_monitor(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    canonical_horizon: &DateTime<Utc>,
    loki_client: &LokiClient,
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
    for payload in &payloads {
        check_missing_payload(&beacon_api, loki_client, mev_pool, payload, relay_pool).await?;
        debug!(
            slot = payload.slot,
            block_hash = payload.block_hash,
            "done checking payload"
        );
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
            alert::send_opsgenie_telegram_alert(&message).await?;
        }
    }

    checkpoint::put_checkpoint(mev_pool, CheckpointId::Inclusion, canonical_horizon).await?;

    Ok(())
}
