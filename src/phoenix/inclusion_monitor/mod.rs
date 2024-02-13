mod loki_client;
mod proposer_meta;

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
    phoenix::{
        inclusion_monitor::proposer_meta::{
            get_proposer_ip, proposer_label_meta, proposer_location,
        },
        telegram::{send_telegram_alert, send_telegram_warning, telegram_escape},
    },
};

use self::{loki_client::LatePayloadStats, proposer_meta::ProposerLocation};

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
    let on_chain_block_hash = telegram_escape(found_block_hash.as_deref().unwrap_or("-"));

    let mut message = formatdoc!(
        "
        *delivered block not found*

        [beaconcha\\.in/slot/{slot}]({explorer_url}/slot/{slot})
        slot: {slot}
        payload\\_block\\_hash: {payload_block_hash}
        on\\_chain\\_block\\_hash: {on_chain_block_hash}
        "
    );

    let is_adjustment_hash = check_is_adjustment_hash(relay_pool, &payload.block_hash).await?;
    message.push_str(&format!("is\\_missed\\_adjustment: {}", is_adjustment_hash));

    // Check if a publish was attempted, if yes, add publish stats.
    let published_stats = loki_client.published_stats(payload.slot).await?;
    match published_stats {
        Some(ref payload_stats) => {
            let PublishedPayloadStats {
                decoded_at_slot_age_ms,
                pre_publish_duration_ms,
                publish_duration_ms,
                request_download_duration_ms,
            } = payload_stats;
            let published_stats_message = formatdoc!(
                "
                publish was attempted, publish stats
                decoded\\_at\\_slot\\_age\\_ms: {decoded_at_slot_age_ms}
                pre\\_publish\\_duration\\_ms: {pre_publish_duration_ms}
                publish\\_duration\\_ms: {publish_duration_ms}
                request\\_download\\_duration\\_ms: {request_download_duration_ms}
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
    let proposer_meta = proposer_label_meta(mev_pool, &payload.proposer_pubkey).await?;
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

    let proposer_ip = get_proposer_ip(mev_pool, &payload.proposer_pubkey).await?;
    let proposer_ip_formatted = {
        let ip = proposer_ip.as_deref().unwrap_or("-");
        telegram_escape(ip)
    };

    let proposer_location = {
        match proposer_ip {
            Some(proposer_ip) => proposer_location(mev_pool, &proposer_ip).await?,
            None => ProposerLocation::default(),
        }
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
        proposer\\_city: {proposer_city}
        proposer\\_country: {proposer_country}
        proposer\\_grafitti: {grafitti}
        proposer\\_ip: {proposer_ip_formatted}
        proposer\\_label: {operator}
        proposer\\_lido\\_operator: {lido_operator}
        ",
    );
    message.push('\n');
    message.push_str(&proposer_meta_message);

    let publish_errors = loki_client.error_messages(slot).await?;
    if !publish_errors.is_empty() {
        message.push('\n');
        message.push_str("found publish errors");
        for error in publish_errors.iter() {
            let error_message = {
                let error_message = formatdoc!(
                    "
                    ```
                    {}
                    ```
                    ",
                    error
                );
                telegram_escape(&error_message)
            };
            message.push('\n');
            message.push_str(&error_message);
        }
    } else {
        message.push('\n');
        message.push_str("no publish errors found");
    }

    let late_call_stats = loki_client.late_call_stats(slot).await?;
    if let Some(late_call_stats) = &late_call_stats {
        let LatePayloadStats {
            decoded_at_slot_age_ms,
            request_download_duration_ms,
        } = late_call_stats;
        let late_call_message = formatdoc!(
            "
            found late call warnings, first warning stats
            decoded\\_at\\_slot\\_age\\_ms: {decoded_at_slot_age_ms}
            request\\_download\\_duration\\_ms: {request_download_duration_ms}
            "
        );
        message.push_str("\n\n");
        message.push_str(&late_call_message);
    } else {
        message.push_str("\n\n");
        message.push_str("no late call warnings found");
    }

    if published_stats.is_none() && publish_errors.is_empty() && late_call_stats.is_some() {
        send_telegram_warning(&message).await?;
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
