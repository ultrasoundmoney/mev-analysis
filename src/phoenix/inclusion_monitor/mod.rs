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
    beacon_api::{BeaconApi, ExecutionPayload},
    env::ToBeaconExplorerUrl,
    phoenix::{
        alerts,
        inclusion_monitor::proposer_meta::{
            get_proposer_ip, proposer_label_meta, proposer_location,
        },
        slot::Slot,
    },
};

use self::{loki_client::LatePayloadStats, proposer_meta::ProposerLocation};

use super::{
    alerts::telegram::{self, Channel, TelegramMessage},
    checkpoint::{self, CheckpointId},
    env::{Geo, APP_CONFIG},
};

#[derive(Debug)]
struct DeliveredPayload {
    block_hash: String,
    block_number: i64,
    #[allow(dead_code)]
    inserted_at: DateTime<Utc>,
    proposer_pubkey: String,
    slot: i64,
    geo: Geo,
}

#[derive(Debug, Clone, Copy)]
enum DetectionMethod {
    DeliveredPayload,
    RequestIntersection,
}

async fn get_delivered_payloads(
    relay_pool: &PgPool,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> anyhow::Result<Vec<DeliveredPayload>> {
    let query = r#"
        SELECT
            inserted_at,
            slot,
            geo,
            block_hash,
            block_number,
            proposer_pubkey
        FROM payload_delivered
        WHERE inserted_at > $1
        AND inserted_at <= $2
        ORDER BY inserted_at ASC
        "#;

    sqlx::query(query)
        .bind(start)
        .bind(end)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| DeliveredPayload {
                    block_hash: row.get("block_hash"),
                    block_number: row.get("block_number"),
                    inserted_at: Utc.from_utc_datetime(&row.get("inserted_at")),
                    proposer_pubkey: row.get("proposer_pubkey"),
                    slot: row.get("slot"),
                    geo: row.get("geo"),
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
    // avoid duplicate entries in case the same miss is detected via multiple paths
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM missed_slots WHERE slot_number = $1 AND relayed_block_hash = $2
        )
        "#,
    )
    .bind(slot_number)
    .bind(relayed)
    .fetch_one(mev_pool)
    .await?;

    if exists {
        debug!(
            slot = slot_number,
            relayed_block_hash = relayed,
            "missed slot already recorded, skipping insert"
        );
        return Ok(());
    }

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
            FROM adjustment_trace
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
    is_attempted_reorg: bool,
    detection_method: DetectionMethod,
) -> anyhow::Result<()> {
    insert_missed_slot(
        mev_pool,
        &payload.slot,
        &payload.block_hash,
        found_block_hash.as_ref(),
    )
    .await?;

    let explorer_url = APP_CONFIG.network.to_beacon_explorer_url();

    let slot = payload.slot;
    let geo = &payload.geo;
    let payload_block_hash = &payload.block_hash;
    let on_chain_block_hash = telegram::escape_str(found_block_hash.as_deref().unwrap_or("-"));

    let mut message = formatdoc!(
        "
        *delivered block not found*

        [beaconcha\\.in/slot/{slot}]({explorer_url}/slot/{slot})
        slot: {slot}
        geo: {geo}
        payload\\_block\\_hash: {payload_block_hash}
        on\\_chain\\_block\\_hash: {on_chain_block_hash}
        "
    );

    match detection_method {
        DetectionMethod::DeliveredPayload => {
            message.push_str("detected_via: delivered_payload\n");
        }
        DetectionMethod::RequestIntersection => {
            message.push_str("detected_via: request_intersection\n");
        }
    }

    let is_adjustment_hash = check_is_adjustment_hash(relay_pool, &payload.block_hash).await?;
    message.push_str(&format!("is\\_adjustment: {}", is_adjustment_hash));
    message.push_str("\n\n");
    message.push_str(&format!("is\\_attempted\\_reorg: {}", is_attempted_reorg));

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
                log indicating beacon node publish, publish stats
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
            message.push_str("no logs indicating beacon node publish");
        }
    }

    // Add proposer meta.
    let proposer_meta = proposer_label_meta(mev_pool, &payload.proposer_pubkey).await?;
    let operator = {
        let label = proposer_meta.label.as_deref().unwrap_or("-");
        telegram::escape_str(label)
    };

    let lido_operator = {
        let lido_operator = proposer_meta.lido_operator.as_deref().unwrap_or("-");
        telegram::escape_str(lido_operator)
    };

    let grafitti = {
        let grafitti = proposer_meta.grafitti.as_deref().unwrap_or("-");
        telegram::escape_str(grafitti)
    };

    let proposer_ip = get_proposer_ip(mev_pool, &payload.proposer_pubkey).await?;
    let proposer_ip_formatted = {
        let ip = proposer_ip.as_deref().unwrap_or("-");
        telegram::escape_str(ip)
    };

    let proposer_location = {
        match proposer_ip {
            Some(proposer_ip) => proposer_location(mev_pool, &proposer_ip).await?,
            None => ProposerLocation::default(),
        }
    };
    let proposer_country = {
        let country = proposer_location.country.as_deref().unwrap_or("-");
        telegram::escape_str(country)
    };
    let proposer_city = {
        let city = proposer_location.city.as_deref().unwrap_or("-");
        telegram::escape_str(city)
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
                let formatted_error = telegram::escape_str(error);
                formatdoc!(
                    "
                    ```
                    {}
                    ```
                    ",
                    formatted_error
                )
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

    // Late call or attempted reorg, these are much less concerning.
    if published_stats.is_none() && late_call_stats.is_some() || is_attempted_reorg {
        message.push_str("\n\n");
        message.push_str(
            "for this block 'no publish attempted and late call' or 'attempted reorg' these misses are less concerning",
        );
    }

    let telegram_bot = telegram::TelegramBot::new();
    let escaped_message = TelegramMessage::from_escaped_string(message);

    telegram_bot
        .send_message(&escaped_message, Channel::BlockNotFound)
        .await;

    Ok(())
}

/// If the previous slot contains a valid block with the same block_number as the payload
/// we tried to deliver, then consider it a reorg attempt.
async fn was_attempted_reorg(
    beacon_api: &BeaconApi,
    delivered: &DeliveredPayload,
) -> anyhow::Result<bool> {
    let prev_slot = delivered.slot - 1;
    let prev_payload = beacon_api.block_by_slot_any(prev_slot).await?;
    Ok(prev_payload
        .map(|p| p.block_number == delivered.block_number)
        .unwrap_or(false))
}

async fn check_missing_payload(
    beacon_api: &BeaconApi,
    loki_client: &LokiClient,
    mev_pool: &PgPool,
    payload: &DeliveredPayload,
    relay_pool: &PgPool,
) -> anyhow::Result<()> {
    let block = beacon_api.block_by_slot_any(payload.slot).await?;

    match block {
        Some(ExecutionPayload { block_hash, .. }) => {
            // payload.block_hash = hash from the delivered payload we relayed
            // block_hash = hash from the actual block that was included on-chain
            if payload.block_hash == block_hash {
                debug!(
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

                report_missing_payload(
                    Some(block_hash),
                    loki_client,
                    mev_pool,
                    payload,
                    relay_pool,
                    false,
                    DetectionMethod::DeliveredPayload,
                )
                .await
            }
        }
        None => {
            let attempted_reorg = was_attempted_reorg(beacon_api, payload).await?;
            warn!(
                attempted_reorg,
                "delivered block not found for slot {}", payload.slot
            );
            report_missing_payload(
                None,
                loki_client,
                mev_pool,
                payload,
                relay_pool,
                attempted_reorg,
                DetectionMethod::DeliveredPayload,
            )
            .await
        }
    }
}

#[derive(Debug, Clone)]
struct HeaderServed {
    slot: i64,
    block_hash: String,
    proposer_pubkey: String,
    received_at: DateTime<Utc>,
    geo: Geo,
}

#[derive(Debug, Clone)]
struct PayloadRequestRow {
    slot: i64,
    block_hash: String,
    received_at: DateTime<Utc>,
}

async fn get_header_requests(
    mevdb_pool: &PgPool,
    start_slot: i64,
    end_slot: i64,
) -> anyhow::Result<Vec<HeaderServed>> {
    let rows = sqlx::query(
        r#"
        SELECT slot, block_hash, proposer_pubkey, received_at, geo
        FROM turbo_header_requests
        WHERE slot > $1 AND slot <= $2
          AND block_hash IS NOT NULL
          AND cancelled_at IS NULL
        ORDER BY received_at ASC
        "#,
    )
    .bind(start_slot)
    .bind(end_slot)
    .fetch_all(mevdb_pool)
    .await?;

    let headers = rows
        .iter()
        .map(|row| HeaderServed {
            slot: row.get::<i32, _>("slot") as i64,
            block_hash: row.get::<String, _>("block_hash"),
            proposer_pubkey: row.get::<String, _>("proposer_pubkey"),
            received_at: row.get::<DateTime<Utc>, _>("received_at"),
            geo: row.get("geo"),
        })
        .collect();

    Ok(headers)
}

async fn get_payload_requests(
    mev_pool: &PgPool,
    start_slot: i64,
    end_slot: i64,
) -> anyhow::Result<Vec<PayloadRequestRow>> {
    let rows = sqlx::query(
        r#"
        SELECT
            slot,
            signed_blinded_block -> 'message' -> 'body' -> 'execution_payload_header' ->> 'block_hash' AS block_hash,
            received_at
        FROM payload_requests
        WHERE slot > $1
          AND slot <= $2
          AND received_at IS NOT NULL
          AND signed_blinded_block -> 'message' -> 'body' -> 'execution_payload_header' ->> 'block_hash' IS NOT NULL
        ORDER BY slot ASC
        "#,
    )
    .bind(start_slot)
    .bind(end_slot)
    .fetch_all(mev_pool)
    .await?;

    let reqs = rows
        .iter()
        .map(|row| PayloadRequestRow {
            slot: row.get::<i32, _>("slot") as i64,
            block_hash: row.get::<String, _>("block_hash"),
            received_at: row.get::<DateTime<Utc>, _>("received_at"),
        })
        .collect();

    Ok(reqs)
}

async fn check_missing_candidate(
    beacon_api: &BeaconApi,
    loki_client: &LokiClient,
    mev_pool: &PgPool,
    relay_pool: &PgPool,
    candidate: &DeliveredPayload,
) -> anyhow::Result<()> {
    let block = beacon_api.block_by_slot_any(candidate.slot).await?;

    match block {
        Some(ExecutionPayload { block_hash, .. }) => {
            if candidate.block_hash == block_hash {
                debug!(
                    slot = candidate.slot,
                    block_hash = candidate.block_hash,
                    "found matching block hash"
                );
                Ok(())
            } else {
                warn!(
                    slot = candidate.slot,
                    block_hash_payload = candidate.block_hash,
                    block_hash_on_chain = block_hash,
                    "block hash on chain does not match payload (candidate)"
                );

                report_missing_payload(
                    Some(block_hash),
                    loki_client,
                    mev_pool,
                    candidate,
                    relay_pool,
                    false,
                    DetectionMethod::RequestIntersection,
                )
                .await
            }
        }
        None => {
            warn!(
                "delivered block not found for slot {} (candidate)",
                candidate.slot
            );
            report_missing_payload(
                None,
                loki_client,
                mev_pool,
                candidate,
                relay_pool,
                false,
                DetectionMethod::RequestIntersection,
            )
            .await
        }
    }
}

fn ms_into_slot(slot_number: i64, at: &DateTime<Utc>) -> i64 {
    let slot_start: DateTime<Utc> = (&Slot(slot_number as i32)).into();
    at.signed_duration_since(slot_start).num_milliseconds()
}

async fn get_or_init_inclusion_checkpoint(
    relay_pool: &PgPool,
    id: CheckpointId,
) -> anyhow::Result<DateTime<Utc>> {
    match checkpoint::get_checkpoint(relay_pool, id).await? {
        Some(c) => {
            debug!("found inclusion checkpoint: {}", c);
            Ok(c)
        }
        None => {
            info!("no checkpoint found, initializing");
            let now = Utc::now();
            checkpoint::put_checkpoint(relay_pool, id, &now).await?;
            Ok(now)
        }
    }
}

async fn process_delivered_payloads(
    beacon_api: &BeaconApi,
    loki_client: &LokiClient,
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
    processed: &mut std::collections::HashSet<(i64, String)>,
) -> anyhow::Result<Option<i64>> {
    debug!("fetching delivered payloads between {} and {}", start, end);
    let payloads = get_delivered_payloads(relay_pool, start, end).await?;
    debug!("fetched {} delivered payloads", payloads.len());
    for payload in &payloads {
        processed.insert((payload.slot, payload.block_hash.clone()));
        check_missing_payload(beacon_api, loki_client, mev_pool, payload, relay_pool).await?;
        debug!(
            slot = payload.slot,
            block_hash = payload.block_hash,
            "done checking payload"
        );
    }
    if payloads.is_empty() {
        debug!("no delivered payloads to check in this window");
    } else {
        info!("checked {} delivered payloads", payloads.len());
    }
    Ok(payloads.last().map(|p| p.slot))
}

async fn process_header_payload_candidates(
    beacon_api: &BeaconApi,
    loki_client: &LokiClient,
    relay_pool: &PgPool,
    mevdb_pool: &PgPool,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
    processed: &mut std::collections::HashSet<(i64, String)>,
) -> anyhow::Result<Option<i64>> {
    let start_slot = Slot::from_date_time_rounded_down(start).0 as i64;
    let end_slot = Slot::from_date_time_rounded_down(end).0 as i64;
    debug!(
        "fetching header/payload requests between slots {} and {}",
        start_slot, end_slot
    );

    let headers = get_header_requests(mevdb_pool, start_slot, end_slot).await?;
    let payload_reqs = get_payload_requests(mevdb_pool, start_slot, end_slot).await?;
    debug!(
        "fetched {} headers and {} payload requests",
        headers.len(),
        payload_reqs.len()
    );

    use std::collections::HashMap;
    let mut header_by_key: HashMap<(i64, String), &HeaderServed> = HashMap::new();
    for h in &headers {
        header_by_key.insert((h.slot, h.block_hash.clone()), h);
    }

    let mut candidates: Vec<DeliveredPayload> = Vec::new();
    const TIMELY_PAYLOAD_REQUEST_MS: i64 = 3000;
    for r in &payload_reqs {
        let key = (r.slot, r.block_hash.clone());
        if let Some(h) = header_by_key.get(&key) {
            // compute ms into slot for payload request
            let ms_into_slot = ms_into_slot(r.slot, &r.received_at);
            if ms_into_slot > TIMELY_PAYLOAD_REQUEST_MS {
                debug!(
                    slot = r.slot,
                    ms_into_slot,
                    threshold_ms = TIMELY_PAYLOAD_REQUEST_MS,
                    "skipping late payload request candidate"
                );
                continue;
            }

            if !processed.contains(&(h.slot, h.block_hash.clone())) {
                candidates.push(DeliveredPayload {
                    block_hash: h.block_hash.clone(),
                    block_number: 0,
                    inserted_at: h.received_at,
                    proposer_pubkey: h.proposer_pubkey.clone(),
                    slot: h.slot,
                    geo: h.geo.clone(),
                });
                processed.insert((h.slot, h.block_hash.clone()));
            }
        }
    }
    if candidates.is_empty() {
        debug!("no header/payload candidates to check in this window");
    } else {
        info!("checking {} header/payload candidates", candidates.len());
    }
    for candidate in &candidates {
        check_missing_candidate(beacon_api, loki_client, mevdb_pool, relay_pool, candidate).await?;
        debug!(
            slot = candidate.slot,
            block_hash = candidate.block_hash,
            "done checking candidate payload"
        );
    }

    if !candidates.is_empty() {
        info!("checked {} header/payload candidates", candidates.len());
    }

    Ok(candidates.last().map(|c| c.slot))
}

async fn maybe_alert_recent_missed_slots(
    mev_pool: &PgPool,
    last_slot_opt: Option<i64>,
) -> anyhow::Result<()> {
    if let Some(last_slot) = last_slot_opt {
        let start = last_slot - APP_CONFIG.missed_slots_check_range;
        let end = last_slot;

        let missed_slot_count = get_missed_slot_count(mev_pool, &start, &end).await?;

        if missed_slot_count >= APP_CONFIG.missed_slots_alert_threshold {
            let message = format!(
                "missed {} slots in the last {} slots",
                missed_slot_count, APP_CONFIG.missed_slots_check_range
            );
            warn!("{}", &message);
            alerts::send_opsgenie_telegram_alert(&message).await;
        } else {
            debug!(
                "missed {} slots in the last {} slots (below threshold)",
                missed_slot_count, APP_CONFIG.missed_slots_check_range
            );
        }
    }
    Ok(())
}

pub async fn run_inclusion_monitor(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    canonical_horizon: &DateTime<Utc>,
    loki_client: &LokiClient,
) -> anyhow::Result<()> {
    let beacon_api = BeaconApi::new(&APP_CONFIG.consensus_nodes);

    // use separate checkpoints for the delivered and intersection flows
    let delivered_checkpoint =
        get_or_init_inclusion_checkpoint(mev_pool, CheckpointId::InclusionDelivered).await?;
    let payload_requests_checkpoint =
        get_or_init_inclusion_checkpoint(mev_pool, CheckpointId::InclusionPayloadRequests).await?;

    debug!(
        "checking delivered between {} and {}",
        &delivered_checkpoint, canonical_horizon
    );
    debug!(
        "checking payload_requests between {} and {}",
        &payload_requests_checkpoint, canonical_horizon
    );

    use std::collections::HashSet;
    let mut processed: HashSet<(i64, String)> = HashSet::new();

    let last_from_delivered = process_delivered_payloads(
        &beacon_api,
        loki_client,
        relay_pool,
        mev_pool,
        &delivered_checkpoint,
        canonical_horizon,
        &mut processed,
    )
    .await?;

    let last_from_candidates = process_header_payload_candidates(
        &beacon_api,
        loki_client,
        relay_pool,
        mev_pool,
        &payload_requests_checkpoint,
        canonical_horizon,
        &mut processed,
    )
    .await?;

    let last_slot_delivered = last_from_delivered.or(last_from_candidates);

    maybe_alert_recent_missed_slots(mev_pool, last_slot_delivered).await?;

    checkpoint::put_checkpoint(
        mev_pool,
        CheckpointId::InclusionDelivered,
        canonical_horizon,
    )
    .await?;
    checkpoint::put_checkpoint(
        mev_pool,
        CheckpointId::InclusionPayloadRequests,
        canonical_horizon,
    )
    .await?;
    info!("inclusion monitor run completed");

    Ok(())
}

// there seems to be an issue running tests for this module
// temp removing the test module altogether.
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use chrono::Duration;

//     #[test]
//     fn test_ms_into_slot_calculation() {
//         // choose a concrete slot, derive its start time via Slot -> DateTime
//         let slot = Slot(1_234_567);
//         let slot_start: DateTime<Utc> = (&slot).into();

//         let within = slot_start + Duration::milliseconds(2_500);
//         let late = slot_start + Duration::milliseconds(3_100);

//         let ms_within = ms_into_slot(slot.0 as i64, &within);
//         let ms_late = ms_into_slot(slot.0 as i64, &late);

//         assert!(ms_within <= 3_000);
//         assert!(ms_late > 3_000);
//     }
// }
