use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};

type JsonValue = serde_json::Value;

/// Statistics on payloads requested. Used to determine if a payload which failed to make it
/// on-chain should concern us.
#[derive(Debug)]
pub struct PublishedPayloadStats {
    pub decoded_at_slot_age_ms: i64,
    pub pre_publish_duration_ms: i64,
    // The time it took to call our consensus node and have it publish the block.
    pub publish_duration_ms: i64,
    pub request_download_duration_ms: i64,
}

/// Statistics on payloads which were requested too late. Used to determine if a payload which
/// failed to make it on-chain should concern us.
#[derive(Debug)]
pub struct LatePayloadStats {
    pub decoded_at_slot_age_ms: i64,
    pub request_download_duration_ms: i64,
}

fn extract_date_time(json_map: &JsonValue, key: &str) -> anyhow::Result<DateTime<Utc>> {
    json_map
        .get(key)
        .and_then(|timestamp| timestamp.as_i64())
        .and_then(|timestamp| Utc.timestamp_millis_opt(timestamp).single())
        .with_context(|| {
            format!(
                "failed to parse {} as timestamp from payload published log",
                key
            )
        })
}

pub fn published_stats_from_logs(
    logs: &[JsonValue],
) -> anyhow::Result<Option<PublishedPayloadStats>> {
    match logs.first() {
        None => Ok(None),
        Some(payload_published_log) => {
            let received_at = extract_date_time(payload_published_log, "timestampRequestStart")?;
            let decoded_at = extract_date_time(payload_published_log, "timestampAfterDecode")?;
            let pre_publish_at =
                extract_date_time(payload_published_log, "timestampBeforePublishing")?;
            let decoded_at_slot_age_ms = payload_published_log["msIntoSlot"]
                .as_i64()
                .context("failed to parse msIntoSlot as i64")?;

            let pre_publish_duration_ms = pre_publish_at
                .signed_duration_since(received_at)
                .num_milliseconds();

            let publish_duration_ms = payload_published_log
                .get("msNeededForPublishing")
                .and_then(|timestamp| timestamp.as_i64())
                .context("failed to parse msNeededForPublishing as i64")?;

            let request_download_duration_ms = decoded_at
                .signed_duration_since(received_at)
                .num_milliseconds();

            let payload_log_stats = PublishedPayloadStats {
                decoded_at_slot_age_ms,
                pre_publish_duration_ms,
                publish_duration_ms,
                request_download_duration_ms,
            };

            Ok(Some(payload_log_stats))
        }
    }
}

pub fn late_call_stats_from_logs(logs: &[JsonValue]) -> anyhow::Result<Option<LatePayloadStats>> {
    match logs.first() {
        None => Ok(None),
        Some(late_call_log) => {
            let received_at = extract_date_time(late_call_log, "timestampRequestStart")?;
            let decoded_at = extract_date_time(late_call_log, "timestampAfterDecode")?;
            let decoded_at_slot_age_ms = late_call_log["msIntoSlot"]
                .as_i64()
                .context("failed to parse msIntoSlot as i64")?;

            let request_download_duration_ms = decoded_at
                .signed_duration_since(received_at)
                .num_milliseconds();

            let late_payload_log_stats = LatePayloadStats {
                decoded_at_slot_age_ms,
                request_download_duration_ms,
            };

            Ok(Some(late_payload_log_stats))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use crate::phoenix::inclusion_monitor::loki_client::loki_res_into_json_logs;

    use super::*;

    #[test]
    fn parse_published_log_test() {
        let str = File::open(
            "src/phoenix/inclusion_monitor/loki_client/test_data/published_8371236.json",
        )
        .map(|mut file| {
            let mut str = String::new();
            file.read_to_string(&mut str).unwrap();
            str
        })
        .unwrap();

        let logs = loki_res_into_json_logs(&str).unwrap();
        published_stats_from_logs(&logs).unwrap();
    }

    #[test]
    fn parse_late_call_log_test() {
        let str = File::open(
            "src/phoenix/inclusion_monitor/loki_client/test_data/late_call_8365873.json",
        )
        .map(|mut file| {
            let mut str = String::new();
            file.read_to_string(&mut str).unwrap();
            str
        })
        .unwrap();

        let logs = loki_res_into_json_logs(&str).unwrap();
        late_call_stats_from_logs(&logs).unwrap();
    }
}
