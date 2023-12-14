use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use reqwest::Url;

/// Statistics on payloads requested. Used to determine if a payload which failed to make it
/// on-chain should concern us.
#[derive(Debug)]
pub struct PayloadLogStats {
    pub decoded_at_slot_age_ms: i64,
    pub pre_publish_duration_ms: i64,
    // The time it took to call our consensus node and have it publish the block.
    pub publish_duration_ms: i64,
    pub request_download_duration_ms: i64,
}

fn extract_date_time(json_map: &serde_json::Value, key: &str) -> anyhow::Result<DateTime<Utc>> {
    json_map
        .get(key)
        .and_then(|timestamp| timestamp.as_str())
        .and_then(|timestamp| timestamp.parse::<i64>().ok())
        .and_then(|timestamp| Utc.timestamp_millis_opt(timestamp).single())
        .with_context(|| {
            format!(
                "failed to parse {} as timestamp from payload published log",
                key
            )
        })
}

/// Takes the payload logs response and extracts the first log line. We rely on the query being
/// crafted in such a way that the oldest matching result is the one we're looking for.
fn parse_payload_published_log(text: &str) -> anyhow::Result<Option<PayloadLogStats>> {
    let payload_published_log: Option<serde_json::Value> = {
        let log_response_json: serde_json::Value = serde_json::from_str(text)
            .context("failed to parse payload log request body as JSON")?;

        // See the tests for an example of the data.
        log_response_json
            // The rest is response metadata
            .get("data")
            // These are the lines, the rest is metadata about the lines
            .and_then(|data| data.get("result"))
            .and_then(|result| result.as_array())
            .and_then(|lines| lines.iter().next())
            // Each of these lines is expected to contain parsed JSON values in the `stream` field and
            // raw values in the `values` field.
            .and_then(|line| line.get("stream"))
            .cloned()
    };

    match payload_published_log {
        None => Ok(None),
        Some(payload_published_log) => {
            let received_at = extract_date_time(&payload_published_log, "timestampRequestStart")?;
            let decoded_at = extract_date_time(&payload_published_log, "timestampAfterDecode")?;
            let pre_publish_at =
                extract_date_time(&payload_published_log, "timestampBeforePublishing")?;
            let decoded_at_slot_age_ms = payload_published_log["msIntoSlot"]
                .as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .context("failed to parse msIntoSlot as i64")?;

            let pre_publish_duration_ms = pre_publish_at
                .signed_duration_since(received_at)
                .num_milliseconds();

            let publish_duration_ms = payload_published_log
                .get("msNeededForPublishing")
                .and_then(|timestamp| timestamp.as_str())
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
                .context("failed to parse msNeededForPublishing as i64")?;

            let request_download_duration_ms = decoded_at
                .signed_duration_since(received_at)
                .num_milliseconds();

            let payload_log_stats = PayloadLogStats {
                decoded_at_slot_age_ms,
                pre_publish_duration_ms,
                publish_duration_ms,
                request_download_duration_ms,
            };

            Ok(Some(payload_log_stats))
        }
    }
}

pub struct LokiClient {
    client: reqwest::Client,
    server_url: String,
}

impl LokiClient {
    pub fn new(server_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            server_url,
        }
    }

    pub async fn payload_logs(&self, slot: i64) -> anyhow::Result<Option<PayloadLogStats>> {
        let query = format!(
            r#"{{app="payload-api"}} |= `"slot":{slot}` |= "block published through beacon node" | json"#
        );
        let since = "24h";

        let url = format!("{}/loki/api/v1/query_range", self.server_url);
        let url_with_params = Url::parse_with_params(
            &url,
            &[
                ("direction", "forward"),
                ("query", query.as_str()),
                ("since", since),
            ],
        )?;

        let response = self.client.get(url_with_params).send().await?;
        let body = response.text().await?;

        parse_payload_published_log(&body)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use super::*;

    #[test]
    fn parse_log_response_test() {
        let str = File::open("src/phoenix/inclusion_monitor/test_data/payload_logs_7496729.json")
            .map(|mut file| {
                let mut str = String::new();
                file.read_to_string(&mut str).unwrap();
                str
            })
            .unwrap();

        parse_payload_published_log(&str).unwrap();
    }
}
