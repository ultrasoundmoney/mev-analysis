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

fn date_time_from_timestamp(
    request_finished_log: &serde_json::Value,
    key: &str,
) -> anyhow::Result<DateTime<Utc>> {
    request_finished_log[key]
        .as_str()
        .and_then(|timestamp| timestamp.parse::<i64>().ok())
        .and_then(|timestamp| Utc.timestamp_millis_opt(timestamp).single())
        .with_context(|| format!("failed to parse {key} as timestamp from payload log"))
}

fn parse_log_response(text: &str) -> anyhow::Result<Option<PayloadLogStats>> {
    let request_finished_log: serde_json::Value = {
        let log_data: serde_json::Value = serde_json::from_str(text)
            .context("failed to parse payload log request body as JSON")?;

        // This is the array of parsed log lines and their raw values.
        let results = log_data["data"]["result"]
            .as_array()
            .context("expected at least one log line in payload logs response")?;

        let log_data = results
            .iter()
            .find(|result| {
                let stream = &result["stream"];
                let msg = stream["msg"].as_str().unwrap_or("");
                msg.contains("request finished")
            })
            .map(|result| &result["stream"])
            .cloned();

        match log_data {
            Some(log_data) => log_data,
            // If there are no logs, we stop here.
            None => return Ok(None),
        }
    };

    let received_at = date_time_from_timestamp(&request_finished_log, "timestampRequestStart")?;
    let decoded_at = date_time_from_timestamp(&request_finished_log, "timestampAfterDecode")?;
    let pre_publish_at =
        date_time_from_timestamp(&request_finished_log, "timestampBeforePublishing")?;
    let post_publish_at =
        date_time_from_timestamp(&request_finished_log, "timestampAfterPublishing")?;
    let decoded_at_slot_age_ms = request_finished_log["msIntoSlot"]
        .as_str()
        .and_then(|s| s.parse::<i64>().ok())
        .context("failed to parse msIntoSlot as i64")?;

    let pre_publish_duration_ms = pre_publish_at
        .signed_duration_since(received_at)
        .num_milliseconds();

    let publish_duration_ms = post_publish_at
        .signed_duration_since(pre_publish_at)
        .num_milliseconds();

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

    pub async fn payload_logs(&self, slot: &i32) -> anyhow::Result<Option<PayloadLogStats>> {
        let query = format!(r#"{{app="proposer-api"}} |= `"slot":{slot}` | json"#);
        let since = "24h";

        let url = format!("{}/loki/api/v1/query_range", self.server_url);
        let url_with_params =
            Url::parse_with_params(&url, &[("query", query.as_str()), ("since", since)])?;

        let response = self.client.get(url_with_params).send().await?;
        let body = response.text().await?;

        parse_log_response(&body)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use super::*;

    #[test]
    fn parse_log_response() {
        let str = File::open("src/phoenix/inclusion_monitor/test_data/payload_logs_7496729.json")
            .map(|mut file| {
                let mut str = String::new();
                file.read_to_string(&mut str).unwrap();
                str
            })
            .unwrap();

        str.parse::<PayloadLogStats>().unwrap();
    }
}
