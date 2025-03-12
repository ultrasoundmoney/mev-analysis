mod slot;
mod stats;

use anyhow::Context;
use chrono::{DateTime, TimeZone, Utc};
use itertools::Itertools;
use reqwest::Url;
use tracing::warn;

use slot::Slot;
pub use stats::{LatePayloadStats, PublishedPayloadStats};

type JsonValue = serde_json::Value;

/// Parses loki query response into a list of log lines. Oldest log line is first.
fn loki_res_into_json_logs(text: &str) -> anyhow::Result<Vec<JsonValue>> {
    let log_response_json: JsonValue = match serde_json::from_str(text) {
        Ok(json) => json,
        Err(e) => {
            warn!("failed to parse payload log request body as json: {}", e);
            return Ok(Vec::new());
        }
    };

    // Loki queries many nodes, each may return a stream of logs. We first discard all metadata
    // leaving only a list of streams.
    let streams = log_response_json
        // The rest is response metadata
        .get("data")
        // These are the lines, the rest is metadata about the lines
        .and_then(|data| data.get("result"))
        .and_then(|result| result.as_array())
        .ok_or_else(|| anyhow::anyhow!("failed to parse payload log response"))?;

    // Each stream may contain many log lines. We discard the metadata and keep only the log lines.
    let log_values = streams
        .iter()
        .map(|line| {
            line.get("values")
                .and_then(|values| values.as_array())
                .ok_or_else(|| anyhow::anyhow!("failed to parse payload log response"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    // Each value is an array of two values: a timestamp and the log line.
    let log_tuples = log_values
        .iter()
        .map(|value| {
            let raw_timestamp = value
                .get(0)
                .and_then(|timestamp| timestamp.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("failed to parse timestamp from payload log response")
                })?;
            let timestamp: DateTime<Utc> = raw_timestamp
                .parse::<i64>()
                .map(|timestamp| Utc.timestamp_nanos(timestamp))
                .context("failed to parse nanosecond timestamp as i64")?;
            let log_str = value
                .get(1)
                .and_then(|log| log.as_str())
                .ok_or_else(|| anyhow::anyhow!("failed to parse log from payload log response"))?;
            let log_value = serde_json::from_str(log_str).context("failed to parse log as JSON")?;
            anyhow::Ok((timestamp, log_value))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let log_lines = log_tuples
        .into_iter()
        .sorted_by_key(|(timestamp, _)| *timestamp)
        .map(|(_, log)| log)
        .collect::<Vec<_>>();

    Ok(log_lines)
}

fn errors_from_logs(logs: &[JsonValue]) -> anyhow::Result<Vec<String>> {
    logs.iter()
        .map(|log| log["msg"].as_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| anyhow::anyhow!("failed to parse error logs"))
}

pub struct LokiClient {
    client: reqwest::Client,
    server_url: String,
}

/// Query loki for stats related to the publishing of payloads.
/// See the tests for an example of the data.
impl LokiClient {
    pub fn new(server_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            server_url,
        }
    }

    pub async fn published_stats(
        &self,
        slot: i64,
    ) -> anyhow::Result<Option<PublishedPayloadStats>> {
        let query = format!(
            r#"{{app="payload-api"}} |= `"slot":"{slot}"` |= "block published through beacon node""#
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

        let logs = loki_res_into_json_logs(&body)?;
        stats::published_stats_from_logs(&logs)
    }

    pub async fn late_call_stats(&self, slot: i64) -> anyhow::Result<Option<LatePayloadStats>> {
        let query = format!(
            r#"{{app="payload-api",level="warning"}} |= `"slot":"{slot}"` |= "getPayload sent too late""#
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

        let logs = loki_res_into_json_logs(&body)?;
        stats::late_call_stats_from_logs(&logs)
    }

    pub async fn error_messages(&self, slot: i64) -> anyhow::Result<Vec<String>> {
        let query = format!(r#"{{app="payload-api",level="error"}} |= `"slot":"{slot}"`"#);
        let slot = Slot(slot as i32);
        let start = slot
            .date_time()
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("Start Time out of range for nanosecond timestamp"))?;
        let end = (slot.date_time() + chrono::Duration::seconds(12))
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("End Time out of range for nanosecond timestamp"))?;

        let url = format!("{}/loki/api/v1/query_range", self.server_url);
        let url_with_params = Url::parse_with_params(
            &url,
            &[
                ("direction", "forward"),
                ("query", query.as_str()),
                ("start", start.to_string().as_str()),
                ("end", end.to_string().as_str()),
            ],
        )?;

        let response = self.client.get(url_with_params).send().await?;
        let body = response.text().await?;

        let logs = loki_res_into_json_logs(&body)?;
        errors_from_logs(&logs)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use super::*;

    #[test]
    fn parse_into_logs_test() {
        let str = File::open(
            "src/phoenix/inclusion_monitor/loki_client/test_data/late_call_8365873.json",
        )
        .map(|mut file| {
            let mut str = String::new();
            file.read_to_string(&mut str).unwrap();
            str
        })
        .unwrap();

        loki_res_into_json_logs(&str).unwrap();
    }

    #[test]
    fn error_messages_test() {
        let str =
            File::open("src/phoenix/inclusion_monitor/loki_client/test_data/error_8365565.json")
                .map(|mut file| {
                    let mut str = String::new();
                    file.read_to_string(&mut str).unwrap();
                    str
                })
                .unwrap();

        let logs = loki_res_into_json_logs(&str).unwrap();
        errors_from_logs(&logs).unwrap();
    }
}
