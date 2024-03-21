use anyhow::bail;
use axum::http::{HeaderMap, HeaderValue};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::phoenix::env::APP_CONFIG;

#[derive(Deserialize)]
struct OpsGenieError {
    message: String,
}

pub async fn send_opsgenie_alert(message: &str) -> anyhow::Result<()> {
    let mut headers = HeaderMap::new();
    let auth_header = format!("GenieKey {}", &APP_CONFIG.opsgenie_api_key);

    headers.insert(
        "Authorization",
        HeaderValue::from_str(&auth_header).unwrap(),
    );

    let res = reqwest::Client::new()
        .post("https://api.opsgenie.com/v2/alerts")
        .headers(headers)
        .json(&json!({ "message": message }))
        .send()
        .await?;

    match res.status() {
        StatusCode::ACCEPTED => {
            tracing::debug!(message, "sent opsgenie alert");
            Ok(())
        }
        status => match res.json::<OpsGenieError>().await {
            Err(err) => {
                bail!(
                    "failed to create alarm with OpsGenie, status: {}, err: {}",
                    status,
                    err
                )
            }
            Ok(body) => {
                bail!(
                    "failed to create alarm with OpsGenie, status: {:?}, message: {}",
                    status,
                    body.message
                )
            }
        },
    }
}
