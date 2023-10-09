use anyhow::{anyhow, Result};
use axum::http::{HeaderMap, HeaderValue};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use crate::{env::Env, phoenix::env::APP_CONFIG};

pub async fn send_telegram_alert(message: &str) -> Result<()> {
    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        &APP_CONFIG.telegram_api_key
    );
    let response = reqwest::Client::new()
        .get(&url)
        .query(&[
            ("chat_id", APP_CONFIG.telegram_channel_id.as_str()),
            ("text", message),
            ("parse_mode", "MarkdownV2"),
            ("disable_web_page_preview", "true"),
        ])
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => {
            debug!("sent telegram alert: {}", message);
            Ok(())
        }
        StatusCode::BAD_REQUEST => {
            let body = response.text().await?;
            Err(anyhow!("failed to send telegram alert: {}", body))
        }
        _ => Err(anyhow!(
            "failed to send telegram alert, status: {:?}",
            response.status()
        )),
    }
}

#[derive(Deserialize)]
struct OpsGenieError {
    message: String,
}

async fn send_opsgenie_alert(message: &str) -> Result<()> {
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

    if res.status() != 202 {
        match res.json::<OpsGenieError>().await {
            Err(_) => {
                panic!("failed to create alarm with OpsGenie")
            }
            Ok(body) => {
                panic!(
                    "failed to create alarm with OpsGenie, message: {}",
                    body.message
                )
            }
        }
    } else {
        Ok(())
    }
}

pub async fn send_alert(message: &str) -> Result<()> {
    if APP_CONFIG.env == Env::Prod {
        send_opsgenie_alert(message).await?;
    }
    send_telegram_alert(message).await?;

    Ok(())
}
