use anyhow::Result;
use axum::http::{HeaderMap, HeaderValue};
use serde::Deserialize;
use serde_json::json;

use crate::{env::Env, phoenix::env::APP_CONFIG};

fn format_message(message: &str) -> String {
    format!("🚨🚨🚨 {} 🚨🚨🚨", message)
}

pub async fn send_telegram_alert(message: &str) -> Result<()> {
    let formatted = format_message(message);
    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        &APP_CONFIG.telegram_api_key
    );
    reqwest::Client::new()
        .get(&url)
        .query(&[
            ("chat_id", &APP_CONFIG.telegram_channel_id),
            ("text", &formatted.to_string()),
        ])
        .send()
        .await?;

    Ok(())
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

    let formatted = format_message(message);

    let res = reqwest::Client::new()
        .post("https://api.opsgenie.com/v2/alerts")
        .headers(headers)
        .json(&json!({ "message": formatted }))
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
