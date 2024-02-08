use anyhow::anyhow;
use reqwest::StatusCode;
use tracing::debug;

use super::env::APP_CONFIG;

// Used to escape characters in telegram messages.
pub fn telegram_escape(input: &str) -> String {
    let mut output = String::new();
    for c in input.chars() {
        match c {
            '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|'
            | '{' | '}' | '.' | '!' => {
                output.push('\\');
            }
            _ => {}
        }
        output.push(c);
    }
    output
}

// Used to escape characters inside markdown code blocks
// https://core.telegram.org/bots/api#markdownv2-style
pub fn escape_code_block(input: &str) -> String {
    let mut output = String::new();
    for c in input.chars() {
        match c {
            '`' | '\\' => {
                output.push('\\');
            }
            _ => {}
        }
        output.push(c);
    }
    output
}

/// Sends a message to the Telegram warnings channel.
/// Make sure the message is escaped.
pub async fn send_telegram_warning(message: &str) -> anyhow::Result<()> {
    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        &APP_CONFIG.telegram_api_key
    );

    let response = reqwest::Client::new()
        .get(&url)
        .query(&[
            ("chat_id", APP_CONFIG.telegram_warnings_channel_id.as_str()),
            ("text", message),
            ("parse_mode", "MarkdownV2"),
            ("disable_web_page_preview", "true"),
        ])
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => {
            debug!(message, "sent telegram warning");
            Ok(())
        }
        StatusCode::BAD_REQUEST => {
            let body = response.text().await?;
            Err(anyhow!(
                "failed to send telegram warning: {}, message: {}",
                body,
                message
            ))
        }
        status => Err(anyhow!(
            "failed to send telegram warning, status: {:?}, message: {}",
            status,
            message
        )),
    }
}

/// Sends a message to the Telegram alerts channel.
/// Make sure the message is escaped.
pub async fn send_telegram_alert(message: &str) -> anyhow::Result<()> {
    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        &APP_CONFIG.telegram_api_key
    );

    let response = reqwest::Client::new()
        .get(&url)
        .query(&[
            ("chat_id", APP_CONFIG.telegram_alerts_channel_id.as_str()),
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
            Err(anyhow!(
                "failed to send telegram alert: {}, message: {}",
                body,
                message
            ))
        }
        _ => Err(anyhow!(
            "failed to send telegram alert, status: {:?}, message: {}",
            response.status(),
            message
        )),
    }
}
