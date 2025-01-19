use std::{collections::HashMap, fmt, sync::LazyLock};

use anyhow::{anyhow, Result};
use reqwest::StatusCode;
use tracing::error;

use crate::phoenix::env::APP_CONFIG;

pub static BUILDER_ID_CHANNEL_ID_MAP: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    vec![
        ("titan".to_string(), "-1002036721274".to_string()),
        ("beaverbuild".to_string(), "-100614386130".to_string()),
        (
            "beaverbuild-staging".to_string(),
            "-100614386130".to_string(),
        ),
    ]
    .into_iter()
    .collect()
});

// Used to escape characters in telegram messages.
// https://core.telegram.org/bots/api#markdownv2-style
pub fn escape_str(input: &str) -> String {
    let mut output = String::new();
    for c in input.chars() {
        match c {
            '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|'
            | '{' | '}' | '.' | '!' => {
                output.push('\\');
            }
            _ => (),
        };
        output.push(c);
    }
    output
}

/// Formats a message to be compatible with the Telegram bot API.
/// Respect escaping as described in: https://core.telegram.org/bots/api#markdownv2-style
/// Respect character limit of 4096.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramMessage(String);

const TELEGRAM_MAX_MESSAGE_LENGTH: usize = 4096;
// Leave a little room for the escape characters and unknowns.
pub const TELEGRAM_SAFE_MESSAGE_LENGTH: usize = TELEGRAM_MAX_MESSAGE_LENGTH - 2048;

impl TelegramMessage {
    pub fn new(input: &str) -> Self {
        let escaped = escape_str(input);
        Self::from_escaped_string(escaped)
    }

    fn slice_to_limit(self) -> Self {
        Self(self.0.chars().take(TELEGRAM_SAFE_MESSAGE_LENGTH).collect())
    }

    pub fn from_escaped_string(input: String) -> Self {
        if input.len() > TELEGRAM_SAFE_MESSAGE_LENGTH {
            tracing::warn!(
                "telegram message too long, truncating to {} characters",
                TELEGRAM_SAFE_MESSAGE_LENGTH
            );
            Self(input).slice_to_limit()
        } else {
            Self(input)
        }
    }
}

impl fmt::Display for TelegramMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Channel {
    Alerts,
    BlockNotFound,
    Demotions,
    Warnings,
    Id(String),
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Channel::Alerts => write!(f, "alerts"),
            Channel::BlockNotFound => write!(f, "block not found"),
            Channel::Demotions => write!(f, "demotions"),
            Channel::Warnings => write!(f, "warnings"),
            Channel::Id(id) => write!(f, "{}", id),
        }
    }
}

#[derive(Clone)]
pub struct TelegramBot {
    client: reqwest::Client,
}

impl Default for TelegramBot {
    fn default() -> Self {
        Self::new()
    }
}

impl TelegramBot {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    async fn send_message_request(
        &self,
        channel: &Channel,
        message: &str,
        button_url: Option<&str>,
    ) -> Result<()> {
        let channel_id = match channel {
            Channel::Alerts => APP_CONFIG.telegram_alerts_channel_id.clone(),
            Channel::BlockNotFound => APP_CONFIG.telegram_block_not_found_channel_id.clone(),
            Channel::Demotions => APP_CONFIG.telegram_demotions_channel_id.clone(),
            Channel::Warnings => APP_CONFIG.telegram_warnings_channel_id.clone(),
            Channel::Id(id) => id.clone(),
        };

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            APP_CONFIG.telegram_api_key
        );

        let mut json_body = serde_json::json!({
            "chat_id": channel_id,
            "text": message,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": true,
        });

        // Add button only for demotion alerts with a provided button URL
        if let Some(url) = button_url {
            json_body["reply_markup"] = serde_json::json!({
                "inline_keyboard": [[{"text": "repromote", "url": url}]]
            });
        }

        let response = self.client.post(&url).json(&json_body).send().await?;

        match response.status() {
            StatusCode::OK => {
                tracing::debug!(%channel, message, "sent telegram message");
                Ok(())
            }
            StatusCode::BAD_REQUEST => {
                let body = response.text().await?;
                Err(anyhow!("failed to send telegram message: {}", body))
            }
            _ => Err(anyhow!(
                "failed to send telegram message, status: {:?}",
                response.status()
            )),
        }
    }

    /// Send a telegram message with various precautions.
    ///
    /// Messages are expected to be quite important like alerts. Messages will be retried.
    /// If retries fail, a simple fallback message will be sent.
    async fn send_message_with_retry(
        &self,
        message: &TelegramMessage,
        channel: Channel,
        button_url: Option<&str>,
    ) {
        // Retry twice, with a delay in between.
        for index in 0..3 {
            let send_result = self
                .send_message_request(&channel, &message.0, button_url)
                .await;

            match send_result {
                Ok(_) => {
                    tracing::debug!(%message, "sent telegram message");
                    return;
                }
                Err(err) => {
                    tracing::error!(
                        attempt = index,
                        %message,
                        %err,
                        "failed to send telegram message"
                    );

                    // We did not succeed, wait then move on to the next attempt.
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                }
            }
        }

        // Last attempt. This message intentionally does not contain *any* special
        // characters as many require escaping, and is within the character limit.
        let message = TelegramMessage::new("failed to send telegram message please check logs");
        self.send_message_request(&channel, &message.0, None)
            .await
            .ok();
    }

    /// Send a simple telegram message to any channel.
    pub async fn send_message(&self, message: &TelegramMessage, channel: Channel) {
        self.send_message_with_retry(message, channel, None).await;
    }

    pub async fn send_message_to_builder(
        &self,
        message: &TelegramMessage,
        builder_id: &str,
        button_url: Option<&str>,
    ) {
        match BUILDER_ID_CHANNEL_ID_MAP.get(builder_id) {
            Some(channel_id) => {
                self.send_message_with_retry(message, Channel::Id(channel_id.clone()), button_url)
                    .await;
            }
            None => {
                error!("failed to find channel_id for builder_id: {}", builder_id);
                let fallback_message =
                    TelegramMessage::new("failed to find channel_id, please check logs");
                self.send_message_with_retry(&fallback_message, Channel::Alerts, None)
                    .await;
            }
        }
    }

    /// Send a demotion message with a button to the Demotions channel.
    pub async fn send_demotion_with_button(&self, message: &TelegramMessage, button_url: &str) {
        self.send_message_with_retry(message, Channel::Demotions, Some(button_url))
            .await;
    }
}
