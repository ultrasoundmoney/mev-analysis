use std::fmt;

use anyhow::{anyhow, Result};
use reqwest::StatusCode;

use crate::phoenix::env::APP_CONFIG;

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

/// Formats a message to be compatible with the Telegram bot API.
/// Respect escaping as described in: https://core.telegram.org/bots/api#markdownv2-style
/// Respect character limit of 4096.
#[derive(Clone, Debug, PartialEq)]
pub struct TelegramSafeAlert(String);

const TELEGRAM_MAX_MESSAGE_LENGTH: usize = 4096;
// Leave a little room for the escape characters and unknowns.
pub const TELEGRAM_SAFE_MESSAGE_LENGTH: usize = TELEGRAM_MAX_MESSAGE_LENGTH - 2048;

impl TelegramSafeAlert {
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
                "telegram alert too long, truncating to {} characters",
                TELEGRAM_SAFE_MESSAGE_LENGTH
            );
            Self(input).slice_to_limit()
        } else {
            Self(input)
        }
    }
}

impl fmt::Display for TelegramSafeAlert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

enum NotificationType {
    Warning,
    Alert,
}

impl fmt::Display for NotificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationType::Warning => write!(f, "warning"),
            NotificationType::Alert => write!(f, "alert"),
        }
    }
}

#[derive(Clone)]
pub struct TelegramAlerts {
    client: reqwest::Client,
}

impl Default for TelegramAlerts {
    fn default() -> Self {
        Self::new()
    }
}

impl TelegramAlerts {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    async fn send_telegram_message(
        &self,
        notification_type: NotificationType,
        message: &str,
    ) -> Result<()> {
        let channel_id = match notification_type {
            NotificationType::Warning => APP_CONFIG.telegram_warnings_channel_id.as_str(),
            NotificationType::Alert => APP_CONFIG.telegram_alerts_channel_id.as_str(),
        };

        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            APP_CONFIG.telegram_api_key
        );

        let response = self
            .client
            .get(&url)
            .query(&[
                ("chat_id", channel_id),
                ("text", message),
                ("parse_mode", "MarkdownV2"),
                ("disable_web_page_preview", "true"),
            ])
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {
                tracing::debug!(%notification_type, message, "sent telegram message");
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

    pub async fn send_warning(&self, message: &TelegramSafeAlert) {
        let result = self
            .send_telegram_message(NotificationType::Warning, &message.0)
            .await;

        if let Err(err) = result {
            tracing::error!(?err, "failed to send telegram warning");
        }
    }

    async fn send_alert(&self, message: &TelegramSafeAlert) -> anyhow::Result<()> {
        self.send_telegram_message(NotificationType::Alert, &message.0)
            .await
    }

    /// Allows to send a telegram alert, with retry, and a simple fallback in case the passed message
    /// fails to be delivered. Telegram has very sensitive rules about escaping. We may also at times
    /// be rate limited.
    pub async fn send_alert_with_fallback(&self, message: &TelegramSafeAlert) {
        for index in 0..3 {
            let message = if index == 2 {
                // Last attempt. This message intentionally does not contain *any* special
                // characters as many require escaping, and is within the character limit.
                TelegramSafeAlert::new("failed to send telegram alert please check logs")
            } else {
                message.clone()
            };

            // We may be timing out, if this is not our first attempt, wait a bit.
            if index != 0 {
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            };

            let send_result = self.send_alert(&message).await;

            match send_result {
                Ok(_) => {
                    tracing::debug!(%message, "sent telegram alert");
                    return;
                }
                Err(err) => {
                    tracing::error!(
                        attempt = index,
                        %message,
                        %err,
                        "failed to send telegram alert"
                    );
                }
            }
        }
    }
}
