mod opsgenie;
pub mod telegram;

use telegram::Channel;
use tracing::{debug, error};

use crate::env::Network;

pub use self::telegram::TelegramMessage;

use super::env::APP_CONFIG;

/// Sends a telegram alert message, and if the network is Mainnet, also sends an OpsGenie alert.
pub async fn send_opsgenie_telegram_alert(message: &str) {
    let telegram_bot = telegram::TelegramBot::new();

    telegram_bot
        .send_message(&TelegramMessage::new(message), Channel::Alerts)
        .await;

    // Only send actual OpsGenie alerts on Mainnet.
    if APP_CONFIG.network == Network::Mainnet {
        let result_send_opsgenie_alert = opsgenie::send_opsgenie_alert(message).await;
        match result_send_opsgenie_alert {
            Ok(_) => {
                debug!(message, "sent OpsGenie alert");
            }
            // If sending the OpsGenie alert fails, log the error and send a telegram message.
            Err(err) => {
                error!(?err, "failed to send OpsGenie alert");

                let escaped_err = telegram::escape_str(&err.to_string());
                let message = {
                    let message = format!("failed to send OpsGenie alert: {}", escaped_err);
                    TelegramMessage::from_escaped_string(message)
                };
                telegram_bot.send_message(&message, Channel::Alerts).await;
            }
        }
    }
}
