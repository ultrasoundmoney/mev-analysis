mod opsgenie;
pub mod telegram;

use tracing::{debug, error};

use crate::env::Network;

pub use self::telegram::TelegramSafeAlert;

use super::env::APP_CONFIG;

/// Ability to communicate an alert to a dev.
#[trait_variant::make(SendAlert: Send)]
#[allow(dead_code)]
pub trait LocalSendAlert {
    async fn send_warning(&self, message: TelegramSafeAlert);
    async fn send_alert(&self, message: TelegramSafeAlert);
}

pub async fn send_opsgenie_telegram_alert(message: &str) {
    let telegram_alerts = telegram::TelegramAlerts::new();

    SendAlert::send_alert(&telegram_alerts, TelegramSafeAlert::new(message)).await;

    if APP_CONFIG.network == Network::Mainnet {
        let result_send_opsgenie_alert = opsgenie::send_opsgenie_alert(message).await;
        match result_send_opsgenie_alert {
            Ok(_) => {
                debug!(message, "sent OpsGenie alert");
            }
            Err(err) => {
                error!(?err, "failed to send OpsGenie alert");

                let escaped_err = telegram::escape_str(&err.to_string());
                let message = {
                    let message = format!("failed to send OpsGenie alert: {}", escaped_err);
                    TelegramSafeAlert::from_escaped_string(message)
                };
                SendAlert::send_alert(&telegram_alerts, message).await;
            }
        }
    }
}
