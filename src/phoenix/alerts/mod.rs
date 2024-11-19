mod opsgenie;
pub mod telegram;

use tracing::{debug, error};

use crate::env::Network;

pub use self::telegram::TelegramSafeAlert;

use super::env::APP_CONFIG;

/// Sends a telegram alert message, and if the network is Mainnet, also sends an OpsGenie alert.
pub async fn send_opsgenie_telegram_alert(message: &str) {
    let telegram_alerts = telegram::TelegramAlerts::new();

    telegram_alerts
        .send_alert_with_fallback(&TelegramSafeAlert::new(message))
        .await;

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
                telegram_alerts.send_alert_with_fallback(&message).await;
            }
        }
    }
}
