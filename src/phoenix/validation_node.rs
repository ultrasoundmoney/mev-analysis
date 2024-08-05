use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tracing::{error, info, warn};

use super::{
    alerts::{
        telegram::{TelegramAlerts, TelegramSafeAlert},
        SendAlert,
    },
    env::APP_CONFIG,
    PhoenixMonitor,
};

#[derive(Deserialize)]
struct SyncResponse {
    result: bool,
}

async fn get_sync_status(client: &reqwest::Client, url: String) -> reqwest::Result<SyncResponse> {
    client
        .post(url)
        .json(&json!({"jsonrpc": "2.0", "method": "eth_syncing", "params": [], "id": 1}))
        .send()
        .await?
        .error_for_status()?
        .json::<SyncResponse>()
        .await
}

pub struct ValidationNodeMonitor {
    client: reqwest::Client,
    telegram_alerts: TelegramAlerts,
}

impl ValidationNodeMonitor {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            telegram_alerts: TelegramAlerts::new(),
        }
    }

    pub async fn get_current_timestamp(&self) -> Result<DateTime<Utc>> {
        let mut results = Vec::new();

        for url in &APP_CONFIG.validation_nodes {
            let status = get_sync_status(&self.client, url.to_string()).await;

            match status {
                Ok(s) => results.push(!s.result),
                Err(err) => {
                    error!("error getting validation node status: {}", err);
                    results.push(false)
                }
            }
        }

        let synced: Vec<&bool> = results.iter().filter(|is_synced| **is_synced).collect();

        info!("{}/{} validation nodes synced", synced.len(), results.len());
        let num_out_of_sync = results.len() - synced.len();

        if num_out_of_sync > 1 {
            Err(anyhow!("multiple validation nodes out of sync"))
        } else {
            if num_out_of_sync == 1 {
                warn!("one validation node is out of sync");
                let message = TelegramSafeAlert::new("one validation node is out of sync");
                self.telegram_alerts.send_warning(message).await;
            }
            Ok(Utc::now())
        }
    }
}

#[async_trait]
impl PhoenixMonitor for ValidationNodeMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>> {
        ValidationNodeMonitor::get_current_timestamp(self).await
    }
}
