use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{error, info, warn};

use super::{
    alerts::{
        telegram::{TelegramAlerts, TelegramSafeAlert},
        SendAlert,
    },
    env::APP_CONFIG,
    PhoenixMonitor,
};
use crate::beacon_api::BeaconApi;

pub struct ConsensusNodeMonitor {
    beacon_api: BeaconApi,
    telegram_alerts: TelegramAlerts,
}

impl ConsensusNodeMonitor {
    pub fn new() -> Self {
        Self {
            beacon_api: BeaconApi::new(&APP_CONFIG.consensus_nodes),
            telegram_alerts: TelegramAlerts::new(),
        }
    }

    pub async fn get_current_timestamp(&self) -> Result<DateTime<Utc>> {
        let mut results = Vec::new();

        for url in &APP_CONFIG.consensus_nodes {
            let status = self.beacon_api.fetch_sync_status(url).await;

            match status {
                Ok(s) => results.push(!s.is_syncing),
                Err(err) => {
                    error!("error getting consensus node status: {}", err);
                    results.push(false)
                }
            }
        }

        let synced: Vec<&bool> = results.iter().filter(|is_synced| **is_synced).collect();

        info!("{}/{} consensus nodes synced", synced.len(), results.len());
        let num_out_of_sync = results.len() - synced.len();

        if num_out_of_sync > 1 {
            Err(anyhow!("all consensus nodes out of sync"))
        } else {
            if num_out_of_sync == 1 {
                warn!("one consensus node is out of sync");
                let message = TelegramSafeAlert::new("one consensus node is out of sync");
                self.telegram_alerts.send_warning(message).await;
            }
            Ok(Utc::now())
        }
    }
}

#[async_trait]
impl PhoenixMonitor for ConsensusNodeMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>> {
        ConsensusNodeMonitor::get_current_timestamp(self).await
    }
}
