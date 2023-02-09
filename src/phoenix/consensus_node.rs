use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{error, info};

use super::{env::APP_CONFIG, PhoenixMonitor};
use crate::beacon_api;

pub struct ConsensusNodeMonitor {}

impl ConsensusNodeMonitor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_current_timestamp() -> Result<DateTime<Utc>> {
        let mut results = Vec::new();

        for url in &APP_CONFIG.consensus_nodes {
            let status = beacon_api::get_sync_status(&url).await;

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

        if synced.len() == APP_CONFIG.consensus_nodes.len() {
            Ok(Utc::now())
        } else {
            Err(anyhow!("one or more consensus nodes out of sync"))
        }
    }
}

#[async_trait]
impl PhoenixMonitor for ConsensusNodeMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>> {
        ConsensusNodeMonitor::get_current_timestamp().await
    }
}
