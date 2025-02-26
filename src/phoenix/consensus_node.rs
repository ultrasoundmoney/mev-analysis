use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info};

use super::{env::APP_CONFIG, PhoenixMonitor};
use crate::beacon_api::BeaconApi;

pub struct ConsensusNodeMonitor {
    beacon_api: BeaconApi,
}

impl ConsensusNodeMonitor {
    pub fn new() -> Self {
        Self {
            beacon_api: BeaconApi::new(&APP_CONFIG.consensus_nodes),
        }
    }

    async fn check_nodes_once(&self) -> Vec<bool> {
        let mut results = Vec::new();

        let statuses = self.beacon_api.sync_status_all().await;
        for status in statuses {
            match status {
                Ok(s) => results.push(!s.is_syncing),
                Err(err) => {
                    error!("error getting consensus node status: {}", err);
                    results.push(false)
                }
            }
        }

        results
    }

    async fn num_unsynced_nodes(&self) -> usize {
        // First attempt
        let mut results = self.check_nodes_once().await;
        let mut offline_nodes = results.iter().filter(|is_synced| !**is_synced).count();

        // If any nodes are offline, retry after 3 seconds
        if offline_nodes > 0 {
            info!(
                "found {} offline consensus nodes, retrying in 3s",
                offline_nodes
            );
            sleep(Duration::from_secs(3)).await;

            results = self.check_nodes_once().await;
            offline_nodes = results.iter().filter(|is_synced| !**is_synced).count();

            // If still offline, try one last time
            if offline_nodes > 0 {
                info!(
                    "still found {} offline consensus nodes, final retry in 3s",
                    offline_nodes
                );
                sleep(Duration::from_secs(3)).await;
                results = self.check_nodes_once().await;
            }
        }

        let synced: Vec<&bool> = results.iter().filter(|is_synced| **is_synced).collect();
        debug!("{}/{} consensus nodes synced", synced.len(), results.len());
        results.len() - synced.len()
    }
}

#[async_trait]
impl PhoenixMonitor for ConsensusNodeMonitor {
    async fn refresh(&self) -> (DateTime<Utc>, usize) {
        let num_unsynced_nodes = self.num_unsynced_nodes().await;
        (Utc::now(), num_unsynced_nodes)
    }
}
