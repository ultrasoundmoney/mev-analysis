use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, error};

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

    async fn num_unsynced_nodes(&self) -> usize {
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
