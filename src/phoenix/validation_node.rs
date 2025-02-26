use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info};

use super::{env::APP_CONFIG, PhoenixMonitor};

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
}

impl ValidationNodeMonitor {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    async fn check_nodes_once(&self) -> Vec<bool> {
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

        results
    }

    pub async fn num_unsynced_nodes(&self) -> usize {
        // First attempt
        let mut results = self.check_nodes_once().await;
        let mut offline_nodes = results.iter().filter(|is_synced| !**is_synced).count();

        // If any nodes are offline, retry after 3 seconds
        if offline_nodes > 0 {
            info!(
                "found {} offline validation nodes, retrying in 3s",
                offline_nodes
            );
            sleep(Duration::from_secs(3)).await;

            results = self.check_nodes_once().await;
            offline_nodes = results.iter().filter(|is_synced| !**is_synced).count();

            // If still offline, try one last time
            if offline_nodes > 0 {
                info!(
                    "still found {} offline validation nodes, final retry in 3s",
                    offline_nodes
                );
                sleep(Duration::from_secs(3)).await;
                results = self.check_nodes_once().await;
            }
        }

        let synced: Vec<&bool> = results.iter().filter(|is_synced| **is_synced).collect();
        debug!("{}/{} validation nodes synced", synced.len(), results.len());

        results.len() - synced.len()
    }
}

#[async_trait]
impl PhoenixMonitor for ValidationNodeMonitor {
    async fn refresh(&self) -> (DateTime<Utc>, usize) {
        let num_unsynced_nodes = self.num_unsynced_nodes().await;
        (Utc::now(), num_unsynced_nodes)
    }
}
