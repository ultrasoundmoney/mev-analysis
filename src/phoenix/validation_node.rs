use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, error};

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

    pub async fn num_unsynced_nodes(&self) -> usize {
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
