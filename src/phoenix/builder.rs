use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;

use super::{env::APP_CONFIG, PhoenixMonitor};

async fn get_status() -> Result<StatusCode> {
    let url = format!("{}/eth/v1/builder/status", APP_CONFIG.relay_api_root);
    reqwest::get(url)
        .await
        .map(|res| res.status())
        .map_err(|e| e.into())
}

pub struct BuilderStatusMonitor {}

impl BuilderStatusMonitor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_current_timestamp() -> Result<DateTime<Utc>> {
        get_status().await.and_then(|status| match status {
            StatusCode::OK => Ok(Utc::now()),
            code => Err(anyhow!("builder status not ok, got {}", code)),
        })
    }
}

#[async_trait]
impl PhoenixMonitor for BuilderStatusMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>> {
        BuilderStatusMonitor::get_current_timestamp().await
    }
}
