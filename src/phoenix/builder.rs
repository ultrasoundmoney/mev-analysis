use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;

use crate::env::Env;

use super::{env::APP_CONFIG, PhoenixMonitor};

async fn get_status() -> Result<StatusCode> {
    let api_root = match APP_CONFIG.env {
        Env::Stag => "https://relay-stag.ultrasound.money",
        Env::Prod => "https://relay.ultrasound.money",
    };
    let url = format!("{}/eth/v1/builder/status", &api_root);
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
