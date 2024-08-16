use super::env::APP_CONFIG;
use crate::env::{Network, ToNetwork};
use anyhow::Result;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = {
        match &APP_CONFIG.env.to_network() {
            Network::Mainnet => "2020-12-01T12:00:23Z".parse().unwrap(),
            Network::Holesky => "2023-09-28T12:00:00Z".parse().unwrap(),
        }
    };
}

const SECONDS_PER_SLOT: u8 = 12;

pub fn get_current_slot() -> Result<u32> {
    let now = Utc::now();
    let seconds_since_genesis: u32 = (now - *GENESIS_TIMESTAMP).num_seconds().try_into()?;
    Ok(seconds_since_genesis / SECONDS_PER_SLOT as u32)
}
