use anyhow::Result;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use sqlx::PgPool;
use tracing::debug;

use crate::{
    env::{Network, ToNetwork},
    phoenix::{Alarm, AlarmType},
};

use super::env::APP_CONFIG;

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = {
        match &APP_CONFIG.env.to_network() {
            Network::Mainnet => "2020-12-01T12:00:23Z".parse().unwrap(),
            Network::Holesky => "2023-09-28T12:00:00Z".parse().unwrap(),
        }
    };
}

const SECONDS_PER_SLOT: u8 = 12;

async fn get_latest_auction_analysis_slot(mev_pool: &PgPool) -> anyhow::Result<u32> {
    sqlx::query_scalar!(
        r#"
        SELECT MAX(slot)
        FROM auction_analysis
        "#,
    )
    .fetch_one(mev_pool)
    .await
    .map(|max| {
        max.expect("No maximum slot found from auction_analysis")
            .try_into()
            .expect("Maximum slot is negative")
    })
    .map_err(Into::into)
}

fn get_current_slot() -> Result<u32> {
    let now = Utc::now();
    let seconds_since_genesis: u32 = (now - *GENESIS_TIMESTAMP).num_seconds().try_into()?;
    Ok(seconds_since_genesis / SECONDS_PER_SLOT as u32)
}

pub async fn run_auction_analysis_monitor(mev_pool: &PgPool, alarm: &mut Alarm) -> Result<()> {
    let latest_slot = get_latest_auction_analysis_slot(mev_pool).await?;
    let current_slot = get_current_slot()?;
    let slot_lag = current_slot - latest_slot;
    debug!(
        "Auction analysis is {:} slots behind current slot",
        slot_lag
    );
    if slot_lag > APP_CONFIG.max_auction_analysis_slot_lag {
        let message = format!(
            "Auction analysis is {:} slots behind the current slot",
            slot_lag
        );
        alarm.fire(&message, &AlarmType::Telegram).await;
    }
    Ok(())
}
