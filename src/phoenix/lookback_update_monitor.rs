use anyhow::Result;
use sqlx::PgPool;
use tracing::debug;

use super::util::get_current_slot;
use crate::phoenix::{Alarm, AlarmType};

use super::env::APP_CONFIG;

async fn get_latest_lookback_updates_slot(mev_pool: &PgPool) -> anyhow::Result<u32> {
    sqlx::query_scalar!(
        r#"
        SELECT MAX(slot)
        FROM lookback_updates
        "#,
    )
    .fetch_one(mev_pool)
    .await
    .map(|max| {
        max.expect("No maximum slot found from lookback_updates")
            .try_into()
            .expect("Maximum slot is negative")
    })
    .map_err(Into::into)
}

pub async fn run_lookback_updates_monitor(mev_pool: &PgPool, alarm: &mut Alarm) -> Result<()> {
    let latest_slot = get_latest_lookback_updates_slot(mev_pool).await?;
    let current_slot = get_current_slot()?;
    let slot_lag = current_slot - latest_slot;
    debug!(
        "lookback_updates is {:} slots behind current slot",
        slot_lag
    );
    if slot_lag > APP_CONFIG.max_lookback_updates_slot_lag {
        let message = format!(
            "lookback_updates is {:} slots behind the current slot",
            slot_lag
        );
        alarm.fire(&message, &AlarmType::Telegram).await;
    }
    Ok(())
}
