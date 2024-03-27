use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use indoc::formatdoc;
use itertools::Itertools;
use sqlx::{PgPool, Row};
use tracing::{debug, info};

use crate::{
    env::{ToBeaconExplorerUrl, ToNetwork},
    phoenix::{
        alerts::{telegram::TelegramSafeAlert, SendAlert},
        promotion_monitor::is_promotable_error,
        telegram,
    },
};

use super::{
    checkpoint::{self, CheckpointId},
    env::APP_CONFIG,
};

#[derive(Debug, Clone)]
pub struct BuilderDemotion {
    pub inserted_at: DateTime<Utc>,
    pub builder_pubkey: String,
    pub builder_id: Option<String>,
    pub slot: i64,
    pub sim_error: String,
}

pub async fn get_builder_demotions(
    relay_pool: &PgPool,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> Result<Vec<BuilderDemotion>> {
    let query = format!(
        "
        SELECT
            bd.inserted_at,
            bd.builder_pubkey,
            bb.builder_id,
            bd.slot,
            bd.sim_error
        FROM {network}_builder_demotions bd
        INNER JOIN {network}_blockbuilder bb
          ON bd.builder_pubkey = bb.builder_pubkey
        WHERE bd.inserted_at > $1
          AND bd.inserted_at <= $2
        ORDER BY bd.inserted_at ASC
     ",
        network = &APP_CONFIG.env.to_network().to_string()
    );

    sqlx::query(&query)
        .bind(start)
        .bind(end)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| BuilderDemotion {
                    inserted_at: Utc.from_utc_datetime(&row.get("inserted_at")),
                    builder_pubkey: row.get("builder_pubkey"),
                    builder_id: row.try_get("builder_id").ok(),
                    slot: row.get("slot"),
                    sim_error: row.get::<String, _>("sim_error").trim().to_string(),
                })
                .collect()
        })
        .map_err(Into::into)
}

/// Demotion errors that shouldn't be broadcast on telegram
pub const IGNORED_ERRORS: &[&str] = &[
    "HTTP status server error (500 Internal Server Error) for url (http://prio-load-balancer/)",
    "Post \"http://prio-load-balancer:80\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
    "json error: request timeout hit before processing",
    "simulation failed: unknown ancestor",
];

async fn fetch_demotions(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    now: DateTime<Utc>,
) -> Result<Vec<BuilderDemotion>> {
    let checkpoint = match checkpoint::get_checkpoint(mev_pool, CheckpointId::Demotion).await? {
        Some(c) => c,
        None => {
            info!("no checkpoint found, initializing");
            checkpoint::put_checkpoint(mev_pool, CheckpointId::Demotion, &now).await?;
            now
        }
    };
    debug!("checking demotions between {} and {}", &checkpoint, &now);
    let demotions = get_builder_demotions(relay_pool, &checkpoint, &now).await?;
    Ok(demotions)
}

fn filter_demotions(demotions: Vec<BuilderDemotion>) -> Vec<BuilderDemotion> {
    demotions
        .into_iter()
        .filter(|d| !IGNORED_ERRORS.contains(&d.sim_error.as_str()))
        .collect_vec()
}

fn format_demotion_message(demotion: &BuilderDemotion) -> String {
    let explorer_url = APP_CONFIG.env.to_beacon_explorer_url();
    let builder_id = demotion.builder_id.as_deref().unwrap_or("unknown");
    let escaped_builder_id = telegram::escape_str(builder_id);
    let builder_pubkey = &demotion.builder_pubkey;
    let error = telegram::escape_code_block(&demotion.sim_error);
    let slot = demotion.slot;
    formatdoc!(
        "
        [beaconcha\\.in/slot/{slot}]({explorer_url}/slot/{slot})
        slot: `{slot}`
        builder\\_id: `{escaped_builder_id}`
        builder\\_pubkey: `{builder_pubkey}`
        ```
        {error}
        ```
        "
    )
}

async fn generate_and_send_alerts(demotions: Vec<BuilderDemotion>) -> Result<()> {
    let filtered_demotions = filter_demotions(demotions);
    let (warning_demotions, alert_demotions): (Vec<BuilderDemotion>, Vec<BuilderDemotion>) =
        filtered_demotions
            .into_iter()
            .partition(|d| is_promotable_error(&d.sim_error));

    let unique_demotions = |demotions: Vec<BuilderDemotion>| {
        let mut seen = std::collections::HashSet::new();
        demotions
            .into_iter()
            .filter(|d| {
                let key = d
                    .builder_id
                    .clone()
                    .unwrap_or_else(|| d.builder_pubkey.clone());
                seen.insert(key)
            })
            .collect::<Vec<_>>()
    };

    let unique_warning_demotions = unique_demotions(warning_demotions);
    let unique_alert_demotions = unique_demotions(alert_demotions);

    let alert_messages: Vec<String> = unique_alert_demotions
        .iter()
        .map(format_demotion_message)
        .collect();
    let warning_messages: Vec<String> = unique_warning_demotions
        .iter()
        .map(format_demotion_message)
        .collect();

    let telegram_alerts = telegram::TelegramAlerts::new();
    if !alert_messages.is_empty() {
        let alert_message = {
            let message = format!("*builder demoted*\n\n{}", alert_messages.join("\n\n"));
            TelegramSafeAlert::from_escaped_string(message)
        };
        info!(?alert_message, "sending telegram alert");
        telegram_alerts.send_alert(alert_message).await
    }

    if !warning_messages.is_empty() {
        let warning_message = {
            let message = format!(
                "*builder demoted \\(with promotable error\\)*\n\n{}",
                warning_messages.join("\n\n")
            );
            TelegramSafeAlert::from_escaped_string(message)
        };
        info!(?warning_message, "sending telegram warning");

        telegram_alerts.send_warning(warning_message).await
    }

    Ok(())
}

async fn update_checkpoint(mev_pool: &PgPool, now: DateTime<Utc>) -> Result<()> {
    checkpoint::put_checkpoint(mev_pool, CheckpointId::Demotion, &now).await?;
    Ok(())
}

pub async fn run_demotion_monitor(relay_pool: &PgPool, mev_pool: &PgPool) -> Result<()> {
    let now = Utc::now();
    let demotions = fetch_demotions(relay_pool, mev_pool, now).await?;
    generate_and_send_alerts(demotions).await?;
    update_checkpoint(mev_pool, now).await?;
    Ok(())
}
