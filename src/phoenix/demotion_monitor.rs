use std::sync::LazyLock;

use anyhow::Result;
use chrono::{DateTime, Utc};
use indoc::formatdoc;
use itertools::Itertools;
use rand::{distributions::Alphanumeric, Rng};
use sqlx::{PgPool, Row};
use tracing::{debug, info};

use crate::{
    env::ToBeaconExplorerUrl,
    phoenix::{
        alerts::telegram::{Channel, TelegramMessage},
        promotion_monitor::is_promotable_error,
        telegram,
    },
};

use super::{
    checkpoint::{self, CheckpointId},
    env::{Geo, APP_CONFIG},
};

static DIRECT_MESSAGE_BUILDER_IDS: LazyLock<Vec<String>> = LazyLock::new(|| {
    telegram::BUILDER_ID_CHANNEL_ID_MAP
        .keys()
        .cloned()
        .collect_vec()
});

#[derive(Debug, Clone)]
pub struct BuilderDemotion {
    pub geo: Geo,
    pub block_hash: String,
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
    let query = r#"
        SELECT
            bd.geo,
            bd.block_hash,
            bd.builder_pubkey,
            bb.builder_id,
            bd.slot,
            bd.sim_error
        FROM builder_demotions bd
        INNER JOIN builder bb
          ON bd.builder_pubkey = bb.builder_pubkey
        WHERE bd.inserted_at > $1
          AND bd.inserted_at <= $2
        ORDER BY bd.inserted_at ASC
     "#;

    sqlx::query(query)
        .bind(start)
        .bind(end)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| BuilderDemotion {
                    geo: row.get("geo"),
                    block_hash: row.get("block_hash"),
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
const IGNORED_ERRORS: &[&str] = &[
    "Post \"http://prio-load-balancer:80\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
    "json error: request timeout hit before processing",
    "simulation failed: unknown ancestor",
    "simulation queue timed out"
];

fn is_ignored_error(error: &str) -> bool {
    IGNORED_ERRORS
        .iter()
        // Use starts_with to account for dynamic info in error message
        .any(|silent_error| error.starts_with(silent_error))
}

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
        .filter(|d| !is_ignored_error(&d.sim_error))
        .collect_vec()
}

// this fn sometimes produces a message telegram doesn't like.
// we escape and truncate the error code block.
fn format_demotion_message(demotion: &BuilderDemotion) -> String {
    const MAX_ERROR_LEN: usize = 512;
    const ERROR_TRUNCATION_MARKER: &str = "..TRUNCATED..";

    let explorer_url = APP_CONFIG.network.to_beacon_explorer_url();
    let network = APP_CONFIG.network.to_string();
    let builder_id = demotion.builder_id.as_deref().unwrap_or("unknown");
    let escaped_builder_id = telegram::escape_str(builder_id);
    let escaped_network = telegram::escape_str(&network);
    let builder_pubkey = &demotion.builder_pubkey;
    let mut escaped_error = telegram::escape_code_block(&demotion.sim_error);

    // Truncate the escaped error if it exceeds the specific limit
    if escaped_error.len() > MAX_ERROR_LEN {
        let max_len_adjusted = MAX_ERROR_LEN.saturating_sub(ERROR_TRUNCATION_MARKER.len());
        escaped_error.truncate(max_len_adjusted);
        escaped_error.push_str(ERROR_TRUNCATION_MARKER);
        // Log the truncation
        tracing::warn!(
            slot = demotion.slot,
            builder_id,
            "truncated demotion sim_error message due to length limit ({})",
            MAX_ERROR_LEN
        );
    }

    let slot = &demotion.slot;
    let geo = &demotion.geo;
    let block_hash = &demotion.block_hash;
    formatdoc!(
        "
        [beaconcha\\.in/slot/{slot}]({explorer_url}/slot/{slot})
        slot: `{slot}`
        network: `{escaped_network}`
        geo: `{geo}`
        builder\\_id: `{escaped_builder_id}`
        builder\\_pubkey: `{builder_pubkey}`
        block\\_hash: `{block_hash}`
        ```
        {escaped_error}
        ```
        "
    )
}

async fn gen_promotion_token(pool: &PgPool, builder_id: &str) -> Result<String> {
    let expires_at = chrono::Utc::now() + chrono::Duration::days(7);
    let mut token: String;

    loop {
        token = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        let token_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM promotion_tokens WHERE token = $1)",
        )
        .bind(&token)
        .fetch_one(pool)
        .await?;

        if !token_exists {
            break;
        }
    }

    sqlx::query(
        "
        INSERT INTO promotion_tokens (
            builder_id,
            token,
            expires_at
        )
        VALUES ($1, $2, $3)
        ",
    )
    .bind(builder_id)
    .bind(&token)
    .bind(expires_at)
    .execute(pool)
    .await?;

    Ok(token)
}

async fn generate_and_send_alerts(
    demotions: Vec<BuilderDemotion>,
    global_db_pool: &PgPool,
) -> Result<()> {
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
                // .insert returns true only if the key is not already in the set.
                // effectively, this means we only keep the first demotion for each builder.
                seen.insert(key)
            })
            .collect::<Vec<_>>()
    };

    let unique_warning_demotions = unique_demotions(warning_demotions);
    let unique_alert_demotions = unique_demotions(alert_demotions);

    let telegram_bot = telegram::TelegramBot::new();
    if !unique_alert_demotions.is_empty() {
        for demotion in unique_alert_demotions {
            let alert_message = format_demotion_message(&demotion);
            let alert_message = TelegramMessage::from_escaped_string(alert_message);

            let builder_id = demotion.builder_id.as_deref().unwrap_or("unknown");
            match gen_promotion_token(global_db_pool, builder_id).await {
                Ok(token) => {
                    let button_url = format!(
                        "{}/ultrasound/v1/data/admin/promote?token={}",
                        APP_CONFIG.relay_analytics_url(),
                        token
                    );
                    info!(%alert_message, "sending telegram message to demotions channel");
                    telegram_bot
                        .send_demotion_with_button(&alert_message, &button_url)
                        .await;

                    if DIRECT_MESSAGE_BUILDER_IDS.contains(&builder_id.to_string()) {
                        info!(%alert_message, builder_id, "sending telegram message to builder");
                        telegram_bot
                            .send_message_to_builder(&alert_message, builder_id, Some(&button_url))
                            .await;
                    }
                }
                Err(err) => {
                    tracing::error!(%err, "failed to generate and store promotion token");
                }
            }
        }
    }

    let warning_messages: Vec<String> = unique_warning_demotions
        .iter()
        .map(format_demotion_message)
        .collect();

    if !warning_messages.is_empty() {
        let warning_message = {
            let message = format!(
                "*builder demoted \\(with promotable error\\)*\n\n{}",
                warning_messages.join("\n\n")
            );
            TelegramMessage::from_escaped_string(message)
        };
        info!(?warning_message, "sending telegram warning");

        telegram_bot
            .send_message(&warning_message, Channel::Warnings)
            .await
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
    generate_and_send_alerts(demotions, relay_pool).await?;
    update_checkpoint(mev_pool, now).await?;
    Ok(())
}
