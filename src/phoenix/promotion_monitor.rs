use std::collections::HashSet;

use anyhow::Result;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

use super::{
    alerts::telegram::{TelegramBot, TelegramMessage},
    checkpoint::{self, CheckpointId},
    demotion_monitor::{get_builder_demotions, BuilderDemotion},
    env::APP_CONFIG,
};

async fn get_missed_slots(mev_pool: &PgPool, start: &DateTime<Utc>) -> Result<Vec<i64>> {
    sqlx::query_scalar!(
        r#"
        SELECT slot_number
        FROM missed_slots
        WHERE inserted_at > $1
        "#,
        start,
    )
    .fetch_all(mev_pool)
    .await
    .map_err(Into::into)
}

async fn promote_builder_ids(
    relay_pool: &PgPool,
    builder_ids: &Vec<String>,
) -> Result<Vec<(String, String)>> {
    let query = "
        UPDATE builder
        SET is_optimistic = true
        WHERE
            builder_id = ANY($1)
        AND collateral > 0
        AND is_optimistic = false
        RETURNING builder_id, builder_pubkey
        ";

    sqlx::query(query)
        .bind(builder_ids)
        .fetch_all(relay_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| (row.get("builder_id"), row.get("builder_pubkey")))
                .collect_vec()
        })
        .map_err(Into::into)
}

#[allow(dead_code, clippy::ptr_arg)]
fn format_builder_list(builders: &Vec<(String, String)>) -> String {
    builders
        .iter()
        .sorted_by_key(|(id, _)| id)
        .group_by(|(id, _)| id.clone())
        .into_iter()
        .fold(String::new(), |acc, (id, group)| {
            let pubkeys = group
                .map(|(_, pubkey)| format!("`{}`", &pubkey))
                .collect_vec()
                .join("\n");

            if acc.is_empty() {
                format!("*{}*\n{}", id, pubkeys)
            } else {
                format!("{}\n\n*{}*\n{}", acc, id, pubkeys)
            }
        })
}

/// Demotion errors eligible for re-promotion if no slot was missed
const PROMOTABLE_ERRORS: &[&str] = &[
    "HTTP status server error (500 Internal Server Error) for url (http://prio-load-balancer/)",
    "Post \"http://prio-load-balancer:80\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
    "json error: request timeout hit before processing",
    "simulation failed: unknown ancestor",
    "simulation failed: incorrect gas limit set",
    "simulation queue timed out"
];

pub fn is_promotable_error(error: &str) -> bool {
    PROMOTABLE_ERRORS
        .iter()
        // Use starts_with to account for additional info in gas limit error
        .any(|promotable_error| error.starts_with(promotable_error))
}

fn is_promotable_trusted_builder_error(
    trusted_builders: &HashSet<String>,
    trusted_promotable_errors: &HashSet<String>,
    builder_id: &String,
    error: &str,
) -> bool {
    trusted_builders.contains(builder_id)
        && trusted_promotable_errors
            .iter()
            // Use starts_with to account for additional info in gas limit error
            .any(|e| error.starts_with(e))
}

fn filter_known_builder_demotions(
    demotions: Vec<BuilderDemotion>,
) -> Vec<(String, BuilderDemotion)> {
    demotions
        .into_iter()
        .filter_map(|d| {
            if let Some(builder_id) = d.builder_id.clone() {
                Some((builder_id, d))
            } else {
                warn!("demotion without builder_id: {:?}", d);
                None
            }
        })
        .collect()
}

fn check_eligibility(
    trusted_builders: &HashSet<String>,
    trusted_promotable_errors: &HashSet<String>,
    builder_id: &String,
    demotions: &[BuilderDemotion],
    missed_slots: &[i64],
) -> bool {
    let no_missed_slots = demotions.iter().all(|d| !missed_slots.contains(&d.slot));
    let all_eligible_errors = demotions.iter().all(|d| {
        is_promotable_error(&d.sim_error)
            || is_promotable_trusted_builder_error(
                trusted_builders,
                trusted_promotable_errors,
                builder_id,
                &d.sim_error,
            )
    });
    no_missed_slots && all_eligible_errors
}

async fn send_telegram_alerts(
    trusted_builders: &HashSet<String>,
    trusted_promotable_errors: &HashSet<String>,
    telegram_alerts: &TelegramBot,
    builder_id: &String,
    demotions: &[BuilderDemotion],
) {
    if demotions.iter().any(|d| {
        is_promotable_trusted_builder_error(
            trusted_builders,
            trusted_promotable_errors,
            builder_id,
            &d.sim_error,
        )
    }) {
        let message = format!(
            "automatically repromoting builder `{}` for error which may result in missed slot",
            builder_id
        );
        telegram_alerts
            .send_message_to_builder(&TelegramMessage::new(&message), builder_id, None)
            .await;
    }
}

pub async fn run_promotion_monitor(
    relay_pool: &PgPool,
    mev_pool: &PgPool,
    canonical_horizon: &DateTime<Utc>,
) -> Result<()> {
    let checkpoint = match checkpoint::get_checkpoint(mev_pool, CheckpointId::Promotion).await? {
        Some(c) => c,
        None => {
            info!("no checkpoint found, initializing");
            let now = Utc::now();
            checkpoint::put_checkpoint(mev_pool, CheckpointId::Promotion, &now).await?;
            now
        }
    };

    debug!(
        "checking promotions between {} and {}",
        &checkpoint, &canonical_horizon
    );

    let demotions = get_builder_demotions(relay_pool, &checkpoint, canonical_horizon).await?;
    let missed_slots = get_missed_slots(mev_pool, &checkpoint).await?;

    debug!(
        "scanning for promotable demotions, demotions: {:?}, missed_slots {:?}",
        &demotions, &missed_slots
    );

    let demotions_with_ids = filter_known_builder_demotions(demotions);
    let grouped_demotions = demotions_with_ids.into_iter().into_group_map();

    let mut eligible_builders = Vec::new();
    let telegram_alerts = TelegramBot::new();
    let trusted_builders = &APP_CONFIG.trusted_builder_ids;
    let trusted_promotable_errors = &APP_CONFIG.trusted_builder_promotable_errors;

    for (builder_id, demotions) in grouped_demotions {
        if check_eligibility(
            trusted_builders,
            trusted_promotable_errors,
            &builder_id,
            &demotions,
            &missed_slots,
        ) {
            eligible_builders.push(builder_id.clone());
            send_telegram_alerts(
                trusted_builders,
                trusted_promotable_errors,
                &telegram_alerts,
                &builder_id,
                &demotions,
            )
            .await;
        }
    }

    if !eligible_builders.is_empty() {
        info!(
            "found builder ids eligible for promotion: {:?}",
            &eligible_builders
        );
        promote_builder_ids(relay_pool, &eligible_builders).await?;
    }

    checkpoint::put_checkpoint(mev_pool, CheckpointId::Promotion, canonical_horizon).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::phoenix::env::Geo;

    use super::*;

    fn get_eligible_builders(
        trusted_builders: &HashSet<String>,
        trusted_promotable_errors: &HashSet<String>,
        demotions: Vec<BuilderDemotion>,
        missed_slots: Vec<i64>,
    ) -> Vec<String> {
        let demotions_with_ids = filter_known_builder_demotions(demotions);
        let grouped_demotions = demotions_with_ids.into_iter().into_group_map();

        let mut eligible_builders = Vec::new();
        for (builder_id, demotions) in grouped_demotions {
            if check_eligibility(
                trusted_builders,
                trusted_promotable_errors,
                &builder_id,
                &demotions,
                &missed_slots,
            ) {
                eligible_builders.push(builder_id.clone());
            }
        }
        eligible_builders.sort();
        eligible_builders
    }

    #[test]
    fn test_get_eligible_builders_all_eligible() {
        let demotions = vec![
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "json error: request timeout hit before processing".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash2".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![];

        let result = get_eligible_builders(
            &HashSet::default(),
            &HashSet::default(),
            demotions,
            missed_slots,
        );
        assert_eq!(result, vec!["builder1".to_string(), "builder2".to_string()]);
    }

    #[test]
    fn test_get_eligible_builders_none_eligible() {
        let demotions = vec![
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "invalid error".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash2".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![2];

        let result = get_eligible_builders(
            &HashSet::default(),
            &HashSet::default(),
            demotions,
            missed_slots,
        );

        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn test_get_eligible_builders_some_eligible() {
        let demotions = vec![
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "json error: request timeout hit before processing".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash2".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "invalid error".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash3".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 3,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![2];

        let result = get_eligible_builders(
            &HashSet::default(),
            &HashSet::default(),
            demotions,
            missed_slots,
        );

        assert_eq!(result, vec!["builder1".to_string()]);
    }

    #[test]
    fn test_same_slot_both_valid_and_invalid() {
        let demotions = vec![
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "invalid error".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "json error: request timeout hit before processing".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: unknown ancestor".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![];

        let result = get_eligible_builders(
            &HashSet::default(),
            &HashSet::default(),
            demotions,
            missed_slots,
        );

        assert_eq!(result, vec!["builder1".to_string()]);
    }

    #[test]
    fn test_trusted_builder_promotion_no_missed() {
        let demotions = vec![
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: invalid merkle root".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "simulation failed: invalid merkle root".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: invalid merkle root".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![];

        let result = get_eligible_builders(
            &HashSet::from_iter(vec!["builder1".to_string()]),
            &HashSet::from_iter(vec!["simulation failed: invalid merkle root".to_string()]),
            demotions.clone(),
            missed_slots.clone(),
        );

        assert_eq!(result, vec!["builder1".to_string()]);

        // Trusted builder but no configured errors should not match
        let result = get_eligible_builders(
            &HashSet::from_iter(vec!["builder1".to_string()]),
            &HashSet::from_iter(vec![]),
            demotions,
            missed_slots,
        );

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_trusted_builder_promotion_missed() {
        let demotions = vec![
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: invalid merkle root".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey1".to_string(),
                sim_error: "simulation failed: invalid merkle root".to_string(),
                slot: 1,
                builder_id: Some("builder1".to_string()),
            },
            BuilderDemotion {
                geo: Geo::RBX,
                block_hash: "block_hash1".to_string(),
                builder_pubkey: "pubkey2".to_string(),
                sim_error: "simulation failed: invalid merkle root".to_string(),
                slot: 2,
                builder_id: Some("builder2".to_string()),
            },
        ];
        let missed_slots = vec![1];

        let result = get_eligible_builders(
            &HashSet::from_iter(vec!["builder1".to_string()]),
            &HashSet::from_iter(vec!["simulation failed: invalid merkle root".to_string()]),
            demotions,
            missed_slots,
        );

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_format_builder_list() {
        let builder_ids = vec![
            ("builder1".to_string(), "pkey1".to_string()),
            ("builder2".to_string(), "pkey2".to_string()),
            ("builder1".to_string(), "pkey3".to_string()),
        ];
        let result = format_builder_list(&builder_ids);
        assert_eq!(
            result,
            "*builder1*\n`pkey1`\n`pkey3`\n\n*builder2*\n`pkey2`"
        );
    }
}
