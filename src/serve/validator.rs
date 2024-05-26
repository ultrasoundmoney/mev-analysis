use anyhow::Result;
use axum::{
    extract::{Path, State},
    Json,
};
use chrono::{DateTime, TimeZone, Utc};
use futures::future::join_all;
use serde::Serialize;
use sqlx::{PgPool, Row};

use crate::env::ToNetwork;

use super::{env::APP_CONFIG, internal_error, AppState};
use super::{relay_redis, ApiResponse};

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorStatsBody {
    validator_count: i64,
    known_validator_count: i64,
}

async fn get_registered_validator_count(relay_pool: &PgPool) -> Result<i64> {
    sqlx::query(&format!(
        "
         select count(*) as validator_count
         from (select distinct pubkey from {}_validator_registration) as sq
        ",
        &APP_CONFIG.env.to_network().to_string()
    ))
    .fetch_one(relay_pool)
    .await
    .map(|row| row.get::<i64, _>("validator_count"))
    .map_err(Into::into)
}

pub async fn validator_stats(State(state): State<AppState>) -> ApiResponse<ValidatorStatsBody> {
    let (known_validator_count, validator_count) = tokio::try_join!(
        relay_redis::get_known_validator_count(&state.redis_client),
        get_registered_validator_count(&state.relay_db_pool)
    )
    .map_err(internal_error)?;

    Ok(Json(ValidatorStatsBody {
        validator_count,
        known_validator_count,
    }))
}

#[derive(Serialize)]
pub struct RegistrationStatusBody {
    status: bool,
}

pub async fn check_validator_registration(
    State(state): State<AppState>,
    Path(pubkey): Path<String>,
) -> ApiResponse<RegistrationStatusBody> {
    let query = format!(
        "
         select exists (
            select pubkey from {}_validator_registration where pubkey = $1
         )
        ",
        &APP_CONFIG.env.to_network().to_string()
    );

    sqlx::query(&query)
        .bind(pubkey)
        .fetch_one(&state.relay_db_pool)
        .await
        .map(|row| {
            Json(RegistrationStatusBody {
                status: row.get("exists"),
            })
        })
        .map_err(internal_error)
}

pub struct ValidatorRegistration {
    inserted_at: DateTime<Utc>,
    pubkey: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorWithIndex {
    inserted_at: DateTime<Utc>,
    pubkey: String,
    pubkey_fragment: String,
    index: String,
}

#[derive(Serialize)]
pub struct ValidatorRegistrationsBody {
    validators: Vec<ValidatorWithIndex>,
}

pub async fn validator_registrations(
    State(state): State<AppState>,
) -> Json<ValidatorRegistrationsBody> {
    let query = format!(
        "
           select inserted_at, pubkey
            from (
                select min(inserted_at) as inserted_at, pubkey
                from {}_validator_registration
                group by pubkey
            ) sq
            order by inserted_at desc
            limit 30

        ",
        &APP_CONFIG.env.to_network().to_string()
    );

    let registrations: Vec<ValidatorRegistration> = sqlx::query(&query)
        .fetch_all(&state.relay_db_pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| ValidatorRegistration {
                    inserted_at: Utc.from_utc_datetime(&row.get("inserted_at")),
                    pubkey: row.get::<String, _>("pubkey"),
                })
                .collect()
        })
        .unwrap();

    let mut cached_validators: Vec<ValidatorWithIndex> = registrations
        .iter()
        .filter_map(|r| {
            state
                .validator_index_cache
                .lock()
                .unwrap()
                .get(&r.pubkey)
                .map(|idx| ValidatorWithIndex {
                    inserted_at: r.inserted_at,
                    pubkey: r.pubkey.to_string(),
                    pubkey_fragment: r.pubkey[0..8].to_string(),
                    index: idx.to_string(),
                })
        })
        .collect();

    let non_cached: Vec<ValidatorRegistration> = registrations
        .into_iter()
        .filter(|reg| {
            !cached_validators
                .iter()
                .any(|cached| reg.pubkey == cached.pubkey)
        })
        .collect();

    let mut futs = Vec::new();

    let beacon_api = &state.beacon_api;

    for v in non_cached {
        futs.push(async move {
            let idx = beacon_api.validator_index(&v.pubkey).await.unwrap();

            ValidatorWithIndex {
                inserted_at: v.inserted_at,
                pubkey: v.pubkey.to_string(),
                pubkey_fragment: v.pubkey[0..8].to_string(),
                index: idx,
            }
        })
    }

    let new_validators = join_all(futs).await;

    // update cache
    state.validator_index_cache.lock().unwrap().extend(
        new_validators
            .iter()
            .map(|nv| (nv.pubkey.clone(), nv.index.clone())),
    );

    cached_validators.extend(new_validators);
    // make sure validators are still ordered desc by registration time
    cached_validators.sort_by(|a, b| b.inserted_at.cmp(&a.inserted_at));

    Json(ValidatorRegistrationsBody {
        validators: cached_validators,
    })
}
