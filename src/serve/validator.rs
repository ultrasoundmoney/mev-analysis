use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, TimeZone, Utc};
use futures::future::join_all;
use serde::Serialize;
use sqlx::{postgres::PgRow, Pool, Postgres, Row};
use tracing::warn;

use super::relay_redis::get_known_validator_count;
use super::{env::APP_CONFIG, internal_error, AppState};

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ValidatorStatsBody {
    validator_count: i64,
    known_validator_count: i64,
    recipient_count: i64,
}

pub async fn get_validator_stats(
    db_pool: &Pool<Postgres>,
    redis_client: &redis::Client,
) -> Result<ValidatorStatsBody, String> {
    let query = format!(
        "
        select
          (
             select count(distinct pubkey)
             from {}_validator_registration
          ) as count,
          (
             select count(distinct sq.fee_recipient) as recipient_count
             from (
               select max(inserted_at), pubkey, fee_recipient
               from {}_validator_registration
               group by pubkey, fee_recipient
             ) sq
          ) as recipient_count
        ",
        &APP_CONFIG.network.to_string(),
        &APP_CONFIG.network.to_string(),
    );

    let (db_counts, known_validator_count) = tokio::join!(
        sqlx::query(&query)
            .map(|row: PgRow| (row.get("count"), row.get("recipient_count")))
            .fetch_one(db_pool),
        get_known_validator_count(redis_client)
    );

    let (count, recipient_count) = db_counts.map_err(|e| e.to_string())?;
    let known_count = known_validator_count.map_err(|e| e.to_string())?;

    Ok(ValidatorStatsBody {
        validator_count: count,
        known_validator_count: known_count,
        recipient_count,
    })
}

pub async fn validator_stats(
    State(state): State<AppState>,
) -> Result<Json<ValidatorStatsBody>, (StatusCode, String)> {
    let cached_result = state.cache.validator_stats.read().unwrap();

    match &*cached_result {
        Some(stats) => Ok(Json(stats.clone())),
        None => {
            warn!("validator_stats cache miss");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "something went wrong".to_string(),
            ))
        }
    }
}

#[derive(Serialize)]
pub struct RegistrationStatusBody {
    status: bool,
}

pub async fn check_validator_registration(
    State(state): State<AppState>,
    Path(pubkey): Path<String>,
) -> Result<Json<RegistrationStatusBody>, (StatusCode, String)> {
    let query = format!(
        "
         select exists (
            select pubkey from {}_validator_registration where pubkey = $1
         )
        ",
        &APP_CONFIG.network.to_string()
    );

    sqlx::query(&query)
        .bind(pubkey)
        .fetch_one(&state.db_pool)
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
        &APP_CONFIG.network.to_string()
    );

    let registrations: Vec<ValidatorRegistration> = sqlx::query(&query)
        .fetch_all(&state.db_pool)
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
            let idx = beacon_api.get_validator_index(&v.pubkey).await.unwrap();

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
