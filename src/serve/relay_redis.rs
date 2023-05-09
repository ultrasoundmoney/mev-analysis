use std::collections::HashSet;

use chrono::{Duration, Utc};

use crate::env::ToNetwork;

use super::APP_CONFIG;

pub async fn get_known_validator_count(client: &redis::Client) -> anyhow::Result<i64> {
    let mut conn = client.get_async_connection().await?;
    let key = format!(
        "boost-relay/{}:stats",
        &APP_CONFIG.env.to_network().to_string()
    );
    redis::cmd("HGET")
        .arg(key)
        .arg("validators-total")
        .query_async(&mut conn)
        .await
        .map_err(Into::into)
}

pub async fn get_active_validator_count(client: &redis::Client) -> anyhow::Result<i64> {
    let active_validators_hours = 3; // default in mev-boost-relay
    let now = Utc::now();
    let keys = (0..active_validators_hours)
        .map(|i| {
            let hour = now.checked_sub_signed(Duration::hours(i)).unwrap();
            format!(
                "boost-relay/{}:active-validators:{}",
                &APP_CONFIG.env.to_network().to_string(),
                hour.format("%Y-%m-%dT%H").to_string()
            )
        })
        .collect::<Vec<String>>();

    let mut validators = HashSet::<String>::new();
    let mut conn = client.get_async_connection().await?;

    for key in keys {
        let entries = redis::cmd("HGETALL")
            .arg(key)
            .query_async::<_, Vec<(String, String)>>(&mut conn)
            .await?
            .into_iter()
            .map(|(k, _)| k);

        validators.extend(entries);
    }

    Ok(validators.len() as i64)
}
