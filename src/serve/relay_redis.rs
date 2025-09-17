use crate::env::ToNetwork;

use super::APP_CONFIG;

pub async fn get_known_validator_count(client: &redis::Client) -> anyhow::Result<i64> {
    let mut conn = client.get_multiplexed_async_connection().await?;

    let key = format!(
        "boost-relay/{}:stats",
        &APP_CONFIG.env.to_network().to_string()
    );
    let count: Option<i64> = redis::cmd("HGET")
        .arg(key)
        .arg("validators-total")
        .query_async(&mut conn)
        .await?;

    Ok(count.unwrap_or(0))
}
