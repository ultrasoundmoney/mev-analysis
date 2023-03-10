use crate::env::ToNetwork;

use super::APP_CONFIG;

pub async fn get_known_validator_count(client: &redis::Client) -> redis::RedisResult<i64> {
    let mut conn = client.get_async_connection().await.unwrap();
    let key = format!(
        "boost-relay/{}:known-validators",
        &APP_CONFIG.env.to_network().to_string()
    );
    redis::cmd("HLEN").arg(key).query_async(&mut conn).await
}
