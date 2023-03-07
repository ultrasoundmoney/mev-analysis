use lazy_static::lazy_static;
use reqwest::Url;

use crate::env::{parse_env, Env, Network, ToNetwork};

pub struct AppConfig {
    pub env: Env,
    pub port: u16,
    pub network: Network,
    pub mev_db_url: String,
    pub relay_db_url: String,
    pub redis_url: String,
    pub consensus_nodes: Vec<Url>,
}

fn parse_nodes(env_var: &str) -> anyhow::Result<Vec<Url>> {
    std::env::var(env_var)?
        .split(",")
        .into_iter()
        .map(|url_str| Ok(url_str.parse()?))
        .collect()
}

fn get_app_config() -> AppConfig {
    let env = std::env::var("ENV").map(parse_env);
    let port = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(3002);
    let mev_db_url = std::env::var("DATABASE_URL");
    let relay_db_url = std::env::var("RELAY_DATABASE_URL");
    let redis_uri = std::env::var("REDIS_URI");
    let consensus_nodes = parse_nodes("CONSENSUS_NODES");

    if let (Ok(env), Ok(mev_db_url), Ok(relay_db_url), Ok(redis_uri), Ok(consensus_nodes)) =
        (env, mev_db_url, relay_db_url, redis_uri, consensus_nodes)
    {
        AppConfig {
            network: env.to_network(),
            env,
            port,
            mev_db_url,
            relay_db_url,
            redis_url: format!("redis://{}", redis_uri),
            consensus_nodes,
        }
    } else {
        panic!("failed to construct AppConfig")
    }
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_stringifies_correctly() {
        let a = Network::Mainnet.to_string();
        let b = Network::Goerli.to_string();
        assert_eq!(a, "mainnet");
        assert_eq!(b, "goerli");
    }
}
