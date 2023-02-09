use lazy_static::lazy_static;
use reqwest::Url;

use crate::env::{parse_env, Env, Network, ToNetwork};

pub struct AppConfig {
    pub env: Env,
    pub network: Network,
    pub port: u16,
    pub relay_api_root: &'static str,
    pub consensus_nodes: Vec<Url>,
    pub validation_nodes: Vec<Url>,
    pub opsgenie_api_key: String,
    pub db_connection_str: String,
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
    let consensus_nodes = parse_nodes("CONSENSUS_NODES");
    let validation_nodes = parse_nodes("VALIDATION_NODES");
    let opsgenie_api_key = std::env::var("OPSGENIE_API_KEY");
    let db_connection_str = std::env::var("DATABASE_URL");

    if let (
        Ok(env),
        Ok(consensus_nodes),
        Ok(validation_nodes),
        Ok(opsgenie_api_key),
        Ok(db_connection_str),
    ) = (
        env,
        consensus_nodes,
        validation_nodes,
        opsgenie_api_key,
        db_connection_str,
    ) {
        AppConfig {
            relay_api_root: match env {
                Env::Stag => "https://relay-stag.ultrasound.money",
                Env::Prod => "https://relay.ultrasound.money",
            },
            network: env.to_network(),
            env,
            port,
            consensus_nodes,
            validation_nodes,
            opsgenie_api_key,
            db_connection_str,
        }
    } else {
        panic!("failed to construct AppConfig")
    }
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
