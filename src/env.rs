use chrono::Duration;
use reqwest::Url;
use serde::{
    de::{DeserializeOwned, Error},
    Deserialize, Deserializer,
};
use std::fmt;
use tracing::error;

#[derive(PartialEq)]
pub enum Env {
    Stag,
    Prod,
}

impl fmt::Display for Env {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Env::Stag => write!(f, "{}", "staging"),
            Env::Prod => write!(f, "{}", "production"),
        }
    }
}

impl<'de> Deserialize<'de> for Env {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        match s.as_ref() {
            "stag" => Ok(Env::Stag),
            "staging" => Ok(Env::Stag),
            "prod" => Ok(Env::Prod),
            "production" => Ok(Env::Prod),
            _ => Err(Error::custom(
                "env present but not stag, staging, prod or production",
            )),
        }
    }
}

#[derive(PartialEq, Deserialize)]
pub enum Network {
    Mainnet,
    Goerli,
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match &self {
            Network::Mainnet => "mainnet",
            Network::Goerli => "goerli",
        };
        write!(f, "{}", str)
    }
}

pub trait ToNetwork {
    fn to_network(&self) -> Network;
}

impl ToNetwork for Env {
    fn to_network(&self) -> Network {
        match *self {
            Env::Stag => Network::Goerli,
            Env::Prod => Network::Mainnet,
        }
    }
}

pub trait ToBeaconExplorerUrl {
    fn to_beacon_explorer_url(&self) -> String;
}

impl ToBeaconExplorerUrl for Env {
    fn to_beacon_explorer_url(&self) -> String {
        match *self {
            Env::Stag => "https://goerli.beaconcha.in",
            Env::Prod => "https://beaconcha.in",
        }
        .to_string()
    }
}

pub fn deserialize_urls<'de, D>(deserializer: D) -> Result<Vec<Url>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.split(',')
        .into_iter()
        .map(|s| Url::parse(s).map_err(Error::custom))
        .collect()
}

pub fn deserialize_duration_minutes<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let num: i64 = Deserialize::deserialize(deserializer)?;
    Ok(Duration::minutes(num))
}

pub fn get_app_config<T: DeserializeOwned>() -> T {
    match envy::from_env::<T>() {
        Ok(config) => config,
        Err(err) => {
            error!("failed to parse config: {}", err);
            std::process::exit(1);
        }
    }
}
