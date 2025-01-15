use reqwest::Url;
use serde::{
    de::{DeserializeOwned, Error},
    Deserialize, Deserializer,
};
use std::{collections::HashSet, fmt};
use tracing::error;

#[derive(PartialEq)]
pub enum Env {
    Stag,
    Prod,
}

impl fmt::Display for Env {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Env::Stag => write!(f, "staging"),
            Env::Prod => write!(f, "production"),
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
    Holesky,
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match &self {
            Network::Mainnet => "mainnet",
            Network::Holesky => "holesky",
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
            Env::Stag => Network::Holesky,
            Env::Prod => Network::Mainnet,
        }
    }
}

pub fn deserialize_network<'de, D>(deserializer: D) -> Result<Network, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    match s.as_ref() {
        "mainnet" => Ok(Network::Mainnet),
        "holesky" => Ok(Network::Holesky),
        _ => Err(Error::custom("network present but not mainnet or goerli")),
    }
}

pub trait ToBeaconExplorerUrl {
    fn to_beacon_explorer_url(&self) -> String;
}

impl ToBeaconExplorerUrl for Env {
    fn to_beacon_explorer_url(&self) -> String {
        match *self {
            Env::Stag => "https://holesky.beaconcha.in",
            Env::Prod => "https://beaconcha.in",
        }
        .to_string()
    }
}

impl ToBeaconExplorerUrl for Network {
    fn to_beacon_explorer_url(&self) -> String {
        match *self {
            Network::Mainnet => "https://beaconcha.in",
            Network::Holesky => "https://holesky.beaconcha.in",
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
        .map(|s| Url::parse(s).map_err(Error::custom))
        .collect()
}

/// Deserialize HashSet from comma separated string
pub fn deserialize_hash_set<'de, D>(deserializer: D) -> Result<HashSet<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Ok(s.split(',').map(|s| s.trim().to_string()).collect())
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
