use std::{fmt::Display, ops, str::FromStr};

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::Deserialize;

use crate::{env::Network, phoenix::env::APP_CONFIG};

lazy_static! {
    static ref GENESIS_TIMESTAMP: DateTime<Utc> = {
        match APP_CONFIG.network {
            Network::Mainnet => "2020-12-01T12:00:23Z".parse().unwrap(),
            Network::Holesky => "2023-09-28T12:00:00Z".parse().unwrap(),
        }
    };
}

#[derive(Debug, Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
pub struct Slot(pub i32);

impl Display for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:07}", self.0)
    }
}

impl Slot {
    const SECONDS_PER_SLOT: u8 = 12;

    pub fn date_time(&self) -> DateTime<Utc> {
        self.into()
    }
}

impl From<&Slot> for DateTime<Utc> {
    fn from(slot: &Slot) -> Self {
        let seconds = slot.0 as i64 * Slot::SECONDS_PER_SLOT as i64;
        *GENESIS_TIMESTAMP + chrono::Duration::seconds(seconds)
    }
}

impl FromStr for Slot {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let slot = s.parse::<i32>()?;
        Ok(Self(slot))
    }
}

impl ops::Add<u32> for Slot {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0 + rhs as i32)
    }
}
