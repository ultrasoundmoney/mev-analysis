use std::{fmt::Display, ops, str::FromStr, sync::LazyLock};

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::{env::Network, phoenix::env::APP_CONFIG};

static GENESIS_TIMESTAMP: LazyLock<DateTime<Utc>> = LazyLock::new(|| match APP_CONFIG.network {
    Network::Mainnet => "2020-12-01T12:00:23Z".parse().unwrap(),
    Network::Holesky => "2023-09-28T12:00:00Z".parse().unwrap(),
    Network::Hoodi => "2025-03-17T12:10:00Z".parse().unwrap(),
});

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

    pub fn now() -> Self {
        Self::from_date_time_rounded_down(Utc::now())
    }

    pub fn from_date_time_rounded_down(date_time: DateTime<Utc>) -> Self {
        if date_time < *GENESIS_TIMESTAMP {
            return Self(0);
        }
        let duration_since_genesis = date_time.signed_duration_since(*GENESIS_TIMESTAMP);
        let slots_since_genesis =
            duration_since_genesis.num_seconds() / i64::from(Self::SECONDS_PER_SLOT);
        Self(slots_since_genesis as i32)
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_from_date_time_rounded_down() {
        let genesis_date_time = "2020-12-01T12:00:23Z".parse::<DateTime<Utc>>().unwrap();

        // Exactly genesis.
        let slot = Slot::from_date_time_rounded_down(genesis_date_time);
        assert_eq!(slot, Slot(0));

        // 11 seconds after genesis, still slot 0.
        let slot = Slot::from_date_time_rounded_down(genesis_date_time + Duration::seconds(11));
        assert_eq!(slot, Slot(0));

        // 12 seconds after genesis, slot 1.
        let slot = Slot::from_date_time_rounded_down(genesis_date_time + Duration::seconds(12));
        assert_eq!(slot, Slot(1));

        // 1 second before genesis, slot 0.
        let slot = Slot::from_date_time_rounded_down(genesis_date_time - Duration::seconds(1));
        assert_eq!(slot, Slot(0));

        // 12 seconds before genesis, slot 0.
        let slot = Slot::from_date_time_rounded_down(genesis_date_time - Duration::seconds(12));
        assert_eq!(slot, Slot(0));

        // 13 seconds before genesis, slot 0.
        let slot = Slot::from_date_time_rounded_down(genesis_date_time - Duration::seconds(13));
        assert_eq!(slot, Slot(0));
    }
}
