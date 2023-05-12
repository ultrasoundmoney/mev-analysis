use chrono::Duration;
use serde::Serialize;
use sqlx::postgres::types::PgInterval;

pub enum Timeframe {
    SevenDays,
    ThirtyDays,
}

impl Timeframe {
    pub fn to_interval(&self) -> PgInterval {
        match self {
            Timeframe::SevenDays => PgInterval::try_from(Duration::days(7)).unwrap(),
            Timeframe::ThirtyDays => PgInterval::try_from(Duration::days(30)).unwrap(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Timeframed<T> {
    pub seven_days: T,
    pub thirty_days: T,
}
