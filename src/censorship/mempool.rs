mod zeromev;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::{fmt, str::FromStr};

use super::chain::Tx;
pub use zeromev::ZeroMev;

// service for timestamping transactions based on their appearance in the mempool(s)
#[async_trait]
pub trait MempoolStore {
    async fn fetch_tx_timestamps(
        &self,
        mut txs: Vec<Tx>,
        start_block: i64,
        end_block: i64,
    ) -> Result<Vec<TaggedTx>>;
}

#[derive(Clone)]
pub struct MempoolTimestamp {
    pub id: SourceId,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct TaggedTx {
    pub timestamps: Vec<MempoolTimestamp>,
    pub tx: Tx,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SourceId {
    ZeroMevInf,
    ZeroMevQn,
    ZeroMevUs,
    ZeroMevEu,
    ZeroMevAs,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseExtractorIdError(String);

impl FromStr for SourceId {
    type Err = ParseExtractorIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Inf" => Ok(SourceId::ZeroMevInf),
            "Qn" => Ok(SourceId::ZeroMevQn),
            "US" => Ok(SourceId::ZeroMevUs),
            "EU" => Ok(SourceId::ZeroMevEu),
            "AS" => Ok(SourceId::ZeroMevAs),
            unknown => Err(ParseExtractorIdError(format!(
                "unknown extractor id: {}",
                unknown
            ))),
        }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let str = match &self {
            SourceId::ZeroMevInf => "zeromev-inf",
            SourceId::ZeroMevQn => "zeromev-qn",
            SourceId::ZeroMevUs => "zeromev-us",
            SourceId::ZeroMevEu => "zeromev-eu",
            SourceId::ZeroMevAs => "zeromev-as",
        };
        write!(f, "{}", str)
    }
}
