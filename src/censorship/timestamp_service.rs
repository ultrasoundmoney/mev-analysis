mod zeromev;

use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::chain_store::Tx;

pub use zeromev::ZeroMev;

// service for timestamping transactions based on their appearance in the mempool(s)
#[async_trait]
pub trait TimestampService {
    async fn fetch_tx_timestamps(&self, mut txs: Vec<Tx>) -> Result<Vec<TaggedTx>>;
}

#[derive(PartialEq, Clone, Debug)]
pub enum ExtractorId {
    ZMevInf,
    ZMevQn,
    ZMevUS,
    ZMevEU,
    ZMevAS,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseExtractorIdError(String);

impl FromStr for ExtractorId {
    type Err = ParseExtractorIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Inf" => Ok(ExtractorId::ZMevInf),
            "Qn" => Ok(ExtractorId::ZMevQn),
            "US" => Ok(ExtractorId::ZMevUS),
            "EU" => Ok(ExtractorId::ZMevEU),
            "AS" => Ok(ExtractorId::ZMevAS),
            unknown => Err(ParseExtractorIdError(format!(
                "unknown extractor id: {}",
                unknown
            ))),
        }
    }
}

#[derive(Debug)]
pub struct ExtractorTimestamp {
    id: ExtractorId,
    timestamp: DateTime<Utc>,
}

#[derive(Debug)]
pub struct TaggedTx {
    timestamps: Vec<ExtractorTimestamp>,
    tx: Tx,
}
