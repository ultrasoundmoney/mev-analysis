mod format;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};
use std::str::FromStr;
use tracing::{debug, error};

use self::format::{parse_tx_data, TxTuple};
use super::{MempoolStore, MempoolTimestamp, SourceId, TaggedTx, Tx};
use crate::censorship::env::APP_CONFIG;

pub struct ZeroMev {
    db_pool: Pool<Postgres>,
}

impl ZeroMev {
    pub async fn new() -> Self {
        let db_pool = PgPoolOptions::new()
            .max_connections(3)
            .acquire_timeout(Duration::seconds(3).to_std().unwrap())
            .connect(&APP_CONFIG.zeromev_database_url)
            .await
            .expect("can't connect to zeromev database");

        Self { db_pool }
    }
}

pub type BlockNumber = i64;

#[derive(Clone)]
struct BlockExtractorRow {
    block_number: BlockNumber,
    block_time: DateTime<Utc>,
    extractor: SourceId,
    tx_data: Vec<TxTuple>,
}

fn tag_transactions(
    mut txs: Vec<Tx>,
    mut rows: Vec<BlockExtractorRow>,
    start_block: i64,
    end_block: i64,
) -> Vec<TaggedTx> {
    // we rely on transaction index to associate timestamps from zeromev to transactions
    // on our side, so we need to make sure everything is sorted
    txs.sort_by_key(|tx| (tx.block_number, tx.transaction_index));
    rows.sort_by_key(|row| row.block_number);

    let block_numbers = start_block..=end_block;

    let data_by_block = block_numbers
        .map(|block_number| {
            let block_txs = txs
                .iter()
                .skip_while(|tx| tx.block_number < block_number)
                .take_while(|tx| tx.block_number == block_number)
                .map(|tx| tx.to_owned())
                .collect_vec();

            let block_extractors = rows
                .iter()
                .skip_while(|row| row.block_number < block_number)
                .take_while(|row| row.block_number == block_number)
                .map(|row| row.to_owned())
                .collect_vec();

            let valid_extractors = filter_extractor_rows(block_txs.len(), block_extractors);

            let missing_extractors = block_txs.len() > 0 && valid_extractors.len() == 0;

            if missing_extractors {
                error!(
                    "no extractors for block {} ({} txs)",
                    block_number,
                    block_txs.len()
                );
                panic!("no extractors for block");
            }

            (block_number, block_txs, valid_extractors)
        })
        .collect_vec();

    data_by_block
        .iter()
        .map(|(_, txs, extractors)| {
            txs.iter().map(|tx| {
                let expected_tx_count = txs.len();
                let timestamps: Vec<MempoolTimestamp> = extractors
                    .iter()
                    // filter out extractors that don't have a tx count that matches what's on chain
                    .filter(|row| row.tx_data.len() == expected_tx_count)
                    .map(|ex| MempoolTimestamp {
                        id: ex.extractor.clone(),
                        timestamp: ex
                            .tx_data
                            .get(usize::try_from(tx.transaction_index).unwrap())
                            .expect("expected extractor data to contain transaction index")
                            .0,
                    })
                    .collect();

                let found_valid_extractors = !timestamps.is_empty();

                if !found_valid_extractors {
                    let received_tx_counts =
                        extractors.iter().map(|ex| ex.tx_data.len()).collect_vec();

                    error!(
                        "no valid extractors found for block {} tx_index {}. Expected {}, got {:?}",
                        tx.block_number,
                        tx.transaction_index,
                        expected_tx_count,
                        received_tx_counts
                    );
                    panic!("expected at least one extractor with matching tx count");
                }

                TaggedTx {
                    timestamps,
                    tx: tx.clone(),
                }
            })
        })
        .flatten()
        .collect()
}

// First filter out extractors that don't have a tx count that matches what's on chain,
// then pick the most recent extractor if there are multiple
fn filter_extractor_rows(
    expected_tx_count: usize,
    rows: Vec<BlockExtractorRow>,
) -> Vec<BlockExtractorRow> {
    rows.into_iter()
        .filter(|row| row.tx_data.len() == expected_tx_count)
        .group_by(|row| row.extractor)
        .into_iter()
        .map(|(_, group)| group.max_by_key(|row| row.block_time).unwrap())
        .collect()
}

// when there's an empty block, this is what tx_data will be set to
const GZIP_HEADER_HEX: &str = "\\x1f8b080000000000000303000000000000000000";

#[async_trait]
impl MempoolStore for ZeroMev {
    async fn fetch_tx_timestamps(
        &self,
        txs: Vec<Tx>,
        start_block: i64,
        end_block: i64,
    ) -> Result<Vec<TaggedTx>> {
        let query = format!(
            "
                SELECT
                    block_number,
                    block_time,
                    extractor.code AS extractor,
                    tx_data
                FROM
                    extractor_block
                INNER JOIN
                    extractor USING (extractor_index)
                WHERE
                    block_number >= {}
                    AND block_number <= {}
                    AND tx_data != '{}'
                ORDER BY
                    block_number, extractor
            ",
            start_block, end_block, &GZIP_HEADER_HEX
        );

        let rows: Vec<BlockExtractorRow> =
            sqlx::query(&query)
                .fetch_all(&self.db_pool)
                .await
                .map(|rows| {
                    rows.iter()
                        .map(|row| BlockExtractorRow {
                            block_number: row.get("block_number"),
                            block_time: row.get("block_time"),
                            extractor: SourceId::from_str(&row.get::<String, _>("extractor"))
                                .expect("failed to parse extractor id"),
                            tx_data: parse_tx_data(row.get("tx_data")),
                        })
                        .collect()
                })?;

        Ok(tag_transactions(txs, rows, start_block, end_block))
    }
}

#[allow(dead_code)]
fn log_non_consecutive(id: &str, ns: Vec<&i64>) {
    let first = ns.first().unwrap();
    ns.iter().skip(1).fold(*first, |prev, n| {
        if *n - prev > 1 {
            debug!("{}: non-consecutive: {}", id, &n);
            return n;
        }
        n
    });
}
