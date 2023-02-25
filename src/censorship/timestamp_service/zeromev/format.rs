use std::{io::Read, ops::Add};

use chrono::{DateTime, Duration, Utc};
use flate2::read::GzDecoder;
use lazy_static::lazy_static;

use super::BlockNumber;

// this module exposes a helper for dealing with zeromev custom binary format

// timestamp, block_number tuple. these are mapped to transactions by their index in the vector (equal to tx_index)
pub type TxTuple = (DateTime<Utc>, BlockNumber);

lazy_static! {
    static ref CENTURY_START: DateTime<Utc> =
        DateTime::parse_from_rfc3339("0001-01-01T00:00:00.00Z")
            .unwrap()
            .with_timezone(&Utc);
}

fn from_dotnet_ticks(ticks: i64) -> DateTime<Utc> {
    CENTURY_START.add(Duration::microseconds(ticks / 10))
}

fn to_i64_vec(bytes: Vec<u8>) -> Vec<i64> {
    assert!(
        bytes.len() % 8 == 0,
        "expected byte array to be evenly divisible by 8"
    );

    bytes
        .chunks(8)
        .map(|slice| i64::from_le_bytes(slice.try_into().unwrap()))
        .collect()
}

pub fn parse_tx_data(bytes: Vec<u8>) -> Vec<TxTuple> {
    let mut decomp_bytes = vec![];

    GzDecoder::new(&bytes[..])
        .read_to_end(&mut decomp_bytes)
        .expect("failed to gzip decode zeromev tx_data bytes");

    let i64_vec = to_i64_vec(decomp_bytes);

    assert!(
        i64_vec.len() % 2 == 0,
        "expected i64 array to be evenly divisible by 2"
    );

    i64_vec
        .chunks(2)
        .map(|slice| (from_dotnet_ticks(slice[0]), slice[1]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::to_i64_vec;

    #[test]
    fn test_bytes_to_i64_vec() {
        let bytes: Vec<u8> = vec![1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let result = to_i64_vec(bytes);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 1);
    }
}
