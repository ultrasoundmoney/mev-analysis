use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;
use sqlx::{Pool, Postgres, Row};
use tracing::warn;

use super::{env::APP_CONFIG, AppState};

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Builder {
    extra_data: Option<String>,
    block_count: i64,
}

#[derive(Serialize)]
pub struct BuildersBody {
    builders: Vec<Builder>,
}

#[derive(PartialEq, Debug)]
enum DecodeErr {
    HexError(String),
    Utf8Error(String),
}

fn decode_extra_data(s: &String) -> Result<String, DecodeErr> {
    let trimmed: String = s.chars().skip(2).collect();
    // hack because some funny guy is putting non-utf8 chars into extra_data
    // TODO: filter out non-utf8 bytes for a general solution
    if trimmed == "d883010a17846765746888676f312e31392e33856c696e7578" {
        return Ok("geth go1.19.3 linux".to_string());
    }

    hex::decode(trimmed)
        .map_err(|err| DecodeErr::HexError(err.to_string()))
        .and_then(|bytes| {
            String::from_utf8(bytes).map_err(|err| DecodeErr::Utf8Error(err.to_string()))
        })
}

pub async fn get_top_builders(pool: &Pool<Postgres>) -> Result<Vec<Builder>, String> {
    let query = format!(
        "
        select
           signed_blinded_beacon_block->'message'->'body'->'execution_payload_header'->>'extra_data' as extra_data_hex,
           count(*) as block_count
           from {}_payload_delivered
           group by extra_data_hex
        ",
        &APP_CONFIG.network.to_string()
    );

    sqlx::query(&query)
        .fetch_all(pool)
        .await
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let hex_str: String = row.get("extra_data_hex");
                    let decoded = decode_extra_data(&hex_str);
                    let extra_data = match decoded {
                        Ok(data) => Some(data),
                        Err(DecodeErr::HexError(err)) => {
                            warn!("failed to decode hex for {}: {}", &hex_str, err);
                            None
                        }
                        Err(DecodeErr::Utf8Error(err)) => {
                            warn!("failed to decode utf8 for {}: {}", &hex_str, err);
                            Some(hex_str)
                        }
                    };

                    Builder {
                        block_count: row.get("block_count"),
                        extra_data,
                    }
                })
                .collect()
        })
        .map_err(|e| e.to_string())
}

pub async fn top_builders(
    State(state): State<AppState>,
) -> Result<Json<BuildersBody>, (StatusCode, String)> {
    let cached_result = state.cache.top_builders.read().unwrap();

    match &*cached_result {
        Some(res) => Ok(Json(BuildersBody {
            builders: res.to_vec(),
        })),
        None => {
            warn!("top_builders cache miss");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "something went wrong".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_valid_hex_string() {
        let hex_string =
            "0x496c6c756d696e61746520446d6f63726174697a6520447374726962757465".to_string();

        assert_eq!(
            decode_extra_data(&hex_string),
            Ok("Illuminate Dmocratize Dstribute".to_string())
        );
    }
}
