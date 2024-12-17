use anyhow::Context;
use sqlx::{PgPool, Row};

#[derive(Default, sqlx::FromRow)]
pub struct ProposerLabelMeta {
    pub grafitti: Option<String>,
    pub label: Option<String>,
    pub lido_operator: Option<String>,
}

pub async fn proposer_label_meta(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<ProposerLabelMeta> {
    sqlx::query_as::<_, ProposerLabelMeta>(
        "
        SELECT 
            COALESCE(pl.label, va.label) as label,
            lido_operator,
            last_graffiti
        FROM validators va
        LEFT JOIN proposer_labels_with_imputed_data_view pl on va.pubkey = pl.pubkey
        WHERE va.pubkey = $1
        ",
    )
    .bind(proposer_pubkey)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.unwrap_or_default())
    .context("failed to get proposer label meta")
}

pub async fn proposer_registration_ip(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<Option<String>> {
    sqlx::query(
        "
        SELECT last_registration_ip_address
        FROM validators
        WHERE pubkey = $1
        ",
    )
    .bind(proposer_pubkey)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.and_then(|row| row.get(0)))
    .context("failed to get proposer ip")
}

pub async fn proposer_payload_request_ip(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<Option<String>> {
    sqlx::query(
        "
        SELECT ip
        FROM payload_requests
        WHERE pubkey = $1
        ",
    )
    .bind(proposer_pubkey)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.and_then(|row| row.get(0)))
    .context("failed to get proposer ip")
}

pub async fn get_proposer_ip(
    pg_pool: &PgPool,
    proposer_pubkey: &str,
) -> anyhow::Result<Option<String>> {
    let registration_ip = proposer_registration_ip(pg_pool, proposer_pubkey).await?;
    let payload_request_ip = proposer_payload_request_ip(pg_pool, proposer_pubkey).await?;

    Ok(registration_ip.or(payload_request_ip))
}

#[derive(Default, sqlx::FromRow)]
pub struct ProposerLocation {
    pub country: Option<String>,
    pub city: Option<String>,
}

pub async fn proposer_location(
    pg_pool: &PgPool,
    ip_address: &str,
) -> anyhow::Result<ProposerLocation> {
    sqlx::query_as::<_, ProposerLocation>(
        "
        SELECT country, city
        FROM ip_meta
        WHERE ip_address = $1
        ",
    )
    .bind(ip_address)
    .fetch_optional(pg_pool)
    .await
    .map(|row| row.unwrap_or_default())
    .context("failed to get proposer location")
}
