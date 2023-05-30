use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::fmt;

pub enum CheckpointId {
    Demotion,
    Inclusion,
    Promotion,
}

impl fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CheckpointId::Demotion => write!(f, "demotion_monitor"),
            CheckpointId::Inclusion => write!(f, "inclusion_monitor"),
            CheckpointId::Promotion => write!(f, "promotion_monitor"),
        }
    }
}

pub async fn get_checkpoint(mev_pool: &PgPool, id: CheckpointId) -> Result<Option<DateTime<Utc>>> {
    sqlx::query_scalar!(
        r#"
        SELECT timestamp
        FROM monitor_checkpoints
        WHERE monitor_id = $1
        LIMIT 1
        "#,
        id.to_string()
    )
    .fetch_optional(mev_pool)
    .await
    .map_err(Into::into)
}

pub async fn put_checkpoint(
    mev_pool: &PgPool,
    id: CheckpointId,
    checkpoint: &DateTime<Utc>,
) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO monitor_checkpoints (monitor_id, timestamp)
        VALUES ($1, $2)
        ON CONFLICT (monitor_id) DO UPDATE SET timestamp = $2
        "#,
        id.to_string(),
        checkpoint
    )
    .execute(mev_pool)
    .await
    .map(|_| ())
    .map_err(Into::into)
}
