mod alert;
mod checkpoint;
mod consensus_node;
mod demotion_monitor;
mod env;
mod inclusion_monitor;
mod markdown;
mod promotion_monitor;
mod validation_node;

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use axum::{http::StatusCode, routing::get, Router};
use chrono::{DateTime, Duration, Utc};
use env::APP_CONFIG;
use lazy_static::lazy_static;
use sqlx::{postgres::PgPoolOptions, Connection, PgConnection, PgPool};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

use crate::log;
use crate::phoenix::{
    consensus_node::ConsensusNodeMonitor, validation_node::ValidationNodeMonitor,
};

use self::{
    demotion_monitor::run_demotion_monitor,
    inclusion_monitor::{run_inclusion_monitor, LokiClient},
    promotion_monitor::run_promotion_monitor,
};

lazy_static! {
    static ref PHOENIX_MAX_LIFESPAN: Duration = Duration::minutes(3);
    static ref MIN_ALARM_WAIT: Duration = Duration::minutes(4);
}

struct Alarm {
    last_fired: Option<DateTime<Utc>>,
}

impl Alarm {
    fn new() -> Self {
        Self { last_fired: None }
    }

    fn is_throttled(&self) -> bool {
        self.last_fired.map_or(false, |last_fired| {
            Utc::now() - last_fired < *MIN_ALARM_WAIT
        })
    }

    async fn fire(&mut self, message: &str) {
        if self.is_throttled() {
            warn!("alarm is throttled, ignoring request to fire alarm");
            return;
        }

        error!(message, "firing alarm");

        let result = alert::send_alert(message).await;
        if let Err(err) = result {
            error!(?err, "failed to send alert!");
        }

        self.last_fired = Some(Utc::now());
    }

    async fn fire_with_name(&mut self, name: &str) {
        let message = format!(
            "{} hasn't updated for more than {} seconds",
            name,
            PHOENIX_MAX_LIFESPAN.num_seconds(),
        );

        self.fire(&message).await
    }
}

struct Phoenix {
    name: &'static str,
    last_seen: DateTime<Utc>,
    monitor: Box<dyn PhoenixMonitor + Send + Sync>,
}

impl Phoenix {
    fn is_age_over_limit(&self) -> bool {
        let age = Utc::now() - self.last_seen;

        debug!(
            name = self.name,
            age = age.num_seconds(),
            limit = PHOENIX_MAX_LIFESPAN.num_seconds(),
            "checking age"
        );
        age >= *PHOENIX_MAX_LIFESPAN
    }

    fn set_last_seen(&mut self, last_seen: DateTime<Utc>) {
        debug!(name = self.name, ?last_seen, "setting last seen");
        self.last_seen = last_seen;
    }
}

#[async_trait]
trait PhoenixMonitor {
    async fn refresh(&self) -> Result<DateTime<Utc>>;
}

async fn run_alarm_loop(last_checked: Arc<Mutex<DateTime<Utc>>>) -> Result<()> {
    info!(
        "releasing phoenix, dies after {} seconds",
        PHOENIX_MAX_LIFESPAN.num_seconds()
    );

    let mut alarm = Alarm::new();

    let mut phoenixes = vec![
        Phoenix {
            last_seen: Utc::now(),
            monitor: Box::new(ConsensusNodeMonitor::new()),
            name: "consensus node",
        },
        Phoenix {
            last_seen: Utc::now(),
            monitor: Box::new(ValidationNodeMonitor::new()),
            name: "validation node",
        },
    ];

    loop {
        for phoenix in phoenixes.iter_mut() {
            if phoenix.is_age_over_limit() {
                alarm.fire_with_name(phoenix.name).await;
            }

            let current = phoenix.monitor.refresh().await;
            match current {
                Ok(current) => phoenix.set_last_seen(current),
                Err(err) => {
                    error!(
                        name = phoenix.name,
                        ?err,
                        "failed to refresh phoenix monitor"
                    );
                }
            }
        }

        // Update the last checked time.
        {
            let mut last_checked = last_checked.lock().unwrap();
            *last_checked = Utc::now();
        }

        sleep(Duration::seconds(10).to_std().unwrap()).await;
    }
}

async fn mount_health_route() -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], APP_CONFIG.port));
    let app = Router::new().route("/", get(|| async { StatusCode::OK }));

    info!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(Into::into)
}

/// Get a database connection, retrying until we can connect. Send an alert if we can't.
async fn get_db_connection(
    db_url: &str,
    retry_interval: &Duration,
    max_retry_duration: &Duration,
) -> Result<PgPool> {
    let start_time = Instant::now();
    loop {
        match PgPoolOptions::new()
            .max_connections(3)
            .acquire_timeout(std::time::Duration::from_secs(3))
            .connect(db_url)
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(error)
                if Instant::now().duration_since(start_time)
                    < max_retry_duration.to_std().unwrap() =>
            {
                error!(?error, "failed to connect to database, retrying");
                tokio::time::sleep(retry_interval.to_std().unwrap()).await;
            }
            Err(error) => {
                error!(?error, "failed to connect to database, sending alert");
                alert::send_alert("phoenix service failed to connect to database").await?;
                return Err(error.into());
            }
        }
    }
}

async fn run_ops_monitors() -> Result<()> {
    let max_retry_duration = Duration::minutes(2);
    let retry_interval = Duration::seconds(10);

    let relay_pool = get_db_connection(
        &APP_CONFIG.relay_database_url,
        &retry_interval,
        &max_retry_duration,
    )
    .await?;

    let mev_pool = get_db_connection(
        &APP_CONFIG.database_url,
        &retry_interval,
        &max_retry_duration,
    )
    .await?;
    let loki_client = LokiClient::new(APP_CONFIG.loki_url.clone());

    loop {
        let canonical_horizon = Utc::now() - Duration::minutes(APP_CONFIG.canonical_wait_minutes);
        run_demotion_monitor(&relay_pool, &mev_pool).await?;
        run_inclusion_monitor(&relay_pool, &mev_pool, &canonical_horizon, &loki_client).await?;
        run_promotion_monitor(&relay_pool, &mev_pool, &canonical_horizon).await?;
        tokio::time::sleep(Duration::minutes(1).to_std().unwrap()).await;
    }
}

pub async fn monitor_critical_services() -> Result<()> {
    log::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.database_url).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let last_checked = Arc::new(Mutex::new(Utc::now()));

    let result = tokio::try_join!(
        mount_health_route(),
        run_alarm_loop(last_checked),
        run_ops_monitors()
    );

    match result {
        Ok(_) => {
            let message = "phoenix processes exited unexpectedly";
            error!("{}", &message);
            alert::send_telegram_alert(message).await?;
            Err(anyhow!(message))
        }
        Err(err) => {
            let message = format!("phoenix process exited with error: {}", err);
            error!("{}", &message);
            alert::send_telegram_alert(&message).await?;
            Err(err)
        }
    }
}
