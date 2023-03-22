mod builder;
mod consensus_node;
mod env;
mod inclusion_monitor;
mod validation_node;

use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use async_trait::async_trait;
use axum::{
    http::{HeaderMap, HeaderValue, StatusCode},
    routing::get,
    Router,
};
use chrono::{DateTime, Duration, Utc};
use env::APP_CONFIG;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde_json::json;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    ConnectOptions, Connection, PgConnection,
};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::phoenix::{
    builder::BuilderStatusMonitor, consensus_node::ConsensusNodeMonitor,
    inclusion_monitor::start_inclusion_monitor, validation_node::ValidationNodeMonitor,
};

lazy_static! {
    static ref PHOENIX_MAX_LIFESPAN: Duration = Duration::minutes(6);
    static ref MIN_ALARM_WAIT: Duration = Duration::minutes(4);
}

#[derive(Deserialize)]
struct OpsGenieError {
    message: String,
}

struct Alarm {
    client: reqwest::Client,
    last_fired: Option<DateTime<Utc>>,
}

impl Alarm {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            last_fired: None,
        }
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

        let mut headers = HeaderMap::new();
        let auth_header = format!("GenieKey {}", &APP_CONFIG.opsgenie_api_key);

        headers.insert(
            "Authorization",
            HeaderValue::from_str(&auth_header).unwrap(),
        );

        let res = self
            .client
            .post("https://api.opsgenie.com/v2/alerts")
            .headers(headers)
            .json(&json!({ "message": message }))
            .send()
            .await
            .unwrap();

        if res.status() != 202 {
            match res.json::<OpsGenieError>().await {
                Err(_) => {
                    panic!("failed to create alarm with OpsGenie")
                }
                Ok(body) => {
                    panic!(
                        "failed to create alarm with OpsGenie, message: {}",
                        body.message
                    )
                }
            }
        }

        self.last_fired = Some(Utc::now());
    }

    async fn fire_with_name(&mut self, name: &str) {
        let message = format!(
            "{} hasn't updated for more than {} seconds on {}!",
            name,
            PHOENIX_MAX_LIFESPAN.num_seconds(),
            &APP_CONFIG.env
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

async fn run_alarm_loop(last_checked: Arc<Mutex<DateTime<Utc>>>) {
    info!(
        "releasing phoenix, dies after {} seconds",
        PHOENIX_MAX_LIFESPAN.num_seconds()
    );

    let mut alarm = Alarm::new();

    let mut phoenixes = vec![
        Phoenix {
            last_seen: Utc::now(),
            monitor: Box::new(BuilderStatusMonitor::new()),
            name: "builder-status",
        },
        Phoenix {
            last_seen: Utc::now(),
            monitor: Box::new(ConsensusNodeMonitor::new()),
            name: "consensus-node",
        },
        Phoenix {
            last_seen: Utc::now(),
            monitor: Box::new(ValidationNodeMonitor::new()),
            name: "validation-node",
        },
    ];

    loop {
        for phoenix in phoenixes.iter_mut() {
            if phoenix.is_age_over_limit() {
                alarm.fire_with_name(&phoenix.name).await;
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

async fn mount_health_route() {
    let addr = SocketAddr::from(([0, 0, 0, 0], APP_CONFIG.port));
    let app = Router::new().route("/", get(|| async { StatusCode::OK }));

    info!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

pub async fn monitor_critical_services() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.database_url).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let relay_pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::seconds(3).to_std()?)
        .connect(&APP_CONFIG.relay_database_url)
        .await?;

    let mev_opts = PgConnectOptions::from_str(&APP_CONFIG.database_url)?
        .disable_statement_logging()
        .to_owned();
    let mev_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(mev_opts)
        .await?;

    tokio::spawn(mount_health_route());
    tokio::spawn(async move { start_inclusion_monitor(&relay_pool, &mev_pool).await? });

    let last_checked = Arc::new(Mutex::new(Utc::now()));
    run_alarm_loop(last_checked).await;

    Ok(())
}
