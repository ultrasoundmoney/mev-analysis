mod alerts;
mod auction_analysis_monitor;
mod checkpoint;
mod consensus_node;
mod delay_update_monitor;
mod demotion_monitor;
mod env;
mod inclusion_monitor;
mod lookback_update_monitor;
mod promotion_monitor;
mod util;
mod validation_node;

use std::{collections::HashMap, net::SocketAddr};

use alerts::telegram::{Channel, TELEGRAM_SAFE_MESSAGE_LENGTH};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use axum::{http::StatusCode, routing::get, Router};
use chrono::{DateTime, Duration, Utc};
use env::APP_CONFIG;
use indoc::formatdoc;
use sqlx::{postgres::PgPoolOptions, Connection, PgConnection, PgPool};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

use crate::log;
use crate::phoenix::{
    consensus_node::ConsensusNodeMonitor, validation_node::ValidationNodeMonitor,
};

use self::{
    alerts::telegram::{self, TelegramAlerts, TelegramSafeAlert},
    auction_analysis_monitor::run_auction_analysis_monitor,
    delay_update_monitor::run_header_delay_updates_monitor,
    demotion_monitor::run_demotion_monitor,
    inclusion_monitor::{run_inclusion_monitor, LokiClient},
    lookback_update_monitor::run_lookback_updates_monitor,
    promotion_monitor::run_promotion_monitor,
};

const PHOENIX_MAX_LIFESPAN: Duration = Duration::minutes(3);
const MIN_ALARM_WAIT: Duration = Duration::minutes(4);
const MIN_WARNING_WAIT: Duration = Duration::minutes(60);

#[derive(Clone, Eq, Hash, PartialEq)]
enum AlarmType {
    Telegram,
    Opsgenie,
}

impl AlarmType {
    fn min_wait(&self) -> Duration {
        match self {
            AlarmType::Opsgenie => MIN_ALARM_WAIT,
            AlarmType::Telegram => MIN_WARNING_WAIT,
        }
    }
}

struct Alarm {
    last_fired: HashMap<AlarmType, DateTime<Utc>>,
    telegram_alerts: TelegramAlerts,
}

impl Alarm {
    fn new() -> Self {
        Self {
            last_fired: HashMap::new(),
            telegram_alerts: TelegramAlerts::new(),
        }
    }

    fn is_throttled(&self, alarm_type: &AlarmType) -> bool {
        self.last_fired.get(alarm_type).map_or(false, |last_fired| {
            Utc::now() - last_fired < alarm_type.min_wait()
        })
    }

    async fn fire(&mut self, message: &str, alarm_type: &AlarmType) {
        if self.is_throttled(alarm_type) {
            warn!("alarm is throttled, ignoring request to fire alarm");
            return;
        }

        error!(message, "firing alarm");

        match alarm_type {
            AlarmType::Opsgenie => alerts::send_opsgenie_telegram_alert(message).await,
            AlarmType::Telegram => {
                self.telegram_alerts
                    .send_message(&TelegramSafeAlert::new(message), Channel::Warnings)
                    .await
            }
        }

        self.last_fired.insert(alarm_type.clone(), Utc::now());
    }
}

struct NodeAlarm {
    alarm: Alarm,
}

impl NodeAlarm {
    fn new() -> Self {
        Self {
            alarm: Alarm::new(),
        }
    }

    async fn fire_age_over_limit(&mut self, name: &str) {
        let message = format!(
            "{} hasn't updated for more than {} seconds on {}",
            name,
            PHOENIX_MAX_LIFESPAN.num_seconds(),
            APP_CONFIG.geo
        );
        self.alarm.fire(&message, &AlarmType::Opsgenie).await;
    }

    async fn fire_num_unsynced_nodes(&mut self, name: &str, num_unsynced_nodes: usize) {
        let message = format!(
            "{} has {} unsynced instances on {}",
            name, num_unsynced_nodes, APP_CONFIG.geo
        );

        if num_unsynced_nodes >= APP_CONFIG.unsynced_nodes_threshold_og_alert {
            self.alarm.fire(&message, &AlarmType::Opsgenie).await;
        }
        if num_unsynced_nodes >= APP_CONFIG.unsynced_nodes_threshold_tg_warning {
            self.alarm.fire(&message, &AlarmType::Telegram).await;
        }
    }
}

struct Phoenix {
    name: &'static str,
    last_seen: DateTime<Utc>,
    num_unsynced_nodes: usize,
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
        age >= PHOENIX_MAX_LIFESPAN
    }

    fn set_last_seen(&mut self, last_seen: DateTime<Utc>) {
        debug!(name = self.name, ?last_seen, "setting last seen");
        self.last_seen = last_seen;
    }
}

#[async_trait]
trait PhoenixMonitor {
    async fn refresh(&self) -> (DateTime<Utc>, usize);
}

async fn run_alarm_loop() -> Result<()> {
    info!(
        "releasing phoenix, dies after {} seconds",
        PHOENIX_MAX_LIFESPAN.num_seconds()
    );

    let mut alarm = NodeAlarm::new();

    let mut phoenixes = [
        Phoenix {
            last_seen: Utc::now(),
            monitor: Box::new(ConsensusNodeMonitor::new()),
            num_unsynced_nodes: 0,
            name: "consensus node",
        },
        Phoenix {
            last_seen: Utc::now(),
            num_unsynced_nodes: 0,
            monitor: Box::new(ValidationNodeMonitor::new()),
            name: "validation node",
        },
    ];

    loop {
        for phoenix in phoenixes.iter_mut() {
            if phoenix.is_age_over_limit() {
                alarm.fire_age_over_limit(phoenix.name).await;
            } else {
                alarm
                    .fire_num_unsynced_nodes(phoenix.name, phoenix.num_unsynced_nodes)
                    .await;
            }

            let (current, num_unsynced_nodes) = phoenix.monitor.refresh().await;
            phoenix.num_unsynced_nodes = num_unsynced_nodes;
            phoenix.set_last_seen(current)
        }

        info!("alarm loop completed, sleeping for 10 seconds");

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
async fn connect_db(
    db_url: &str,
    retry_interval: &Duration,
    max_retry_duration: &Duration,
) -> Result<PgPool> {
    let start_time = Instant::now();
    loop {
        match PgPoolOptions::new()
            .max_connections(3)
            .acquire_timeout(std::time::Duration::from_secs(9))
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
                alerts::send_opsgenie_telegram_alert(
                    "phoenix service failed to connect to database",
                )
                .await;
                return Err(error.into());
            }
        }
    }
}

async fn run_ops_monitors() -> Result<()> {
    let max_retry_duration = Duration::minutes(2);
    let retry_interval = Duration::seconds(10);

    let relay_pool = connect_db(
        &APP_CONFIG.relay_database_url,
        &retry_interval,
        &max_retry_duration,
    )
    .await?;

    let mev_pool = connect_db(
        &APP_CONFIG.database_url,
        &retry_interval,
        &max_retry_duration,
    )
    .await?;
    let loki_client = LokiClient::new(APP_CONFIG.loki_url.clone());

    // Separate alarm instances mean throttling will be applied separately
    let mut auction_analysis_alarm = Alarm::new();
    let mut header_delay_updates_alarm = Alarm::new();
    let mut run_lookback_updates_monitor_alarm = Alarm::new();

    loop {
        let canonical_horizon = Utc::now() - Duration::minutes(APP_CONFIG.canonical_wait_minutes);
        run_demotion_monitor(&relay_pool, &mev_pool).await?;
        run_inclusion_monitor(&relay_pool, &mev_pool, &canonical_horizon, &loki_client).await?;
        run_promotion_monitor(&relay_pool, &mev_pool, &canonical_horizon).await?;
        run_auction_analysis_monitor(&mev_pool, &mut auction_analysis_alarm).await?;
        run_header_delay_updates_monitor(&mev_pool, &mut header_delay_updates_alarm).await?;
        run_lookback_updates_monitor(&mev_pool, &mut run_lookback_updates_monitor_alarm).await?;
        tokio::time::sleep(Duration::minutes(1).to_std().unwrap()).await;
    }
}

pub async fn monitor_critical_services() -> Result<()> {
    log::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.database_url).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let telegram_alerts = TelegramAlerts::new();

    // Skip global checks and only check nodes
    if APP_CONFIG.ff_node_check_only {
        let result = tokio::try_join!(mount_health_route(), run_alarm_loop());
        match result {
            Ok(_) => handle_unexpected_exit(telegram_alerts).await,
            Err(err) => handle_unexpected_error(telegram_alerts, err).await,
        }
    }
    // Run all checks
    else {
        let result = tokio::try_join!(mount_health_route(), run_alarm_loop(), run_ops_monitors());
        match result {
            Ok(_) => handle_unexpected_exit(telegram_alerts).await,
            Err(err) => handle_unexpected_error(telegram_alerts, err).await,
        }
    }
}

async fn handle_unexpected_exit(telegram_alerts: TelegramAlerts) -> Result<()> {
    let message = TelegramSafeAlert::new("phoenix processes exited unexpectedly");
    telegram_alerts
        .send_message(&message, Channel::Alerts)
        .await;
    Err(anyhow!(message))
}

async fn handle_unexpected_error(
    telegram_alerts: TelegramAlerts,
    err: anyhow::Error,
) -> Result<()> {
    let shortned_err = err
        .to_string()
        .chars()
        .take(TELEGRAM_SAFE_MESSAGE_LENGTH)
        .collect::<String>();
    let escaped_err = telegram::escape_str(&shortned_err);
    let formatted_message = formatdoc!(
        "
        phoenix process exited with error
        ```error
        {escaped_err}
        ```
        "
    );
    let message = TelegramSafeAlert::from_escaped_string(formatted_message);
    telegram_alerts
        .send_message(&message, Channel::Alerts)
        .await;
    Err(anyhow!(err))
}
