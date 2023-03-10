mod builder;
mod censorship;
mod env;
mod payload;
mod relay_redis;
mod validator;

use anyhow::Result;
use axum::{
    extract::State,
    http::{Method, StatusCode},
    routing::get,
    Json, Router,
};
use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    Connection, PgConnection,
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::beacon_api::BeaconAPI;
use env::APP_CONFIG;

#[derive(Clone)]
pub struct AppState {
    mev_db_pool: PgPool,
    relay_db_pool: PgPool,
    redis_client: redis::Client,
    beacon_api: BeaconAPI,
    validator_index_cache: Arc<Mutex<HashMap<String, String>>>,
}

pub async fn start_server() -> Result<()> {
    tracing_subscriber::fmt::init();

    let mut db_conn = PgConnection::connect(&APP_CONFIG.database_url).await?;
    sqlx::migrate!().run(&mut db_conn).await?;
    db_conn.close().await?;

    let addr = SocketAddr::from(([0, 0, 0, 0], APP_CONFIG.port));

    let mev_db_pool = PgPoolOptions::new()
        .max_connections(30)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&APP_CONFIG.database_url)
        .await
        .expect("can't connect to mev database");

    let relay_db_pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&APP_CONFIG.relay_database_url)
        .await
        .expect("can't connect to relay database");

    let redis_client = redis::Client::open(format!("redis://{}", APP_CONFIG.redis_uri))?;

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any);

    let validator_index_cache = Arc::new(Mutex::new(HashMap::new()));

    let beacon_api = BeaconAPI::new(&APP_CONFIG.consensus_nodes);

    let shared_state = AppState {
        mev_db_pool,
        relay_db_pool,
        redis_client,
        beacon_api,
        validator_index_cache,
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/api/validators", get(validator::validator_registrations))
        .route("/api/validators/stats", get(validator::validator_stats))
        .route(
            "/api/validators/:pubkey",
            get(validator::check_validator_registration),
        )
        .route("/api/payloads", get(payload::delivered_payloads))
        .route("/api/payloads/stats", get(payload::payload_stats))
        .route("/api/payloads/top", get(payload::top_payloads))
        .route("/api/builders/top", get(builder::top_builders))
        .route("/api/censorship/operators", get(censorship::operators))
        .route("/api/censorship/builders", get(censorship::builders))
        .route("/api/censorship/relays", get(censorship::relays))
        .route(
            "/api/censorship/censorship-categories",
            get(censorship::censorship_categories),
        )
        .route(
            "/api/censorship/delay-categories",
            get(censorship::delay_categories),
        )
        .route("/api/censorship/delayed-txs", get(censorship::delayed_txs))
        .route(
            "/api/censorship/recent-delayed-txs",
            get(censorship::recent_delayed_txs),
        )
        .route(
            "/api/censorship/censored-txs",
            get(censorship::censored_txs),
        )
        .with_state(shared_state)
        .layer(cors);

    info!("listening on {}", addr);

    let _ = axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn health(State(state): State<AppState>) -> StatusCode {
    let mev_conn = state.mev_db_pool.acquire().await;
    let relay_conn = state.relay_db_pool.acquire().await;
    let redis_conn = state.redis_client.get_async_connection().await;
    match (mev_conn, relay_conn, redis_conn) {
        (Ok(_), Ok(_), Ok(_)) => StatusCode::OK,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub type ApiResponse<T> = Result<Json<T>, (StatusCode, String)>;

pub fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::fmt::Display,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
