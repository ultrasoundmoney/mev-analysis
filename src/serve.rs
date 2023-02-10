use axum::{
    extract::State,
    http::{Method, StatusCode},
    routing::get,
    Router,
};
use futures::{try_join, TryFutureExt};
use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    Pool, Postgres,
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::task::JoinHandle;
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, error, info};

extern crate redis;

mod builder;
mod env;
mod payload;
mod relay_redis;
mod validator;

use env::APP_CONFIG;

use builder::Builder;

use crate::beacon_api::BeaconAPI;

type CachedValue<T> = RwLock<Option<T>>;

struct Cache {
    top_builders: CachedValue<Vec<Builder>>,
}

async fn update_cache(
    pool: &Pool<Postgres>,
    cache: &CachedValue<Vec<Builder>>,
) -> Result<(), String> {
    let new_builders = builder::get_top_builders(pool).await;

    match new_builders {
        Ok(builders) => {
            let mut new_cache = cache.write().unwrap();
            *new_cache = Some(builders);
            Ok(())
        }
        Err(err) => {
            error!("failed to get new builders during cache update");
            Err(err)
        }
    }
}

async fn start_cache_update(state: AppState) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let res = update_cache(&state.db_pool, &state.cache.top_builders).await;
            match res {
                Ok(_) => {
                    debug!("cache updated");
                }
                Err(err) => {
                    error!("failed to update cache: {}", err);
                }
            }
            tokio::time::sleep(Duration::from_secs(5 * 60)).await;
        }
    })
}

#[derive(Clone)]
pub struct AppState {
    db_pool: PgPool,
    redis_client: redis::Client,
    beacon_api: BeaconAPI,
    cache: Arc<Cache>,
    // avoid calling sync-leader nodes for every displayed validator registration every time
    validator_index_cache: Arc<Mutex<HashMap<String, String>>>,
}

pub async fn start_server() {
    tracing_subscriber::fmt::init();

    let addr = SocketAddr::from(([0, 0, 0, 0], APP_CONFIG.port));

    let db_pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&APP_CONFIG.db_connection_str)
        .await
        .expect("can't connect to database");

    let redis_client = redis::Client::open(APP_CONFIG.redis_url.clone()).unwrap();

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any);

    let validator_index_cache = Arc::new(Mutex::new(HashMap::new()));

    let cache = Arc::new(Cache {
        top_builders: RwLock::new(None),
    });

    let beacon_api = BeaconAPI::new(&APP_CONFIG.consensus_nodes);

    let shared_state = AppState {
        db_pool,
        redis_client,
        beacon_api,
        cache,
        validator_index_cache,
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/api/validators", get(validator::validator_registrations))
        // deprecate route
        .route("/api/validators/count", get(validator::validator_stats))
        .route("/api/validators/stats", get(validator::validator_stats))
        .route(
            "/api/validators/:pubkey",
            get(validator::check_validator_registration),
        )
        .route("/api/payloads", get(payload::delivered_payloads))
        // deprecate route
        .route("/api/payloads/count", get(payload::payload_stats))
        .route("/api/payloads/stats", get(payload::payload_stats))
        .route("/api/payloads/top", get(payload::top_payloads))
        .route("/api/builders/top", get(builder::top_builders))
        .with_state(shared_state.clone())
        .layer(cors);

    let update_cache_thread = start_cache_update(shared_state.clone()).await;
    let server_thread = axum::Server::bind(&addr).serve(app.into_make_service());

    info!("listening on {}", addr);

    try_join!(
        update_cache_thread.map_err(|err| error!("{}", err)),
        server_thread.map_err(|err| error!("{}", err))
    )
    .unwrap();
}

async fn health(State(state): State<AppState>) -> StatusCode {
    let conn = &state.db_pool.acquire().await;
    match conn {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
