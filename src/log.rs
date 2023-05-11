use tracing_subscriber::{
    fmt::Layer, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

pub fn init() {
    tracing_subscriber::registry()
        .with(Layer::default().json().flatten_event(true))
        .with(EnvFilter::from_default_env())
        .init();
}
