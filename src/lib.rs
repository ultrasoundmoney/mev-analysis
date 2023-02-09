mod beacon_api;
mod env;
mod phoenix;
mod serve;

pub use phoenix::monitor_critical_services;
pub use serve::start_server;
