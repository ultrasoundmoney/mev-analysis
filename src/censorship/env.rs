use lazy_static::lazy_static;
use reqwest::Url;

pub struct AppConfig {
    pub db_connection_str: String,
    pub zeromev_connection_str: String,
    pub relay_urls: Vec<Url>,
}

fn get_app_config() -> AppConfig {
    let db_connection_str = std::env::var("DATABASE_URL");
    let zeromev_connection_str = std::env::var("ZEROMEV_DATABASE_URL");
    let relay_urls = vec![
        Url::parse("https://boost-relay.flashbots.net").unwrap(),
        Url::parse("https://relay.ultrasound.money").unwrap(),
        Url::parse("https://agnostic-relay.net").unwrap(),
        Url::parse("https://bloxroute.max-profit.blxrbdn.com").unwrap(),
        Url::parse("https://bloxroute.regulated.blxrbdn.com").unwrap(),
        Url::parse("https://bloxroute.ethical.blxrbdn.com").unwrap(),
        Url::parse("https://builder-relay-mainnet.blocknative.com").unwrap(),
        Url::parse("https://relay.edennetwork.io").unwrap(),
        Url::parse("https://relayooor.wtf").unwrap(),
        Url::parse("https://relayooor.wtf").unwrap(),
    ];

    if let (Ok(db_connection_str), Ok(zeromev_connection_str)) =
        (db_connection_str, zeromev_connection_str)
    {
        AppConfig {
            db_connection_str,
            zeromev_connection_str,
            relay_urls,
        }
    } else {
        panic!("failed to construct AppConfig")
    }
}

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = get_app_config();
}
