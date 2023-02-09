#[tokio::main]
pub async fn main() {
    relay_backend::monitor_critical_services().await;
}
