#[tokio::main]
pub async fn main() {
    relay_backend::start_server().await;
}
