use anyhow::Result;

#[tokio::main]
pub async fn main() -> Result<()> {
    relay_backend::start_server().await
}
