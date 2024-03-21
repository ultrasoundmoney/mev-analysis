use anyhow::Result;

#[tokio::main]
pub async fn main() -> Result<()> {
    relay_backend::monitor_critical_services().await?;
    Ok(())
}
