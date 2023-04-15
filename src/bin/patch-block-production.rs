use std::env;

use anyhow::Result;

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    let start = args[1].parse::<i64>()?;
    let end = args[2].parse::<i64>()?;
    relay_backend::patch_block_production_interval(start, end).await
}
