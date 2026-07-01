use anyhow::Result;
use crate::utils::RuntimeConfig;

pub async fn run_async(config: RuntimeConfig) -> Result<()> {
    crate::runtime::run(config).await
}
