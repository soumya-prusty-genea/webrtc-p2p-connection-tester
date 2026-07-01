use anyhow::Result;

pub fn validate_source_endpoint(endpoint: &str) -> Result<()> {
    if endpoint.trim().is_empty() {
        anyhow::bail!("ZeroMQ source endpoint cannot be empty");
    }

    if !endpoint.starts_with("tcp://")
        && !endpoint.starts_with("ipc://")
        && !endpoint.starts_with("inproc://")
    {
        anyhow::bail!("Unsupported ZeroMQ endpoint scheme: {}", endpoint);
    }

    Ok(())
}
