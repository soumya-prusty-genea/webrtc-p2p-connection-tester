use crate::utils::RuntimeConfig;

#[derive(Clone)]
pub struct Settings {
    pub runtime: RuntimeConfig,
}

impl Settings {
    pub fn load() -> Self {
        // Environment-only configuration mode.
        // In Docker, `.env` is injected by docker-compose via `env_file`.
        // Outside Docker, export variables in the shell before running.
        Self {
            runtime: RuntimeConfig::from_env(),
        }
    }
}
