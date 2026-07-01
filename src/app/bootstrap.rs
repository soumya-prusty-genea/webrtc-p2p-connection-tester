use anyhow::Result;
use log::info;

use crate::config::Settings;
use crate::utils::init_logging;

pub struct App {
    pub settings: Settings,
}

impl App {
    pub fn run(self) -> Result<()> {
        info!(" App bootstrap run start for SFU {}", self.settings.runtime.sfu_id);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        // Pass the already-loaded config so runtime does not re-read env vars
        // and Settings::load() (file + env) remains the single source of truth.
        rt.block_on(crate::app::runtime::run_async(self.settings.runtime))
    }
}

pub fn initialize() -> Result<App> {
    let settings = Settings::load();
    init_logging();
    info!(" Bootstrap initialized");
    Ok(App { settings })
}
