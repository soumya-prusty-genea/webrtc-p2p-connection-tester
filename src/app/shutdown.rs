use anyhow::Result;
use log::info;

use crate::state::AppState;

pub async fn graceful_shutdown(app_state: &AppState) -> Result<()> {
    let room_names = app_state.list_cameras();
    for room in room_names {
        let _ = app_state.remove_camera(&room).await;
    }
    info!(" Graceful shutdown complete");
    Ok(())
}
