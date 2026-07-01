use anyhow::Result;
use std::sync::Arc;
use gstreamer as gst;
use log::{error, info, warn};
use tokio::time::{sleep, Duration};

use crate::auth::JwtConfig;
use crate::camera_manager::CameraStream;
use crate::events::{CameraInfo, VideoCodec};
use crate::messaging::SignalingSocketIOClient;
use crate::monitoring::PerformanceMonitor;
use crate::state::AppState;
use crate::utils::{init_logging, RuntimeConfig};

pub async fn run(config: RuntimeConfig) -> Result<()> {
    init_logging();
    info!(" Starting Multi-Camera WebRTC SFU with External Signaling");

    gst::init()?;
    info!(" GStreamer initialized");

    log_runtime_config(&config);

    let test_stream_mode = env_bool("USE_TEST_STREAM", true);

    let app_state = Arc::new(AppState::new_socketio(config.sfu_id.clone(), config.clone()));
    info!(" Application state initialized");

    if test_stream_mode {
        info!(" Skipping persistent camera restore in test stream mode");
    } else {
        initialize_persistent_state(&app_state).await;
    }

    let perf_monitor = Arc::new(PerformanceMonitor::new());
    perf_monitor.start();
    info!(" Performance monitoring started");

    start_signaling_socketio_thread(
        config.sfu_id.clone(),
        config.signaling_server_url.clone(),
        app_state.clone(),
        config.jwt_config(),
    );

    info!(" Signaling Socket.IO client started");

    if test_stream_mode {
        info!(" Test stream mode enabled (synthetic camera)");
        ensure_test_stream_camera(&app_state).await?;
        info!(" Test stream is ready; waiting for viewer requests");
    } else {
        info!(" Waiting for viewer requests (cameras restored from persistent state)");
    }

    loop {
        sleep(Duration::from_secs(60)).await;
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn log_runtime_config(config: &RuntimeConfig) {
    info!(" Configuration:");
    info!("  SFU ID: {}", config.sfu_id);
    info!("  Signaling Server Socket.IO: {}", config.signaling_server_url);
    match (&config.jwt_auth_enabled, &config.gateway_uuid, &config.customer_uuid, &config.jwt_secret) {
        (true, Some(gw), Some(cu), Some(_)) => {
            info!("  Auth: JWT ENABLED  (gateway={}, customer={})", gw, cu);
        }
        (false, _, _, _) => {
            info!("  Auth: DISABLED via JWT_AUTH_ENABLED=false");
        }
        _ => {
            info!("  Auth: DISABLED (set GATEWAY_UUID, CUSTOMER_UUID and JWT_SECRET to enable)");
        }
    }
}

async fn initialize_persistent_state(app_state: &Arc<AppState>) {
    info!(" Initializing persistent state...");
    if let Err(e) = app_state.initialize_persistent_state().await {
        error!(" Failed to initialize persistent state: {}", e);
        error!(" Continuing without persistent state restoration...");
    } else {
        info!(" Persistent state initialized successfully");
    }
}

fn start_signaling_socketio_thread(
    sfu_id: String,
    signaling_url: String,
    app_state: Arc<AppState>,
    jwt_config: Option<JwtConfig>,
) {
    std::thread::spawn(move || {
        let mut signaling_socketio_client =
            SignalingSocketIOClient::new(sfu_id, signaling_url, app_state, jwt_config);

        let base_delay: u64 = std::env::var("SIGNALING_RECONNECT_DELAY_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(5);
        const MAX_DELAY: u64 = 60;
        let mut current_delay = base_delay;

        loop {
            info!(" Opening fresh signaling WebSocket session...");
            let failed = match signaling_socketio_client.connect_and_run() {
                Ok(()) => {
                    warn!(
                        " Signaling session ended; reconnecting in {}s",
                        base_delay
                    );
                    false
                }
                Err(e) => {
                    error!(
                        " Signaling session failed ({}); reconnecting in {}s",
                        e, current_delay
                    );
                    true
                }
            };

            std::thread::sleep(std::time::Duration::from_secs(
                if failed { current_delay } else { base_delay },
            ));
            current_delay = if failed {
                (current_delay * 2).min(MAX_DELAY)
            } else {
                base_delay
            };
        }
    });
}

async fn ensure_test_stream_camera(app_state: &Arc<AppState>) -> Result<()> {
    let room_name = std::env::var("TEST_STREAM_ID").unwrap_or_else(|_| "test-stream".to_string());
    if app_state.has_camera(&room_name) {
        info!(" Test stream camera already present: {}", room_name);
        return Ok(());
    }

    let camera_uuid = std::env::var("TEST_CAMERA_UUID").unwrap_or_else(|_| "CAM-TEST-001".to_string());
    let camera_name = std::env::var("TEST_CAMERA_NAME").unwrap_or_else(|_| "Synthetic Test Camera".to_string());
    let codec = VideoCodec::H264;
    let camera_info = CameraInfo {
        camera_uuid,
        camera_name,
        room_name: room_name.clone(),
        // Marker endpoint: handled by CameraStream::start_zmq_consumption()
        // as a built-in GStreamer test pattern source.
        zmq_endpoint: "test://videotestsrc?frames=60".to_string(),
        codec,
    };

    let camera = CameraStream::new(
        camera_info,
        app_state.config.webrtc_stun_server.clone(),
        app_state.config.webrtc_turn_servers.clone(),
    )?;

    app_state.add_camera(camera).await?;
    info!(" Registered synthetic test stream '{}'", room_name);
    Ok(())
}
