use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use rust_socketio::client::Client;

use crate::camera_manager::CameraStream;
use crate::events::CameraInfo;
use crate::metrics;
use crate::state::state_manager::StateManager;
use crate::tester::report::NetworkProbeReport;
use crate::utils::config::RuntimeConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewerLimitMode {
    Camera,
    Gateway,
}

/// Global application state managing all camera streams
#[derive(Clone)]
pub struct AppState {
    /// SFU ID for this instance
    pub sfu_id: String,
    /// Map of room_name -> CameraStream
    pub cameras: Arc<RwLock<HashMap<String, CameraStream>>>,
    /// Socket.IO client for signaling server
    pub socketio_signaling_client: Arc<RwLock<Option<Client>>>,
    /// Maximum viewers per camera (configurable limit)
    pub max_viewers_per_camera: usize,
    /// Maximum total viewers across all cameras in this gateway instance
    pub max_viewers_per_gateway: usize,
    /// Active viewer limit mode (camera or gateway)
    pub viewer_limit_mode: ViewerLimitMode,
    /// Persistent state manager
    pub state_manager: Arc<RwLock<StateManager>>,
    /// Runtime configuration
    pub config: RuntimeConfig,
    /// Startup network probe results (STUN/NAT/TURN/Signaling)
    pub network_probe_result: Arc<Mutex<Option<NetworkProbeReport>>>,
}

impl AppState {
    /// Create new AppState for Socket.IO mode
    pub fn new_socketio(sfu_id: String, config: RuntimeConfig) -> Self {
        // Get max viewers per camera from environment variable (default: 10)
        let max_viewers = std::env::var("MAX_VIEWERS_PER_CAMERA")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<usize>()
            .unwrap_or(10);

        // Get max total viewers per gateway from environment variable (default: 50)
        let max_gateway_viewers = match std::env::var("MAX_VIEWERS_PER_GATEWAY") {
            Ok(value) => match value.parse::<usize>() {
                Ok(parsed) => parsed,
                Err(_) => {
                    log::warn!(
                        " Invalid MAX_VIEWERS_PER_GATEWAY='{}'. Falling back to 50",
                        value
                    );
                    50
                }
            },
            Err(_) => 50,
        };

        // Select one active limit policy. Supported values: camera | gateway
        let viewer_limit_mode = match std::env::var("VIEWER_LIMIT_MODE")
            .unwrap_or_else(|_| "camera".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "gateway" => ViewerLimitMode::Gateway,
            "camera" => ViewerLimitMode::Camera,
            other => {
                log::warn!(
                    " Invalid VIEWER_LIMIT_MODE='{}'. Falling back to 'camera'",
                    other
                );
                ViewerLimitMode::Camera
            }
        };

        // Get state file path from environment variable
        let state_file_path = std::env::var("STATE_FILE_PATH")
            .ok();

        log::info!(" Maximum viewers per camera: {}", max_viewers);
        log::info!(" Maximum viewers per gateway: {}", max_gateway_viewers);
        log::info!(" Viewer limit mode: {:?}", viewer_limit_mode);
        log::info!(" State file path: {:?}", state_file_path.as_ref().unwrap_or(&"sfu_state.json".to_string()));

        // Create state manager
        let state_manager = StateManager::new(sfu_id.clone(), state_file_path);

        AppState {
            sfu_id,
            cameras: Arc::new(RwLock::new(HashMap::new())),
            socketio_signaling_client: Arc::new(RwLock::new(None)),
            max_viewers_per_camera: max_viewers,
            max_viewers_per_gateway: max_gateway_viewers,
            viewer_limit_mode,
            state_manager: Arc::new(RwLock::new(state_manager)),
            config,
            network_probe_result: Arc::new(Mutex::new(None)),
        }
    }

    /// Backward compatibility: Create new AppState for WebSocket-only mode (deprecated)
    #[deprecated(note = "Use new_socketio instead")]
    pub fn new_websocket_only(sfu_id: String, config: RuntimeConfig) -> Self {
        Self::new_socketio(sfu_id, config)
    }

    /// Set maximum viewers per camera
    pub fn with_max_viewers_per_camera(mut self, max_viewers: usize) -> Self {
        self.max_viewers_per_camera = max_viewers;
        log::info!(" Updated maximum viewers per camera: {}", max_viewers);
        self
    }

    /// Set the Socket.IO signaling client
    pub fn set_socketio_signaling_client(&self, client: Client) {
        let mut socketio_client = self.socketio_signaling_client.write()
            .expect("Failed to acquire write lock on Socket.IO client");
        *socketio_client = Some(client);
    }

    /// Get the Socket.IO signaling client
    pub fn get_socketio_signaling_client(&self) -> Option<Client> {
        self.socketio_signaling_client.read()
            .expect("Failed to acquire read lock on Socket.IO client")
            .clone()
    }

    async fn emit_socketio(&self, event: &str, data: serde_json::Value) -> anyhow::Result<()> {
        let client = match self.get_socketio_signaling_client() {
            Some(client) => client,
            None => return Ok(()),
        };

        let event = event.to_string();
        tokio::task::spawn_blocking(move || client.emit(event.as_str(), data))
            .await
            .map_err(|e| anyhow::anyhow!("Socket.IO emit task join error: {}", e))??;

        Ok(())
    }

    /// Backward compatibility: Set the WebSocket signaling sender (deprecated)
    #[deprecated(note = "Use set_socketio_signaling_client instead")]
    pub fn set_ws_signaling_sender(&self, _sender: tokio::sync::mpsc::UnboundedSender<String>) {
        log::warn!(" set_ws_signaling_sender is deprecated, use set_socketio_signaling_client instead");
    }

    /// Register a stream with the Socket.IO signaling server
    pub async fn register_stream_with_socketio_signaling_info(&self, camera_info: &CameraInfo) -> anyhow::Result<()> {
        if self.get_socketio_signaling_client().is_some() {
            let metadata = serde_json::json!({
                "room_name": camera_info.room_name,
                "camera_name": camera_info.camera_name,
                "camera_uuid": camera_info.camera_uuid,
                "resolution": "1080p",
                "fps": 30,
                "codec": camera_info.codec.to_string()
            });

            let register_data = serde_json::json!({
                "sfu_id": self.sfu_id,
                "stream_id": camera_info.room_name,
                "metadata": metadata
            });

            self.emit_socketio("stream_register", register_data).await?;
            log::info!(" Registered stream with Socket.IO signaling server: {} (sfu_id: {}, codec: {})", 
                       camera_info.room_name, self.sfu_id, camera_info.codec.to_string());
        }
        Ok(())
    }
    
    /// Backward compatibility: Register a stream with the WebSocket signaling server (deprecated)
    #[deprecated(note = "Use register_stream_with_socketio_signaling_info instead")]
    pub async fn register_stream_with_ws_signaling_info(&self, camera_info: &CameraInfo) -> anyhow::Result<()> {
        self.register_stream_with_socketio_signaling_info(camera_info).await
    }

    /// Register a stream with the Socket.IO signaling server (legacy method for compatibility)
    pub async fn register_stream_with_socketio_signaling(&self, room_name: &str) -> anyhow::Result<()> {
        if self.get_socketio_signaling_client().is_some() {
            // Try to get real camera info first from app state
            if let Some(camera) = self.get_camera(room_name) {
                // Use real camera info when available
                let metadata = serde_json::json!({
                    "room_name": camera.room_name,
                    "camera_name": camera.camera_name,
                    "camera_uuid": camera.camera_uuid,
                    "resolution": "1080p",
                    "fps": 30,
                    "codec": camera.codec.to_string()
                });

                let register_data = serde_json::json!({
                    "sfu_id": self.sfu_id,
                    "stream_id": room_name,
                    "metadata": metadata
                });

                self.emit_socketio("stream_register", register_data).await?;
                log::info!(" Registered stream with Socket.IO signaling server: {} (sfu_id: {}, real_uuid: {})", 
                          room_name, self.sfu_id, camera.camera_uuid);
            } else {
                // Fallback to legacy fake UUIDs only when camera not found
                let metadata = serde_json::json!({
                    "room_name": room_name,
                    "camera_name": format!("Camera {}", room_name),
                    "camera_uuid": format!("uuid_{}", room_name), // Fallback fake UUID
                    "resolution": "1080p",
                    "fps": 30,
                    "codec": "H264"
                });

                let register_data = serde_json::json!({
                    "sfu_id": self.sfu_id,
                    "stream_id": room_name,
                    "metadata": metadata
                });

                self.emit_socketio("stream_register", register_data).await?;
                log::info!(" Registered stream with Socket.IO signaling server (fallback): {} (sfu_id: {})", room_name, self.sfu_id);
            }
        }
        Ok(())
    }
    
    /// Backward compatibility: Register a stream with the WebSocket signaling server (deprecated)
    #[deprecated(note = "Use register_stream_with_socketio_signaling instead")]
    pub async fn register_stream_with_ws_signaling(&self, room_name: &str) -> anyhow::Result<()> {
        self.register_stream_with_socketio_signaling(room_name).await
    }

    /// Unregister a stream from the Socket.IO signaling server
    pub async fn unregister_stream_from_socketio_signaling(&self, room_name: &str) -> anyhow::Result<()> {
        if self.get_socketio_signaling_client().is_some() {
            // Get real camera info from app state if available
            if let Some(camera) = self.get_camera(room_name) {
                // Include metadata with real camera UUID for unregistration
                let metadata = serde_json::json!({
                    "room_name": camera.room_name,
                    "camera_name": camera.camera_name,
                    "camera_uuid": camera.camera_uuid,
                    "resolution": "1080p",
                    "fps": 30,
                    "codec": camera.codec.to_string()
                });

                let unregister_data = serde_json::json!({
                    "sfu_id": self.sfu_id,
                    "stream_id": room_name,
                    "metadata": metadata
                });

                self.emit_socketio("stream_unregister", unregister_data).await?;
                log::info!(" Unregistered stream from Socket.IO signaling server: {} (sfu_id: {}, real_uuid: {})", 
                          room_name, self.sfu_id, camera.camera_uuid);
            } else {
                // Fallback: send unregister without metadata if camera not found
                let unregister_data = serde_json::json!({
                    "sfu_id": self.sfu_id,
                    "stream_id": room_name
                });

                self.emit_socketio("stream_unregister", unregister_data).await?;
                log::info!(" Unregistered stream from Socket.IO signaling server (fallback): {} (sfu_id: {})", 
                          room_name, self.sfu_id);
            }
        }
        Ok(())
    }

    /// Backward compatibility: Unregister a stream from the WebSocket signaling server (deprecated)
    #[deprecated(note = "Use unregister_stream_from_socketio_signaling instead")]
    pub async fn unregister_stream_from_ws_signaling(&self, room_name: &str) -> anyhow::Result<()> {
        self.unregister_stream_from_socketio_signaling(room_name).await
    }

    /// Send WebRTC offer to viewer via Socket.IO signaling server
    pub async fn send_webrtc_offer_via_signaling(&self, viewer_id: &str, sdp_offer: &str) -> anyhow::Result<()> {
        if self.get_socketio_signaling_client().is_some() {
            let offer_data = serde_json::json!({
                "viewer_id": viewer_id,
                "data": {
                    "sdp": {
                        "type": "offer",
                        "sdp": sdp_offer
                    }
                }
            });

            self.emit_socketio("webrtc_message_to_viewer", offer_data).await?;
            log::info!(" Sent WebRTC offer to viewer {} via Socket.IO signaling server", viewer_id);
        } else {
            log::warn!(" Socket.IO signaling client not available for viewer {}", viewer_id);
        }

        Ok(())
    }

    /// Send ICE candidate to viewer via Socket.IO signaling server
    pub async fn send_ice_candidate_via_signaling(&self, viewer_id: &str, candidate: &str, sdp_m_line_index: u32) -> anyhow::Result<()> {
        if self.get_socketio_signaling_client().is_some() {
            let ice_data = serde_json::json!({
                "viewer_id": viewer_id,
                "data": {
                    "ice": {
                        "candidate": candidate,
                        "sdpMLineIndex": sdp_m_line_index
                    }
                }
            });

            self.emit_socketio("webrtc_message_to_viewer", ice_data).await?;
            log::debug!(" Sent ICE candidate to viewer {} via Socket.IO signaling server", viewer_id);
        } else {
            log::warn!(" Socket.IO signaling client not available for ICE candidate to viewer {}", viewer_id);
        }

        Ok(())
    }

    /// Get comprehensive viewer statistics across all cameras
    pub fn get_all_viewer_stats(&self) -> serde_json::Value {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for stats");
        let mut total_viewers = 0;
        let mut camera_stats = Vec::new();
        let mut active_cameras = 0;
        let mut consuming_cameras = 0;

        for (room_name, camera) in cameras.iter() {
            let viewer_count = camera.get_viewer_count();
            let is_consuming = camera.is_zmq_consuming();
            
            total_viewers += viewer_count;
            if viewer_count > 0 {
                active_cameras += 1;
            }
            if is_consuming {
                consuming_cameras += 1;
            }

            camera_stats.push(serde_json::json!({
                "room_name": room_name,
                "camera_name": camera.camera_name,
                "viewer_count": viewer_count,
                "viewers": camera.get_viewer_ids(),
                "status": camera.get_status(),
                "zmq_consuming": is_consuming
            }));
        }

        serde_json::json!({
            "total_cameras": cameras.len(),
            "active_cameras": active_cameras,
            "consuming_cameras": consuming_cameras,
            "total_viewers": total_viewers,
            "cameras": camera_stats,
            "timestamp": chrono::Utc::now().to_rfc3339()
        })
    }

    /// Get viewer count for a specific camera
    pub fn get_camera_viewer_count(&self, room_name: &str) -> Option<usize> {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for viewer count");
        cameras.get(room_name).map(|camera| camera.get_viewer_count())
    }

    /// Get all viewers across all cameras
    pub fn get_all_viewers(&self) -> std::collections::HashMap<String, Vec<String>> {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for all viewers");
        let mut all_viewers = std::collections::HashMap::new();
        
        for (room_name, camera) in cameras.iter() {
            let viewers = camera.get_viewer_ids();
            if !viewers.is_empty() {
                all_viewers.insert(room_name.clone(), viewers);
            }
        }
        
        all_viewers
    }

    /// Initialize state manager and restore cameras from persistent state
    pub async fn initialize_persistent_state(&self) -> anyhow::Result<()> {
        log::info!(" Initializing persistent state...");
        
        // Load or create state file
        {
            let mut state_manager = self.state_manager.write()
                .expect("Failed to acquire write lock on state manager");
            state_manager.load_or_create()?;
        }

        // Get cameras to restore
        let cameras_to_restore = {
            let state_manager = self.state_manager.read()
                .expect("Failed to acquire read lock on state manager");
            state_manager.get_cameras_to_restore()
        };

        if cameras_to_restore.is_empty() {
            log::info!(" No cameras to restore from persistent state");
            return Ok(());
        }

        log::info!(" Restoring {} cameras from persistent state...", cameras_to_restore.len());

        // Restore each camera
        for camera_info in cameras_to_restore {
            log::info!(" Restoring camera: {} ({})", camera_info.room_name, camera_info.camera_name);
            
            let room_name = camera_info.room_name.clone(); // Clone for error handling
            match self.restore_camera_from_state(camera_info).await {
                Ok(()) => {
                    log::info!(" Successfully restored camera: {}", room_name);
                }
                Err(e) => {
                    log::error!(" Failed to restore camera {}: {}", room_name, e);
                    
                    // Mark camera as error in persistent state
                    let mut state_manager = self.state_manager.write()
                        .expect("Failed to acquire write lock on state manager for error update");
                    let _ = state_manager.update_camera_state(&room_name, "error");
                }
            }
        }

        log::info!(" Persistent state initialization complete");
        Ok(())
    }

    /// Restore a single camera from persistent state
    async fn restore_camera_from_state(&self, camera_info: CameraInfo) -> anyhow::Result<()> {
        // Create camera stream with WebRTC config
        let mut camera = CameraStream::new(
            camera_info.clone(),
            self.config.webrtc_stun_server.clone(),
            self.config.webrtc_turn_servers.clone(),
        )?;
        
        // Start the camera (this creates the pipeline but doesn't start ZMQ consumption)
        camera.start()?;

        // Register with WebSocket signaling server with codec information
        self.register_stream_with_socketio_signaling_info(&camera_info).await?;

        // Add to camera registry
        {
            let mut cameras = self.cameras.write()
                .expect("Failed to acquire write lock on cameras for restoration");
            cameras.insert(camera_info.room_name.clone(), camera);
        }

        metrics::register_stream(
            &camera_info.room_name,
            &camera_info.camera_uuid,
            &camera_info.camera_name,
        );

        log::info!(" Camera restored and registered: {}", camera_info.room_name);
        Ok(())
    }

    /// Clean up stale entries from persistent state
    pub fn cleanup_stale_state_entries(&self, max_inactive_hours: i64) -> anyhow::Result<usize> {
        let mut state_manager = self.state_manager.write()
            .expect("Failed to acquire write lock on state manager for cleanup");
        state_manager.cleanup_stale_entries(max_inactive_hours)
    }

    /// Get persistent state summary
    pub fn get_persistent_state_summary(&self) -> serde_json::Value {
        let state_manager = self.state_manager.read()
            .expect("Failed to acquire read lock on state manager for summary");
        state_manager.get_state_summary()
    }

    /// Add a camera stream to the registry
    pub async fn add_camera(&self, mut camera: CameraStream) -> anyhow::Result<()> {
        let room_name = camera.room_name.clone();
        let camera_uuid = camera.camera_uuid.clone();
        let camera_name = camera.camera_name.clone();
        let codec = camera.codec.clone();

        // Start the camera stream
        camera.start()?;

        // Add to registry
        {
            let mut cameras = self.cameras.write()
                .expect("Failed to acquire write lock on cameras for adding camera");
            cameras.insert(room_name.clone(), camera);
        }

        metrics::register_stream(&room_name, &camera_uuid, &camera_name);

        // Register with WebSocket signaling server including codec information
        let camera_info = CameraInfo {
            camera_uuid: camera_uuid.clone(),
            camera_name: camera_name.clone(),
            room_name: room_name.clone(),
            zmq_endpoint: format!("ipc:///tmp/zmq/{}", camera_uuid),
            codec: codec.clone(),
        };
        
        if let Err(e) = self.register_stream_with_socketio_signaling_info(&camera_info).await {
            log::error!("Failed to register camera {} with WebSocket signaling server: {}", room_name, e);
            // Don't fail the entire operation if signaling registration fails
        }

        // Add to persistent state
        {
            let mut state_manager = self.state_manager.write()
                .expect("Failed to acquire write lock on state manager for adding camera");
            if let Err(e) = state_manager.add_camera(&camera_info) {
                log::error!(" Failed to persist camera state: {}", e);
            }
        }

        log::info!(" Camera added to registry: {} (codec: {}, total: {})", 
                   room_name, codec.to_string(), self.camera_count());

        Ok(())
    }

    /// Remove a camera stream from the registry
    pub async fn remove_camera(&self, room_name: &str) -> anyhow::Result<()> {
        // First, unregister from WebSocket signaling server BEFORE removing from app state
        // This ensures we can still access camera details for real UUID
        if let Err(e) = self.unregister_stream_from_socketio_signaling(room_name).await {
            log::warn!("Failed to unregister camera {} from WebSocket signaling server: {}", room_name, e);
            // Don't fail the operation
        }

        let mut removed_camera = {
            let mut cameras = self.cameras.write()
                .expect("Failed to acquire write lock on cameras for removing camera");
            cameras.remove(room_name)
        };

        if let Some(ref mut camera) = removed_camera {
            // Stop the camera stream
            camera.stop()?;

            metrics::unregister_stream(room_name);

            // Remove from persistent state
            {
                let mut state_manager = self.state_manager.write()
                    .expect("Failed to acquire write lock on state manager for removing camera");
                if let Err(e) = state_manager.remove_camera(room_name) {
                    log::error!(" Failed to remove camera from persistent state: {}", e);
                }
            }

            log::info!(" Camera removed from registry: {} (remaining: {})", room_name, self.camera_count());
        } else {
            log::warn!(" Attempted to remove non-existent camera: {}", room_name);
        }

        Ok(())
    }

    /// Get a camera stream by room name
    pub fn get_camera(&self, room_name: &str) -> Option<CameraStream> {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for get_camera");
        cameras.get(room_name).cloned()
    }

    /// Check if a camera exists
    pub fn has_camera(&self, room_name: &str) -> bool {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for has_camera");
        cameras.contains_key(room_name)
    }

    /// Get all camera room names
    pub fn get_all_camera_names(&self) -> Vec<String> {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for get_all_camera_names");
        cameras.keys().cloned().collect()
    }

    /// List cameras for debugging
    pub fn list_cameras(&self) -> Vec<String> {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for list_cameras");
        cameras.keys().cloned().collect()
    }

    /// Get count of active cameras
    pub fn camera_count(&self) -> usize {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for camera_count");
        cameras.len()
    }

    fn with_camera<T, F>(&self, room_name: &str, action: F) -> anyhow::Result<T>
    where
        F: FnOnce(&CameraStream) -> anyhow::Result<T>,
    {
        let cameras = self
            .cameras
            .read()
            .expect("Failed to acquire read lock on cameras for camera operation");

        if let Some(camera) = cameras.get(room_name) {
            action(camera)
        } else {
            Err(anyhow::anyhow!("Camera not found: {}", room_name))
        }
    }

    /// Add viewer to a specific camera stream
    pub fn add_viewer_to_camera(
        &self,
        room_name: &str,
        viewer_id: &str,
        sender: tokio::sync::mpsc::UnboundedSender<String>
    ) -> anyhow::Result<()> {
        self.with_camera(room_name, |camera| {
            camera.add_viewer(viewer_id, sender)?;
            Ok(())
        })?;

        // Grab the ConnectionMonitor Arc so the async probe can write results into it
        let monitor_opt: Option<std::sync::Arc<std::sync::Mutex<crate::tester::connection_monitor::ConnectionMonitor>>> = {
            let cameras = self.cameras.read()
                .expect("Failed to acquire read lock on cameras for probe spawn");
            cameras.get(room_name).and_then(|cam| {
                cam.viewers.lock().ok().and_then(|viewers| {
                    viewers.get(viewer_id).and_then(|v| v.monitor.clone())
                })
            })
        };

        if let Some(monitor) = monitor_opt {
            let stun      = self.config.webrtc_stun_server.clone();
            let turn      = self.config.webrtc_turn_servers.clone();
            let signaling = self.config.signaling_server_url.clone();
            let vid       = viewer_id.to_string();
            let rname     = room_name.to_string();
            tokio::spawn(async move {
                use crate::tester::network_probe::run_all_probes;
                log::debug!(" [PROBE][{}][{}] Starting per-viewer network probe...", rname, vid);
                let report = run_all_probes(&stun, &turn, &signaling).await;
                log::debug!(" [PROBE][{}][{}] Probe complete — NAT={}", rname, vid, report.nat_type);
                if let Ok(mut m) = monitor.lock() {
                    m.set_network_probe(report);
                    // If ICE is already done by the time the probe finishes, print an
                    // updated early report that includes the network section.
                    m.print_updated_network_report();
                }
            });
        }

        Ok(())
    }

    /// Remove viewer from a specific camera stream
    pub fn remove_viewer_from_camera(&self, room_name: &str, viewer_id: &str) -> anyhow::Result<()> {
        let mut cameras = self.cameras.write()
            .expect("Failed to acquire write lock on cameras for removing viewer");
        if let Some(camera) = cameras.get_mut(room_name) {
            camera.remove_viewer(viewer_id);
            log::info!(" Removed viewer {} from camera {}", viewer_id, room_name);
            
            let viewer_count = camera.get_viewer_count();
            
            // Stop ZMQ consumption if this was the last viewer
            if viewer_count == 0 && camera.is_zmq_consuming() {
                if let Err(e) = camera.stop_zmq_consumption() {
                    log::error!(" Failed to stop ZMQ consumption for {}: {}", room_name, e);
                } else {
                    log::info!(" Stopped ZMQ consumption for {} (no viewers remaining)", room_name);
                }
            }
            
            // Note: Viewer count updates now handled by WebSocket signaling server
            
            Ok(())
        } else {
            log::warn!(" Attempted to remove viewer from non-existent camera: {}", room_name);
            Ok(())
        }
    }

    /// Create WebRTC pipeline for viewer on specific camera
    pub fn create_viewer_webrtc_pipeline(&self, room_name: &str, viewer_id: &str) -> anyhow::Result<()> {
        self.with_camera(room_name, |camera| camera.create_viewer_webrtc_pipeline(viewer_id))
    }

    /// Create WebRTC pipeline for viewer via enhanced signaling server
    pub fn create_signaling_webrtc_pipeline(self: &Arc<Self>, room_name: &str, viewer_id: &str) -> anyhow::Result<()> {
        let mut cameras = self.cameras.write()
            .expect("Failed to acquire write lock on cameras for creating signaling WebRTC pipeline");

        // Compute gateway-wide total only when gateway mode is active.
        let total_gateway_viewers = if self.viewer_limit_mode == ViewerLimitMode::Gateway {
            Some(cameras.values().map(|cam| cam.get_viewer_count()).sum::<usize>())
        } else {
            None
        };

        if let Some(camera) = cameras.get_mut(room_name) {
            let current_viewer_count = camera.get_viewer_count();
            
            log::info!(" [{}] Processing viewer request: {} (current viewers: {})", 
                      room_name, viewer_id, current_viewer_count);

            match self.viewer_limit_mode {
                ViewerLimitMode::Camera => {
                    // Camera mode: only enforce per-camera cap.
                    if current_viewer_count >= self.max_viewers_per_camera {
                        log::warn!(" [{}] VIEWER LIMIT EXCEEDED! Cannot add viewer {} (current: {}, max: {})", 
                                  room_name, viewer_id, current_viewer_count, self.max_viewers_per_camera);
                        log::warn!(" [{}] Current viewers: {:?}", room_name, camera.get_viewer_ids());
                        return Err(anyhow::anyhow!(
                            "Viewer limit exceeded for camera {}. Current: {}, Max: {}", 
                            room_name, current_viewer_count, self.max_viewers_per_camera
                        ));
                    }

                    log::info!(" [{}] Camera mode limit check passed ({}/{} viewers)", 
                              room_name, current_viewer_count, self.max_viewers_per_camera);
                }
                ViewerLimitMode::Gateway => {
                    // Gateway mode: do NOT enforce per-camera cap.
                    let total_gateway_viewers = total_gateway_viewers.unwrap_or(0);
                    if total_gateway_viewers >= self.max_viewers_per_gateway {
                        log::warn!(
                            " [gateway] VIEWER LIMIT EXCEEDED! Cannot add viewer {} on camera {} (current gateway viewers: {}, max gateway viewers: {})",
                            viewer_id,
                            room_name,
                            total_gateway_viewers,
                            self.max_viewers_per_gateway
                        );
                        return Err(anyhow::anyhow!(
                            "Viewer limit exceeded for gateway. Current: {}, Max: {}",
                            total_gateway_viewers,
                            self.max_viewers_per_gateway
                        ));
                    }

                    log::info!(
                        " [gateway] Gateway mode limit check passed ({}/{} total viewers)",
                        total_gateway_viewers,
                        self.max_viewers_per_gateway
                    );
                }
            }

            // Start ZMQ consumption if this is the first viewer
            if current_viewer_count == 0 && !camera.is_zmq_consuming() {
                if let Err(e) = camera.start_zmq_consumption() {
                    log::error!(" [{}] Failed to start ZMQ consumption: {}", room_name, e);
                } else {
                    log::info!(" [{}] Started ZMQ consumption (first viewer)", room_name);
                }
            }

            // In the enhanced signaling architecture, we create the WebRTC pipeline
            // and pass the app_state so the camera can route messages through the signaling server
            let result = camera.create_signaling_viewer_pipeline(viewer_id, self.clone());
            
            if result.is_ok() {
                let new_viewer_count = camera.get_viewer_count();
                match self.viewer_limit_mode {
                    ViewerLimitMode::Camera => {
                        log::info!(" [{}] Viewer {} successfully added! Total viewers: {}/{}", 
                                  room_name, viewer_id, new_viewer_count, self.max_viewers_per_camera);
                    }
                    ViewerLimitMode::Gateway => {
                        let total_gateway_viewers = total_gateway_viewers.unwrap_or(0);
                        log::info!(
                            " [gateway] Viewer {} successfully added on {}. Total gateway viewers: {}/{}",
                            viewer_id,
                            room_name,
                            total_gateway_viewers + 1,
                            self.max_viewers_per_gateway
                        );
                    }
                }
            }
            
            result
        } else {
            log::error!(" Camera not found: {}", room_name);
            Err(anyhow::anyhow!("Camera not found: {}", room_name))
        }
    }

    /// Handle SDP answer from viewer
    pub fn handle_viewer_sdp_answer(
        &self, 
        room_name: &str, 
        viewer_id: &str, 
        sdp: &serde_json::Value
    ) -> anyhow::Result<()> {
        self.with_camera(room_name, |camera| camera.handle_viewer_sdp_answer(viewer_id, sdp))
    }

    /// Handle ICE candidate from viewer
    pub fn handle_viewer_ice_candidate(
        &self, 
        room_name: &str, 
        viewer_id: &str, 
        ice: &serde_json::Value
    ) -> anyhow::Result<()> {
        self.with_camera(room_name, |camera| camera.handle_viewer_ice_candidate(viewer_id, ice))
    }

    /// Apply viewer-side decode/render telemetry (from the headless viewer agent)
    /// to the per-viewer ConnectionMonitor.
    pub fn apply_viewer_telemetry(
        &self,
        room_name: &str,
        viewer_id: &str,
        frames_decoded: u64,
        decode_fps: f64,
        first_decoded_ms: Option<u64>,
    ) -> anyhow::Result<()> {
        self.with_camera(room_name, |camera| {
            camera.apply_viewer_telemetry(viewer_id, frames_decoded, decode_fps, first_decoded_ms)
        })
    }

    /// Get summary information for all cameras
    pub fn get_camera_summary(&self) -> Vec<CameraSummary> {
        let cameras = self.cameras.read()
            .expect("Failed to acquire read lock on cameras for get_camera_summary");
        cameras
            .values()
            .map(|camera| CameraSummary {
                camera_uuid: camera.camera_uuid.clone(),
                camera_name: camera.camera_name.clone(),
                room_name: camera.room_name.clone(),
                viewer_count: camera.get_viewer_count(),
                status: camera.status.clone(),
            })
            .collect()
    }
}

/// Camera information summary
#[derive(Debug, Clone, serde::Serialize)]
pub struct CameraSummary {
    pub camera_uuid: String,
    pub camera_name: String,
    pub room_name: String,
    pub viewer_count: usize,
    pub status: String,
}