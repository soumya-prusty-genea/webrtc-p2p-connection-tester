use anyhow::Result;
use log::{debug, error, info, warn};
use rust_socketio::{client::Client, ClientBuilder, Event, Payload, TransportType};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use url::Url;

use crate::auth::{generate_token, AuthStatus, JwtConfig};
use crate::state::AppState;

/// Socket.IO client for connecting to the enhanced signaling server
pub struct SignalingSocketIOClient {
    sfu_id: String,
    signaling_url: String,
    app_state: Arc<AppState>,
    client: Option<Client>,
    viewer_streams: Arc<Mutex<HashMap<String, String>>>, // viewer_id -> stream_id
    /// Optional JWT configuration.  When present, a signed HS256 token is
    /// generated and sent as `Authorization: Bearer <token>` in the HTTP
    /// upgrade request that establishes the Socket.IO connection.
    jwt_config: Option<JwtConfig>,
    /// Shared auth status — updated by event handlers so the main loop can
    /// observe whether the server accepted or rejected the token.
    auth_status: Arc<Mutex<AuthStatus>>,
}

impl SignalingSocketIOClient {
    pub fn new(
        sfu_id: String,
        signaling_url: String,
        app_state: Arc<AppState>,
        jwt_config: Option<JwtConfig>,
    ) -> Self {
        let initial_status = if jwt_config.is_some() {
            AuthStatus::Pending
        } else {
            AuthStatus::Disabled
        };
        Self {
            sfu_id,
            signaling_url,
            app_state,
            client: None,
            viewer_streams: Arc::new(Mutex::new(HashMap::new())),
            jwt_config,
            auth_status: Arc::new(Mutex::new(initial_status)),
        }
    }

    /// Return a snapshot of the current authentication status.
    pub fn auth_status(&self) -> AuthStatus {
        self.auth_status.lock().unwrap().clone()
    }

    /// Connect to the enhanced signaling server as an SFU client using Socket.IO
    pub fn connect_and_run(&mut self) -> Result<()> {
        info!(" Connecting to Socket.IO signaling server: {}", self.signaling_url);

        let (socket_url, namespace) = parse_socketio_target(&self.signaling_url)?;

        info!(" Socket.IO base URL: {}", socket_url);
        info!(" Socket.IO namespace: {}", namespace);

        // Create event handlers
        let sfu_id = self.sfu_id.clone();
        let app_state = self.app_state.clone();
        let viewer_streams = self.viewer_streams.clone();
        let auth_status = self.auth_status.clone();

        // Reset to Pending on every new connection attempt so stale status
        // from a previous failed attempt doesn't persist.
        {
            let mut s = auth_status.lock().unwrap();
            if *s != AuthStatus::Disabled {
                *s = AuthStatus::Pending;
            }
        }

        // Create separate clones for each closure to avoid ownership issues
        let sfu_id_for_connect = sfu_id.clone();
        let sfu_id_for_register_ack = sfu_id.clone();
        let auth_status_for_ack = auth_status.clone();
        let auth_status_for_error = auth_status.clone();
        let auth_status_for_auth_error = auth_status.clone();
        
        // --- JWT Authentication ---
        // Generate a fresh token for every new connection attempt so that
        // reconnections after expiry automatically get a valid token.
        let base_builder = {
            let builder = ClientBuilder::new(socket_url)
                .namespace(namespace)
                .transport_type(TransportType::Websocket)
                .reconnect(true)
                .reconnect_on_disconnect(true)
                .reconnect_delay(1000, 5000)
                .max_reconnect_attempts(10);

            match self.jwt_config.as_ref().map(generate_token) {
                Some(Ok(bearer)) => {
                    let mode = match self.jwt_config.as_ref().unwrap() {
                        crate::auth::JwtConfig::PreBuilt { .. }     => "pre-built (GATEWAY_JWT)",
                        crate::auth::JwtConfig::SelfSigned { .. }   => "self-signed (JWT_SECRET)",
                    };
                    info!(" JWT auth enabled [{}] — injecting Authorization header", mode);
                    builder.opening_header("Authorization", bearer)
                }
                Some(Err(e)) => {
                    warn!("  JWT generation failed, connecting without auth: {}", e);
                    builder
                }
                None => {
                    info!("  No JWT config — connecting to signaling server without authentication");
                    builder
                }
            }
        };

        // Create Socket.IO client with event handlers
        // NOTE: follows the same structure confirmed working against
        //       wss://signaling.dev-sequr.io (see utils/test_auth.rs).
        let client = base_builder
            .on("open", move |_payload, _socket| {
                info!(" Transport open");
            })
            .on(Event::Connect, move |_payload, socket| {
                let sfu_id = sfu_id_for_connect.clone();
                info!(" Socket.IO namespace connected — sending sfu_register (sfu_id={})", sfu_id);

                let register_data = json!({ "sfu_id": sfu_id });
                if let Err(e) = socket.emit("sfu_register", register_data) {
                    error!(" Failed to send SFU registration: {}", e);
                } else {
                    info!(" sfu_register sent");
                }
            })
            .on("connect_error", move |payload, _socket| {
                // connect_error fires on handshake-level failures including
                // auth rejection before the namespace is established.
                let msg = match payload {
                    Payload::String(s) => s,
                    other => format!("{:?}", other),
                };
                error!(" connect_error — signaling server refused connection: {}", msg);
            })
            .on(Event::Close, |_payload, _socket| {
                warn!(" Disconnected from Socket.IO signaling server");
            })
            .on(Event::Error, move |payload, _socket| {
                // Detect authentication rejection from the server.  Servers
                // that reject a bad/missing token typically close the transport
                // with a message containing "unauthorized" or "401".
                let msg = format!("{:?}", payload);
                let lower = msg.to_lowercase();
                if lower.contains("unauthorized")
                    || lower.contains("401")
                    || lower.contains("forbidden")
                    || lower.contains("403")
                    || lower.contains("invalid token")
                    || lower.contains("jwt")
                {
                    error!(" Authentication FAILED — signaling server rejected the token: {}", msg);
                    *auth_status_for_error.lock().unwrap() = AuthStatus::Failed(msg);
                } else {
                    error!(" Socket.IO error: {:?}", payload);
                }
            })
            .on("auth_error", move |payload, _socket| {
                // Explicit auth rejection event (server-defined).
                let reason = match payload {
                    Payload::String(s) => s,
                    other => format!("{:?}", other),
                };
                error!(" Authentication FAILED — server sent auth_error: {}", reason);
                *auth_status_for_auth_error.lock().unwrap() =
                    AuthStatus::Failed(reason);
            })
            .on("sfu_register_ack", {
                let app_state = app_state.clone();
                let sfu_id = sfu_id_for_register_ack.clone();
                move |_payload, _socket| {
                    let app_state = app_state.clone();
                    let sfu_id = sfu_id.clone();

                    // sfu_register_ack is only sent after the server validates
                    // the connection, so receiving it proves auth succeeded.
                    {
                        let mut s = auth_status_for_ack.lock().unwrap();
                        match &*s {
                            AuthStatus::Disabled => {
                                info!(" SFU registration acknowledged (no auth required)");
                            }
                            _ => {
                                info!(" Authentication VERIFIED — signaling server accepted the token");
                                *s = AuthStatus::Verified;
                            }
                        }
                    }

                    if let Err(e) = register_existing_streams(&app_state, &sfu_id) {
                        error!(" Failed to register existing streams: {}", e);
                    }
                }
            })
            .on("viewer_request", {
                let app_state = app_state.clone();
                let viewer_streams = viewer_streams.clone();
                move |payload, _socket| {
                    let app_state = app_state.clone();
                    let viewer_streams = viewer_streams.clone();

                    if let Payload::String(data) = payload {
                        if let Ok(data) = serde_json::from_str::<Value>(&data) {
                            let viewer_id = data["viewer_id"].as_str().unwrap_or("");
                            let stream_id = data["stream_id"].as_str().unwrap_or("");
                            let session_id = data["session_id"].as_str().unwrap_or("");

                            info!(" Viewer request: {} wants stream {}", viewer_id, stream_id);
                            if let Err(e) = handle_viewer_request(&app_state, &viewer_streams, viewer_id, stream_id, session_id) {
                                error!(" Failed to handle viewer request: {}", e);
                            }
                        }
                    }
                }
            })
            .on("webrtc_message_from_viewer", {
                let app_state = app_state.clone();
                let viewer_streams = viewer_streams.clone();
                move |payload, _socket| {
                    let app_state = app_state.clone();
                    let viewer_streams = viewer_streams.clone();

                    if let Payload::String(data) = payload {
                        if let Ok(data) = serde_json::from_str::<Value>(&data) {
                            let from_viewer = data["viewer_id"].as_str().unwrap_or("");
                            let webrtc_msg = &data["data"];

                            if let Err(e) = handle_webrtc_message_from_viewer(&app_state, &viewer_streams, from_viewer, webrtc_msg) {
                                error!(" Failed to handle WebRTC message from viewer: {}", e);
                            }
                        }
                    }
                }
            })
            .on("webrtc_answer", {
                let app_state = app_state.clone();
                let viewer_streams = viewer_streams.clone();
                move |payload, _socket| {
                    let app_state = app_state.clone();
                    let viewer_streams = viewer_streams.clone();

                    if let Payload::String(data) = payload {
                        if let Ok(data) = serde_json::from_str::<Value>(&data) {
                            let from_viewer = data["viewer_id"].as_str().unwrap_or("");
                            let webrtc_data = &data["data"];

                            let normalized = json!({
                                "sdp": {
                                    "type": webrtc_data["type"],
                                    "sdp": webrtc_data["sdp"]
                                }
                            });

                            if let Err(e) = handle_webrtc_message_from_viewer(&app_state, &viewer_streams, from_viewer, &normalized) {
                                error!(" Failed to handle WebRTC answer from viewer: {}", e);
                            }
                        }
                    }
                }
            })
            .on("webrtc_ice_candidate", {
                let app_state = app_state.clone();
                let viewer_streams = viewer_streams.clone();
                move |payload, _socket| {
                    let app_state = app_state.clone();
                    let viewer_streams = viewer_streams.clone();

                    if let Payload::String(data) = payload {
                        if let Ok(data) = serde_json::from_str::<Value>(&data) {
                            let from_viewer = data["viewer_id"].as_str().unwrap_or("");
                            let webrtc_data = &data["data"];

                            let normalized = json!({
                                "ice": {
                                    "candidate": webrtc_data["candidate"],
                                    "sdpMLineIndex": webrtc_data["sdpMLineIndex"]
                                }
                            });

                            if let Err(e) = handle_webrtc_message_from_viewer(&app_state, &viewer_streams, from_viewer, &normalized) {
                                error!(" Failed to handle WebRTC ICE candidate from viewer: {}", e);
                            }
                        }
                    }
                }
            })
            .on("viewer_telemetry", {
                let app_state = app_state.clone();
                let viewer_streams = viewer_streams.clone();
                move |payload, _socket| {
                    let app_state = app_state.clone();
                    let viewer_streams = viewer_streams.clone();

                    if let Payload::String(data) = payload {
                        if let Ok(data) = serde_json::from_str::<Value>(&data) {
                            let viewer_id = data["viewer_id"].as_str().unwrap_or("").to_string();
                            let telemetry = &data["data"];
                            if let Err(e) = handle_viewer_telemetry(&app_state, &viewer_streams, &viewer_id, telemetry) {
                                error!(" Failed to handle viewer telemetry from {}: {}", viewer_id, e);
                            }
                        }
                    }
                }
            })
            .on("viewer_disconnect", {
                let app_state = app_state.clone();
                let viewer_streams = viewer_streams.clone();
                move |payload, _socket| {
                    let app_state = app_state.clone();
                    let viewer_streams = viewer_streams.clone();

                    if let Payload::String(data) = payload {
                        if let Ok(data) = serde_json::from_str::<Value>(&data) {
                            let viewer_id = data["viewer_id"].as_str().unwrap_or("");
                            let stream_id = data["stream_id"].as_str().unwrap_or("");
                            let session_id = data["session_id"].as_str().unwrap_or("");

                            info!(" Viewer {} disconnected from stream {}, cleaning up pipeline", viewer_id, stream_id);
                            if let Err(e) = handle_viewer_disconnect(&app_state, &viewer_streams, viewer_id, stream_id, session_id) {
                                error!(" Failed to handle viewer disconnect: {}", e);
                            }
                        }
                    }
                }
            });

        // Connect client (this will block)
        info!(" Connecting to Socket.IO signaling server...");
        let connected_client = client.connect()?;
        
        info!(" Socket.IO client connected successfully");

        // Log initial auth state right after the TCP/WS connection is up.
        // The definitive verdict arrives asynchronously via sfu_register_ack
        // or auth_error, so we show Pending here if auth is configured.
        match &*self.auth_status.lock().unwrap() {
            AuthStatus::Disabled => info!("  Auth: disabled (no JWT config)"),
            AuthStatus::Pending  => info!(" Auth: token sent — awaiting server verification..."),
            other => info!("Auth status at connect: {:?}", other),
        }

        self.client = Some(connected_client.clone());

        // Set the client in app state so other components can use it
        self.app_state.set_socketio_signaling_client(connected_client);

        // Keep this function alive
        self.keep_connection_alive()
    }

    /// Keep the Socket.IO connection alive and handle reconnections
    fn keep_connection_alive(&self) -> Result<()> {
        let interval = Duration::from_secs(30);

        loop {
            thread::sleep(interval);

            // Log auth status so it's visible in production logs.
            match &*self.auth_status.lock().unwrap() {
                AuthStatus::Verified  => debug!(" Auth: verified"),
                AuthStatus::Disabled  => debug!("  Auth: disabled"),
                AuthStatus::Pending   => warn!(" Auth: still pending — server has not sent sfu_register_ack"),
                AuthStatus::Failed(r) => error!(" Auth: FAILED ({}). The signaling server rejected the token. \
                                                 Check JWT_SECRET, GATEWAY_UUID and CUSTOMER_UUID.", r),
            }
        }
    }

    /// Register a camera stream with the signaling server
    pub fn register_stream_with_signaling_server(&self, room_name: &str) -> Result<()> {
        if let Some(client) = &self.client {
            // Get real camera info from app state
            if let Some(camera) = self.app_state.get_camera(room_name) {
                let metadata = json!({
                    "room_name": camera.room_name,
                    "camera_name": camera.camera_name,
                    "camera_uuid": camera.camera_uuid,
                    "resolution": "1080p",
                    "fps": 30,
                    "codec": camera.codec.to_string()
                });

                let register_data = json!({
                    "sfu_id": self.sfu_id,
                    "stream_id": room_name,
                    "metadata": metadata
                });

                let client: &Client = client;
                client.emit("stream_register", register_data)?;
                info!(" Registered stream with signaling server: {} (sfu_id: {}, real_uuid: {})", 
                      room_name, self.sfu_id, camera.camera_uuid);
            } else {
                warn!(" Cannot register stream {}: camera not found in app state", room_name);
            }
        } else {
            warn!(" Socket.IO client not connected");
        }
        
        Ok(())
    }

    /// Unregister a camera stream from the signaling server
    pub fn unregister_stream_from_signaling_server(&self, room_name: &str) -> Result<()> {
        if let Some(client) = &self.client {
            // Get real camera info from app state
            if let Some(camera) = self.app_state.get_camera(room_name) {
                // Include metadata with real camera UUID for unregistration
                let metadata = json!({
                    "room_name": camera.room_name,
                    "camera_name": camera.camera_name,
                    "camera_uuid": camera.camera_uuid,
                    "resolution": "1080p",
                    "fps": 30,
                    "codec": camera.codec.to_string()
                });

                let unregister_data = json!({
                    "sfu_id": self.sfu_id,
                    "stream_id": room_name,
                    "metadata": metadata
                });

                let client: &Client = client;
                client.emit("stream_unregister", unregister_data)?;
                info!(" Unregistered stream from signaling server: {} (sfu_id: {}, real_uuid: {})", 
                      room_name, self.sfu_id, camera.camera_uuid);
            } else {
                warn!(" Cannot unregister stream {}: camera not found in app state", room_name);
                
                // Fallback: send unregister without metadata if camera not found
                let unregister_data = json!({
                    "sfu_id": self.sfu_id,
                    "stream_id": room_name
                });
                let client: &Client = client;
                client.emit("stream_unregister", unregister_data)?;
                info!(" Unregistered stream from signaling server (fallback): {} (sfu_id: {})", 
                      room_name, self.sfu_id);
            }
        } else {
            warn!(" Socket.IO client not connected");
        }
        
        Ok(())
    }

    /// Send WebRTC message to viewer via signaling server
    pub fn send_webrtc_message_to_viewer(&self, viewer_id: &str, webrtc_data: &Value) -> Result<()> {
        if let Some(client) = &self.client {
            let message_data = json!({
                "viewer_id": viewer_id,
                "data": webrtc_data
            });

            let client: &Client = client;
            client.emit("webrtc_message_to_viewer", message_data)?;
            debug!(" Sent WebRTC message to viewer {}", viewer_id);
        } else {
            warn!(" Socket.IO client not connected");
        }

        Ok(())
    }

    /// Get the Socket.IO client for external use
    pub fn get_client(&self) -> Option<Client> {
        self.client.clone()
    }
}

fn parse_socketio_target(raw_url: &str) -> Result<(String, String)> {
    let url = Url::parse(raw_url)?;

    let mut base_url = format!("{}://{}", url.scheme(), url.host_str().unwrap_or("localhost"));
    if let Some(port) = url.port() {
        base_url = format!("{}:{}", base_url, port);
    }

    let path = url.path().trim();
    let namespace = if path.is_empty() || path == "/" {
        "/".to_string()
    } else {
        path.to_string()
    };

    Ok((base_url, namespace))
}

/////////////// Helper Functions ///////////////

/// Register all existing camera streams with the signaling server
fn register_existing_streams(app_state: &Arc<AppState>, sfu_id: &str) -> Result<()> {
    let camera_list = app_state.list_cameras();
    
    for room_name in camera_list {
        if let Some(camera) = app_state.get_camera(&room_name) {
            let metadata = json!({
                "room_name": camera.room_name,
                "camera_name": camera.camera_name,
                "camera_uuid": camera.camera_uuid,
                "resolution": "1080p",
                "fps": 30,
                "codec": camera.codec.to_string()
            });

            let register_data = json!({
                "sfu_id": sfu_id,
                "stream_id": room_name,
                "metadata": metadata
            });

            if let Some(client) = app_state.get_socketio_signaling_client() {
                if let Err(e) = client.emit("stream_register", register_data) {
                    error!(" Failed to register stream {}: {}", room_name, e);
                } else {
                    info!(" Registered existing stream: {} (uuid: {})", room_name, camera.camera_uuid);
                }
            }
        }
    }
    
    Ok(())
}

/// Handle viewer request from signaling server
fn handle_viewer_request(
    app_state: &Arc<AppState>, 
    viewer_streams: &Arc<Mutex<HashMap<String, String>>>,
    viewer_id: &str, 
    stream_id: &str, 
    session_id: &str
) -> Result<()> {
    info!(
        " Processing viewer request: viewer_id={} stream_id={} session_id={}",
        viewer_id,
        stream_id,
        session_id
    );

    // Check if stream exists
    if !app_state.has_camera(stream_id) {
        error!(" Requested stream not found: {}", stream_id);
        return Ok(());
    }

    // Track viewer-to-stream mapping
    {
        let mut streams = viewer_streams.lock().unwrap();
        streams.insert(viewer_id.to_string(), stream_id.to_string());
    }

    // Create WebRTC pipeline for this viewer via signaling server architecture
    match app_state.create_signaling_webrtc_pipeline(stream_id, viewer_id) {
        Ok(_) => {
            info!(
                " [{}] Created WebRTC pipeline for viewer {} (session_id={})",
                stream_id,
                viewer_id,
                session_id
            );
            info!(
                " [{}] ICE connection state tracking enabled for viewer {} (session_id={})",
                stream_id,
                viewer_id,
                session_id
            );
            
            // WebRTC offer will be sent automatically by the camera manager
            // through the signaling routing mechanism we set up
        }
        Err(e) => {
            let error_msg = e.to_string();
            
            // Check if this is a viewer limit error
            if error_msg.contains("Viewer limit exceeded") {
                error!(
                    " [{}] VIEWER LIMIT EXCEEDED for viewer {} (session_id={}): {}",
                    stream_id,
                    viewer_id,
                    session_id,
                    e
                );
                warn!(" [{}] Cannot accept more viewers - limit reached", stream_id);
            } else {
                error!(
                    " [{}] Failed to create WebRTC pipeline for viewer {} (session_id={}): {}",
                    stream_id,
                    viewer_id,
                    session_id,
                    e
                );
            }
            
            // Remove mapping on error
            {
                let mut streams = viewer_streams.lock().unwrap();
                streams.remove(viewer_id);
            }
            
            // Send appropriate error message to signaling server
            let user_friendly_error = if error_msg.contains("Viewer limit exceeded") {
                "Camera has reached maximum viewer capacity. Please try again later.".to_string()
            } else {
                format!("Failed to create WebRTC connection: {}", e)
            };
            
            send_error_to_viewer(app_state, viewer_id, &user_friendly_error)?;
        }
    }

    Ok(())
}

/// Send error message to viewer via signaling server
fn send_error_to_viewer(app_state: &Arc<AppState>, viewer_id: &str, error_msg: &str) -> Result<()> {
    if let Some(client) = app_state.get_socketio_signaling_client() {
        let error_data = json!({
            "viewer_id": viewer_id,
            "data": {
                "error": error_msg
            }
        });

        let client: Client = client;
        client.emit("webrtc_message_to_viewer", error_data)?;
        warn!(" Sent error to viewer {}: {}", viewer_id, error_msg);
    }

    Ok(())
}

/// Handle WebRTC signaling messages from viewers
fn handle_webrtc_message_from_viewer(
    app_state: &Arc<AppState>, 
    viewer_streams: &Arc<Mutex<HashMap<String, String>>>,
    from_viewer: &str, 
    webrtc_msg: &Value
) -> Result<()> {
    debug!("WebRTC message from viewer {}: {:?}", from_viewer, webrtc_msg);

    // Get the stream_id for this viewer
    let stream_id = {
        let streams = viewer_streams.lock().unwrap();
        streams.get(from_viewer).cloned()
    };

    let stream_id = match stream_id {
        Some(id) => id,
        None => {
            error!(" No stream mapping found for viewer {}", from_viewer);
            return Ok(());
        }
    };

    if let Some(sdp) = webrtc_msg.get("sdp") {
        if sdp["type"] == "answer" {
            info!("Received SDP answer from viewer {} for stream {}", from_viewer, stream_id);
            
            // Handle SDP answer - in real implementation, this would be passed to GStreamer WebRTC
            if let Err(e) = app_state.handle_viewer_sdp_answer(&stream_id, from_viewer, sdp) {
                error!(" Failed to handle SDP answer from viewer {}: {}", from_viewer, e);
            }

            // Send ICE candidates back to viewer
            send_ice_candidates_to_viewer(app_state, from_viewer)?;
        }
    } else if let Some(ice) = webrtc_msg.get("ice") {
        debug!("Received ICE candidate from viewer {} for stream {}", from_viewer, stream_id);

        // Handle ICE candidate - pass the already-extracted `ice` value, no unwrap needed.
        if let Err(e) = app_state.handle_viewer_ice_candidate(&stream_id, from_viewer, ice) {
            error!(" Failed to handle ICE candidate from viewer {}: {}", from_viewer, e);
        }
    }

    Ok(())
}

/// Handle viewer disconnect notification from signaling server
fn handle_viewer_disconnect(
    app_state: &Arc<AppState>, 
    viewer_streams: &Arc<Mutex<HashMap<String, String>>>,
    viewer_id: &str, 
    stream_id: &str, 
    _session_id: &str
) -> Result<()> {
    info!("Cleaning up WebRTC pipeline for disconnected viewer {} on stream {}", viewer_id, stream_id);
    
    // Remove viewer from stream mapping
    {
        let mut streams = viewer_streams.lock().unwrap();
        streams.remove(viewer_id);
    }
    
    // Clean up WebRTC pipeline in camera manager
    if let Err(e) = app_state.remove_viewer_from_camera(stream_id, viewer_id) {
        error!(" Failed to remove viewer {} from camera {}: {}", viewer_id, stream_id, e);
    } else {
        info!(" Successfully cleaned up pipeline for viewer {} on stream {}", viewer_id, stream_id);
    }
    
    Ok(())
}

/// Handle viewer-side decode/render telemetry reported by the headless viewer agent.
fn handle_viewer_telemetry(
    app_state: &Arc<AppState>,
    viewer_streams: &Arc<Mutex<HashMap<String, String>>>,
    viewer_id: &str,
    telemetry: &Value,
) -> Result<()> {
    let stream_id = {
        let streams = viewer_streams.lock().unwrap();
        streams.get(viewer_id).cloned()
    };

    let stream_id = match stream_id {
        Some(id) => id,
        None => {
            debug!(" No stream mapping for viewer telemetry from {}", viewer_id);
            return Ok(());
        }
    };

    let frames_decoded = telemetry["frames_decoded"].as_u64().unwrap_or(0);
    let decode_fps = telemetry["decode_fps"].as_f64().unwrap_or(0.0);
    let first_decoded_ms = telemetry["first_decoded_ms"].as_u64();

    debug!(
        " Viewer telemetry from {} (stream {}): frames_decoded={} fps={:.1} first_decoded_ms={:?}",
        viewer_id, stream_id, frames_decoded, decode_fps, first_decoded_ms
    );

    app_state.apply_viewer_telemetry(&stream_id, viewer_id, frames_decoded, decode_fps, first_decoded_ms)
}

/// Send ICE candidates to viewer via signaling server
fn send_ice_candidates_to_viewer(app_state: &Arc<AppState>, viewer_id: &str) -> Result<()> {
    if let Some(client) = app_state.get_socketio_signaling_client() {
        // In a real implementation, this would get actual ICE candidates from GStreamer WebRTC
        // For now, we'll send a mock ICE candidate
        let mock_ice = json!({
            "candidate": format!("candidate:1 1 UDP 2130706431 192.168.1.100 54400 typ host generation 0 ufrag {} network-cost 999", 
                               rand::random::<u32>()),
            "sdpMLineIndex": 0
        });

        let ice_data = json!({
            "viewer_id": viewer_id,
            "data": {
                "ice": mock_ice
            }
        });

        let client: Client = client;
        client.emit("webrtc_message_to_viewer", ice_data)?;
        debug!(" Sent ICE candidate to viewer {}", viewer_id);
    }

    Ok(())
}