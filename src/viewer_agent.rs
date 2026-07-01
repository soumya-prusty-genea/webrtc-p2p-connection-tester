//! Headless WebRTC viewer agent.
//!
//! This binary closes the end-to-end test loop: it connects to the same
//! Socket.IO signaling server as a *viewer*, requests a stream from the
//! gateway/SFU, negotiates a `recvonly` WebRTC session, decodes the incoming
//! video, and reports decode telemetry back to the gateway over the
//! `viewer_telemetry` signaling event. The gateway folds that telemetry into
//! its per-connection `ConnectionMonitor`, so the diagnostic report can prove
//! that video was actually displayable on the viewer side (not merely sent).
//!
//! Protocol assumptions (mirror the gateway handlers in
//! `messaging/signaling_socketio_client.rs`):
//!   - viewer -> server : `viewer_request`        {viewer_id, stream_id, session_id}
//!   - server -> viewer : `webrtc_offer` | `webrtc_message_to_viewer`
//!                         carrying an SDP offer and/or ICE candidates
//!   - viewer -> server : `webrtc_answer`          {viewer_id, data:{type, sdp}}
//!   - viewer -> server : `webrtc_ice_candidate`   {viewer_id, data:{candidate, sdpMLineIndex}}
//!   - viewer -> server : `viewer_telemetry`       {viewer_id, data:{frames_decoded, decode_fps, first_decoded_ms}}
//!
//! Configuration (environment variables):
//!   SIGNALING_SERVER_URL   signaling base URL (default http://localhost:8443)
//!   VIEWER_STREAM_ID       stream/room id to view (required)
//!   VIEWER_ID              viewer identity (default random)
//!   WEBRTC_STUN_SERVER     stun://host:port
//!   WEBRTC_TURN_SERVERS    comma-separated turn(s):host:port?transport=...
//!   VIEWER_JWT/GATEWAY_JWT optional bearer token for the signaling handshake
//!   VIEWER_TEST_DURATION_SECS  run duration before printing verdict (0 = forever)
//!   VIEWER_TELEMETRY_INTERVAL_SECS  telemetry emit cadence (default 2)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_sdp as gst_sdp;
use gstreamer_webrtc as gst_webrtc;
use log::{debug, error, info, warn};
use rust_socketio::{client::Client, ClientBuilder, Event, Payload, TransportType};
use serde_json::{json, Value};
use url::Url;

/// Shared state observed by both the GStreamer callbacks and the signaling client.
struct ViewerState {
    viewer_id: String,
    stream_id: String,
    started_at: Instant,
    frames_decoded: AtomicU64,
    first_decoded_ms: Mutex<Option<u64>>,
    webrtcbin: Mutex<Option<gst::Element>>,
    client: Mutex<Option<Client>>,
}

impl ViewerState {
    fn decode_fps(&self) -> f64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            0.0
        } else {
            self.frames_decoded.load(Ordering::Relaxed) as f64 / elapsed
        }
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn main() -> Result<()> {
    env_logger::init();
    gst::init()?;

    let signaling_url = env_or("SIGNALING_SERVER_URL", "http://localhost:8443");
    let stream_id = std::env::var("VIEWER_STREAM_ID")
        .map_err(|_| anyhow!("VIEWER_STREAM_ID is required (the stream/room id to view)"))?;
    let viewer_id = std::env::var("VIEWER_ID")
        .unwrap_or_else(|_| format!("viewer_agent_{}", rand_suffix()));
    let stun = env_or("WEBRTC_STUN_SERVER", "stun://stun.l.google.com:19302");
    let turn_servers: Vec<String> = std::env::var("WEBRTC_TURN_SERVERS")
        .ok()
        .map(|s| s.split(',').map(|u| u.trim().to_string()).filter(|u| !u.is_empty()).collect())
        .unwrap_or_default();
    let duration_secs = std::env::var("VIEWER_TEST_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30);
    let telemetry_interval = std::env::var("VIEWER_TELEMETRY_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(2);

    info!(" Viewer agent starting: viewer_id={} stream_id={}", viewer_id, stream_id);
    info!(" Signaling: {} | STUN: {} | TURN servers: {}", signaling_url, stun, turn_servers.len());

    let state = Arc::new(ViewerState {
        viewer_id: viewer_id.clone(),
        stream_id: stream_id.clone(),
        started_at: Instant::now(),
        frames_decoded: AtomicU64::new(0),
        first_decoded_ms: Mutex::new(None),
        webrtcbin: Mutex::new(None),
        client: Mutex::new(None),
    });

    // Build the recvonly pipeline and store the webrtcbin handle.
    let pipeline = build_recv_pipeline(&state, &stun, &turn_servers)?;
    pipeline.set_state(gst::State::Playing)?;
    info!(" Viewer pipeline PLAYING; waiting for offer from gateway");

    // Connect to signaling and drive negotiation.
    let _client = connect_signaling(&state, &signaling_url)?;

    // Periodically report telemetry back to the gateway.
    spawn_telemetry_loop(state.clone(), telemetry_interval);

    // Drive the GLib main loop so webrtcbin callbacks fire.
    let main_loop = gst::glib::MainLoop::new(None, false);
    if duration_secs > 0 {
        let ml = main_loop.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(duration_secs));
            ml.quit();
        });
    }
    main_loop.run();

    // Final verdict.
    let frames = state.frames_decoded.load(Ordering::Relaxed);
    let _ = pipeline.set_state(gst::State::Null);
    if frames > 0 {
        info!(
            " >>> VIEWER VERDICT: PASS <<< decoded {} frames ({:.1} fps) for stream {}",
            frames,
            state.decode_fps(),
            stream_id
        );
        Ok(())
    } else {
        error!(
            " >>> VIEWER VERDICT: FAIL <<< no frames decoded for stream {} (video not displayable)",
            stream_id
        );
        std::process::exit(2);
    }
}

fn rand_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
    format!("{:06x}", n & 0xff_ffff)
}

/// Build `webrtcbin name=recv` and attach pad-added/ICE handlers. Decoding is
/// linked dynamically once the gateway's media pad appears.
fn build_recv_pipeline(state: &Arc<ViewerState>, stun: &str, turn_servers: &[String]) -> Result<gst::Pipeline> {
    let pipeline = gst::Pipeline::new(Some("viewer-recv"));
    let webrtcbin = gst::ElementFactory::make("webrtcbin")
        .name("recv")
        .property_from_str("bundle-policy", "max-bundle")
        .build()?;
    webrtcbin.set_property("stun-server", stun);
    for turn in turn_servers {
        let _ = webrtcbin.emit_by_name::<bool>("add-turn-server", &[&turn.as_str()]);
    }
    pipeline.add(&webrtcbin)?;

    *state.webrtcbin.lock().unwrap() = Some(webrtcbin.clone());

    // Decode and count incoming media when webrtcbin exposes a src pad.
    {
        let pipeline_weak = pipeline.downgrade();
        let state = state.clone();
        webrtcbin.connect_pad_added(move |_wb, pad| {
            if pad.direction() != gst::PadDirection::Src {
                return;
            }
            let pipeline = match pipeline_weak.upgrade() {
                Some(p) => p,
                None => return,
            };
            if let Err(e) = link_decode_branch(&pipeline, pad, &state) {
                error!(" Failed to build decode branch: {}", e);
            }
        });
    }

    // Relay local ICE candidates to the gateway via signaling.
    {
        let state = state.clone();
        webrtcbin.connect("on-ice-candidate", false, move |args| {
            let mline = args[1].get::<u32>().unwrap_or(0);
            let candidate = args[2].get::<String>().unwrap_or_default();
            if let Some(client) = state.client.lock().unwrap().clone() {
                let msg = json!({
                    "viewer_id": state.viewer_id,
                    "data": { "candidate": candidate, "sdpMLineIndex": mline }
                });
                if let Err(e) = client.emit("webrtc_ice_candidate", msg) {
                    warn!(" Failed to send viewer ICE candidate: {}", e);
                }
            }
            None
        });
    }

    Ok(pipeline)
}

/// Link `webrtc src pad -> depay -> decodebin -> fakesink` and count decoded frames.
fn link_decode_branch(pipeline: &gst::Pipeline, pad: &gst::Pad, state: &Arc<ViewerState>) -> Result<()> {
    let caps = pad
        .current_caps()
        .unwrap_or_else(|| pad.query_caps(None));
    let structure = caps.structure(0).ok_or_else(|| anyhow!("empty caps"))?;
    let encoding = structure
        .get::<String>("encoding-name")
        .unwrap_or_else(|_| "H264".to_string())
        .to_uppercase();

    let depay_name = match encoding.as_str() {
        "H264" => "rtph264depay",
        "H265" => "rtph265depay",
        "VP8" => "rtpvp8depay",
        "VP9" => "rtpvp9depay",
        other => {
            warn!(" Unknown encoding '{}', defaulting to H264 depay", other);
            "rtph264depay"
        }
    };

    info!(" Incoming media encoding={}, building {} -> decodebin", encoding, depay_name);

    let depay = gst::ElementFactory::make(depay_name).build()?;
    let queue = gst::ElementFactory::make("queue").build()?;
    let decodebin = gst::ElementFactory::make("decodebin").build()?;
    pipeline.add_many(&[&depay, &queue, &decodebin])?;

    // Count frames once decodebin produces raw video.
    {
        let pipeline_weak = pipeline.downgrade();
        let state = state.clone();
        decodebin.connect_pad_added(move |_db, src_pad| {
            let pipeline = match pipeline_weak.upgrade() {
                Some(p) => p,
                None => return,
            };
            if let Err(e) = attach_counting_sink(&pipeline, src_pad, &state) {
                error!(" Failed to attach counting sink: {}", e);
            }
        });
    }

    depay.sync_state_with_parent()?;
    queue.sync_state_with_parent()?;
    decodebin.sync_state_with_parent()?;

    let depay_sink = depay.static_pad("sink").ok_or_else(|| anyhow!("depay has no sink pad"))?;
    pad.link(&depay_sink)?;
    gst::Element::link_many(&[&depay, &queue, &decodebin])?;
    Ok(())
}

fn attach_counting_sink(pipeline: &gst::Pipeline, src_pad: &gst::Pad, state: &Arc<ViewerState>) -> Result<()> {
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    let sink = gst::ElementFactory::make("fakesink")
        .property("sync", false)
        .build()?;
    pipeline.add_many(&[&convert, &sink])?;
    convert.sync_state_with_parent()?;
    sink.sync_state_with_parent()?;

    let sink_pad = sink.static_pad("sink").ok_or_else(|| anyhow!("fakesink has no sink pad"))?;
    {
        let state = state.clone();
        sink_pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, info| {
            if let Some(gst::PadProbeData::Buffer(_)) = info.data {
                let count = state.frames_decoded.fetch_add(1, Ordering::Relaxed) + 1;
                if count == 1 {
                    let ms = state.started_at.elapsed().as_millis() as u64;
                    *state.first_decoded_ms.lock().unwrap() = Some(ms);
                    info!(" First decoded frame at {}ms", ms);
                }
            }
            gst::PadProbeReturn::Ok
        });
    }

    let convert_sink = convert.static_pad("sink").ok_or_else(|| anyhow!("videoconvert has no sink pad"))?;
    src_pad.link(&convert_sink)?;
    gst::Element::link_many(&[&convert, &sink])?;
    Ok(())
}

/// Connect to the signaling server, register as a viewer, and handle SDP/ICE.
fn connect_signaling(state: &Arc<ViewerState>, signaling_url: &str) -> Result<()> {
    let (base_url, namespace) = parse_socketio_target(signaling_url)?;
    info!(" Socket.IO base={} namespace={}", base_url, namespace);

    let builder = ClientBuilder::new(base_url)
        .namespace(namespace)
        .transport_type(TransportType::Websocket)
        .reconnect(true)
        .reconnect_on_disconnect(true)
        .reconnect_delay(1000, 5000);

    let builder = match jwt_bearer() {
        Some(bearer) => {
            info!(" Injecting Authorization header for viewer handshake");
            builder.opening_header("Authorization", bearer)
        }
        None => builder,
    };

    let viewer_id = state.viewer_id.clone();
    let stream_id = state.stream_id.clone();

    let state_for_connect = state.clone();
    let state_for_offer = state.clone();
    let state_for_offer2 = state.clone();
    let state_for_ice = state.clone();

    let client = builder
        .on(Event::Connect, move |_payload, socket| {
            info!(" Connected; sending viewer_request for stream {}", stream_id);
            let req = json!({
                "viewer_id": viewer_id,
                "stream_id": stream_id,
                "session_id": format!("session_{}", rand_suffix()),
            });
            if let Err(e) = socket.emit("viewer_request", req) {
                error!(" Failed to send viewer_request: {}", e);
            }
        })
        .on("webrtc_offer", move |payload, _socket| {
            handle_signaling_payload(&state_for_offer, payload);
        })
        .on("webrtc_message_to_viewer", move |payload, _socket| {
            handle_signaling_payload(&state_for_offer2, payload);
        })
        .on("webrtc_ice_candidate", move |payload, _socket| {
            handle_signaling_payload(&state_for_ice, payload);
        })
        .on(Event::Error, |payload, _socket| {
            error!(" Socket.IO error: {:?}", payload);
        })
        .connect()?;

    *state_for_connect.client.lock().unwrap() = Some(client);
    Ok(())
}

/// Parse either an SDP offer or an ICE candidate out of a relayed payload and
/// feed it to webrtcbin. Tolerates several envelope shapes used by signaling
/// servers (`data.sdp.sdp`, `data.sdp`, top-level `sdp`, `data.ice`, ...).
fn handle_signaling_payload(state: &Arc<ViewerState>, payload: Payload) {
    let raw = match payload {
        Payload::String(s) => s,
        other => {
            debug!(" Ignoring non-string payload: {:?}", other);
            return;
        }
    };
    let value: Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(e) => {
            warn!(" Could not parse signaling payload: {}", e);
            return;
        }
    };

    // The interesting body may be under "data" or at the top level.
    let body = if value.get("data").is_some() { &value["data"] } else { &value };

    // SDP offer?
    if let Some(sdp_text) = extract_offer_sdp(body) {
        if let Err(e) = handle_offer(state, &sdp_text) {
            error!(" Failed to handle offer: {}", e);
        }
        return;
    }

    // ICE candidate?
    if let Some((candidate, mline)) = extract_ice(body) {
        if let Some(webrtcbin) = state.webrtcbin.lock().unwrap().clone() {
            webrtcbin.emit_by_name::<()>("add-ice-candidate", &[&mline, &candidate]);
            debug!(" Added remote ICE candidate (mline={})", mline);
        }
    }
}

fn extract_offer_sdp(body: &Value) -> Option<String> {
    // Shapes: {sdp:{type:"offer", sdp:"..."}} | {type:"offer", sdp:"..."}
    if let Some(sdp_obj) = body.get("sdp") {
        if sdp_obj.is_object() {
            let is_offer = sdp_obj.get("type").and_then(|t| t.as_str()) != Some("answer");
            if is_offer {
                if let Some(s) = sdp_obj.get("sdp").and_then(|s| s.as_str()) {
                    return Some(s.to_string());
                }
            }
        } else if let Some(s) = sdp_obj.as_str() {
            // {type:"offer", sdp:"..."} where sdp is a bare string
            if body.get("type").and_then(|t| t.as_str()) != Some("answer") {
                return Some(s.to_string());
            }
        }
    }
    None
}

fn extract_ice(body: &Value) -> Option<(String, u32)> {
    let ice = if body.get("ice").is_some() { &body["ice"] } else { body };
    let candidate = ice.get("candidate").and_then(|c| c.as_str())?;
    if candidate.is_empty() {
        return None;
    }
    let mline = ice.get("sdpMLineIndex").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
    Some((candidate.to_string(), mline))
}

/// Apply the remote offer, create + send an answer.
fn handle_offer(state: &Arc<ViewerState>, sdp_text: &str) -> Result<()> {
    let webrtcbin = state
        .webrtcbin
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| anyhow!("webrtcbin not ready"))?;

    info!(" Received SDP offer ({} bytes); applying remote description", sdp_text.len());

    let sdp = gst_sdp::SDPMessage::parse_buffer(sdp_text.as_bytes())
        .map_err(|e| anyhow!("failed to parse offer SDP: {:?}", e))?;
    let offer = gst_webrtc::WebRTCSessionDescription::new(gst_webrtc::WebRTCSDPType::Offer, sdp);

    let webrtcbin_for_answer = webrtcbin.clone();
    let state = state.clone();
    let on_remote_set = gst::Promise::with_change_func(move |_reply| {
        create_and_send_answer(&webrtcbin_for_answer, &state);
    });
    webrtcbin.emit_by_name::<()>("set-remote-description", &[&offer, &on_remote_set]);
    Ok(())
}

fn create_and_send_answer(webrtcbin: &gst::Element, state: &Arc<ViewerState>) {
    let webrtcbin_for_local = webrtcbin.clone();
    let state = state.clone();
    let promise = gst::Promise::with_change_func(move |reply| {
        let reply = match reply {
            Ok(Some(r)) => r,
            _ => {
                error!(" create-answer produced no reply");
                return;
            }
        };
        let answer = match reply
            .value("answer")
            .ok()
            .and_then(|v| v.get::<gst_webrtc::WebRTCSessionDescription>().ok())
        {
            Some(a) => a,
            None => {
                error!(" create-answer reply missing answer");
                return;
            }
        };

        let local_promise = gst::Promise::new();
        webrtcbin_for_local.emit_by_name::<()>("set-local-description", &[&answer, &local_promise]);

        let sdp_text = answer.sdp().as_text().unwrap_or_default();
        if let Some(client) = state.client.lock().unwrap().clone() {
            let msg = json!({
                "viewer_id": state.viewer_id,
                "data": { "type": "answer", "sdp": sdp_text }
            });
            if let Err(e) = client.emit("webrtc_answer", msg) {
                error!(" Failed to send answer: {}", e);
            } else {
                info!(" Sent SDP answer to gateway");
            }
        }
    });
    webrtcbin.emit_by_name::<()>("create-answer", &[&None::<gst::Structure>, &promise]);
}

fn spawn_telemetry_loop(state: Arc<ViewerState>, interval_secs: u64) {
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(interval_secs));
        let frames = state.frames_decoded.load(Ordering::Relaxed);
        let first = *state.first_decoded_ms.lock().unwrap();
        let client = state.client.lock().unwrap().clone();
        if let Some(client) = client {
            let msg = json!({
                "viewer_id": state.viewer_id,
                "data": {
                    "frames_decoded": frames,
                    "decode_fps": state.decode_fps(),
                    "first_decoded_ms": first,
                }
            });
            if let Err(e) = client.emit("viewer_telemetry", msg) {
                debug!(" Failed to send viewer_telemetry: {}", e);
            }
        }
    });
}

fn jwt_bearer() -> Option<String> {
    for key in ["VIEWER_JWT", "GATEWAY_JWT"] {
        if let Ok(raw) = std::env::var(key) {
            if !raw.trim().is_empty() {
                return Some(if raw.trim_start().starts_with("Bearer ") {
                    raw.trim().to_string()
                } else {
                    format!("Bearer {}", raw.trim())
                });
            }
        }
    }
    None
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
