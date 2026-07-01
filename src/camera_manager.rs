use anyhow::{anyhow, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gst_app::{AppSink, AppSinkCallbacks};
use gstreamer_webrtc as gst_webrtc;
use gstreamer_sdp as gst_sdp;
use log::{debug, error, info, warn};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::events::{CameraInfo, VideoCodec};
use crate::metrics;
use crate::tester::connection_monitor::ConnectionMonitor;
use crate::tester::report::{CandidatePairAttemptTelemetry, DtlsFailureReason, IceCandidateType};

#[derive(Clone, Copy, Debug)]
enum TimestampSource {
    System,
    Zmq,
}

impl TimestampSource {
    fn from_env() -> Self {
        match std::env::var("TIMESTAMP_SOURCE")
            .unwrap_or_else(|_| "system".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "zmq" => Self::Zmq,
            "system" => Self::System,
            other => {
                warn!(
                    "Invalid TIMESTAMP_SOURCE='{}'; defaulting to 'system'",
                    other
                );
                Self::System
            }
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Zmq => "zmq",
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}


fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy)]
struct RecoveryConfig {
    watchdog_check_interval_secs: u64,
    watchdog_stalled_windows: u64,
    watchdog_min_input_fps: f64,
    restart_cooldown_secs: u64,
    dtls_warning_window_secs: u64,
    dtls_warning_threshold: u64,
}

impl RecoveryConfig {
    fn from_env() -> Self {
        Self {
            watchdog_check_interval_secs: env_u64("RECOVERY_WATCHDOG_INTERVAL_SECS", 5),
            watchdog_stalled_windows: env_u64("RECOVERY_WATCHDOG_STALLED_WINDOWS", 3),
            watchdog_min_input_fps: std::env::var("RECOVERY_MIN_INPUT_FPS")
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or(1.0),
            restart_cooldown_secs: env_u64("RECOVERY_RESTART_COOLDOWN_SECS", 20),
            dtls_warning_window_secs: env_u64("RECOVERY_DTLS_WINDOW_SECS", 20),
            dtls_warning_threshold: env_u64("RECOVERY_DTLS_WARNING_THRESHOLD", 3),
        }
    }
}

#[derive(Debug)]
struct DtlsWarningTracker {
    window_started_at: Instant,
    warnings_in_window: u64,
}

impl DtlsWarningTracker {
    fn new() -> Self {
        Self {
            window_started_at: Instant::now(),
            warnings_in_window: 0,
        }
    }

    fn register_warning(&mut self, config: &RecoveryConfig) -> u64 {
        if self.window_started_at.elapsed().as_secs() >= config.dtls_warning_window_secs {
            self.window_started_at = Instant::now();
            self.warnings_in_window = 0;
        }

        self.warnings_in_window = self.warnings_in_window.saturating_add(1);
        self.warnings_in_window
    }

    fn reset(&mut self) {
        self.window_started_at = Instant::now();
        self.warnings_in_window = 0;
    }
}

fn warning_matches_dtls_runtime_pattern(warning: &str, debug: Option<&str>) -> bool {
    let warning_lower = warning.to_ascii_lowercase();
    let debug_lower = debug.unwrap_or_default().to_ascii_lowercase();

    warning_lower.contains("dtls")
        || warning_lower.contains("bio_buffer")
        || warning_lower.contains("gstdtlsconnection")
        || debug_lower.contains("gstdtlsconnection")
        || debug_lower.contains("bio_buffer")
        || debug_lower.contains("dtls")
}

fn parse_sdp_fingerprint(sdp_text: &str) -> Option<String> {
    for line in sdp_text.lines() {
        if let Some(value) = line.strip_prefix("a=fingerprint:") {
            return Some(value.trim().to_string());
        }
    }
    None
}

fn parse_candidate_type(value: &str) -> IceCandidateType {
    match value.to_ascii_lowercase().as_str() {
        "host" => IceCandidateType::Host,
        "srflx" | "serverreflexive" | "server-reflexive" => IceCandidateType::ServerReflexive,
        "relay" => IceCandidateType::Relay,
        _ => IceCandidateType::Unknown,
    }
}

// ─── Structured webrtcbin get-stats access ───────────────────────────────────
//
// webrtcbin's `get-stats` returns a GstStructure whose every field value is
// itself a GstStructure (one per RTCStats object), keyed by the stat id.
// We read fields programmatically with typed getters instead of scraping the
// Debug string, which is fragile across GStreamer versions.

fn stat_u64(s: &gst::StructureRef, key: &str) -> Option<u64> {
    if let Ok(v) = s.get::<u64>(key) {
        return Some(v);
    }
    if let Ok(v) = s.get::<i64>(key) {
        return Some(v.max(0) as u64);
    }
    if let Ok(v) = s.get::<u32>(key) {
        return Some(v as u64);
    }
    if let Ok(v) = s.get::<i32>(key) {
        return Some(v.max(0) as u64);
    }
    None
}

fn stat_f64(s: &gst::StructureRef, key: &str) -> Option<f64> {
    if let Ok(v) = s.get::<f64>(key) {
        return Some(v);
    }
    if let Ok(v) = s.get::<f32>(key) {
        return Some(v as f64);
    }
    None
}

fn stat_bool(s: &gst::StructureRef, key: &str) -> Option<bool> {
    s.get::<bool>(key).ok()
}

fn stat_string(s: &gst::StructureRef, key: &str) -> Option<String> {
    s.get::<String>(key).ok()
}

/// Look up the RTCStats sub-structure whose stat id equals `id`.
fn stat_block(stats: &gst::StructureRef, id: &str) -> Option<gst::Structure> {
    stats.get::<gst::Structure>(id).ok()
}

/// Resolve the candidate type for a candidate-id by reading its sub-structure.
fn candidate_type_for_id(stats: &gst::StructureRef, id: Option<&str>) -> IceCandidateType {
    id.and_then(|id| stat_block(stats, id))
        .and_then(|b| stat_string(&b, "candidate-type"))
        .map(|s| parse_candidate_type(&s))
        .unwrap_or(IceCandidateType::Unknown)
}

/// Aggregated egress proof derived from outbound-rtp + remote-inbound-rtp stats.
#[derive(Default, Clone, Copy)]
struct EgressSample {
    packets_sent: u64,
    remote_packets_received: u64,
    fraction_lost: Option<f64>,
    nack_count: u64,
    pli_count: u64,
    fir_count: u64,
    remote_feedback_seen: bool,
}

type SelectedPairTuple = (
    Option<String>,
    IceCandidateType,
    IceCandidateType,
    Option<String>,
    Option<String>,
    Option<f64>,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
    u64,
);

/// True when the sub-structure looks like an RTCIceCandidatePair stat.
fn is_candidate_pair(sub: &gst::StructureRef) -> bool {
    stat_string(sub, "local-candidate-id").is_some()
        && stat_string(sub, "remote-candidate-id").is_some()
}

fn parse_selected_pair_from_stats(stats: &gst::StructureRef) -> Option<SelectedPairTuple> {
    // The selected pair id is published on the transport stat.
    let mut selected_pair_id: Option<String> = None;
    for field in stats.fields() {
        if let Ok(sub) = stats.get::<gst::Structure>(field.as_str()) {
            if let Some(id) = stat_string(&sub, "selected-candidate-pair-id") {
                selected_pair_id = Some(id);
                break;
            }
        }
    }

    // Fallback: pick a nominated candidate-pair directly.
    if selected_pair_id.is_none() {
        for field in stats.fields() {
            if let Ok(sub) = stats.get::<gst::Structure>(field.as_str()) {
                if is_candidate_pair(&sub) && stat_bool(&sub, "nominated").unwrap_or(false) {
                    selected_pair_id = stat_string(&sub, "id")
                        .or_else(|| Some(field.as_str().to_string()));
                    break;
                }
            }
        }
    }

    let pair_id = selected_pair_id?;
    let pair_block = stat_block(stats, &pair_id)?;

    let local_id = stat_string(&pair_block, "local-candidate-id");
    let remote_id = stat_string(&pair_block, "remote-candidate-id");
    let local_type = candidate_type_for_id(stats, local_id.as_deref());
    let remote_type = candidate_type_for_id(stats, remote_id.as_deref());

    let transport = stat_string(&pair_block, "protocol")
        .or_else(|| stat_string(&pair_block, "transport"));
    let nomination_role = stat_bool(&pair_block, "nominated").map(|b| b.to_string());
    let current_rtt_ms = stat_f64(&pair_block, "current-round-trip-time").map(|s| s * 1000.0);
    let bytes_sent = stat_u64(&pair_block, "bytes-sent").unwrap_or(0);
    let bytes_received = stat_u64(&pair_block, "bytes-received").unwrap_or(0);
    let packets_sent = stat_u64(&pair_block, "packets-sent").unwrap_or(0);
    let packets_received = stat_u64(&pair_block, "packets-received").unwrap_or(0);
    let consent_requests = stat_u64(&pair_block, "requests-received").unwrap_or(0);
    let consent_failures = stat_u64(&pair_block, "responses-sent").unwrap_or(0);
    let retransmission_indicators = stat_u64(&pair_block, "retransmissions-received").unwrap_or(0)
        + stat_u64(&pair_block, "retransmissions-sent").unwrap_or(0);

    Some((
        Some(pair_id),
        local_type,
        remote_type,
        transport,
        nomination_role,
        current_rtt_ms,
        bytes_sent,
        bytes_received,
        packets_sent,
        packets_received,
        consent_requests,
        consent_failures,
        retransmission_indicators,
    ))
}

/// Media quality from inbound/outbound RTP stats (loss %, jitter, packet count).
fn parse_media_quality_from_stats(stats: &gst::StructureRef) -> (Option<f64>, Option<f64>, Option<u64>) {
    let mut packets_sent: Option<u64> = None;
    let mut packets_received: Option<u64> = None;
    let mut packets_lost: Option<u64> = None;
    let mut jitter_ms: Option<f64> = None;

    for field in stats.fields() {
        if let Ok(sub) = stats.get::<gst::Structure>(field.as_str()) {
            if let Some(v) = stat_u64(&sub, "packets-sent") {
                packets_sent = Some(packets_sent.unwrap_or(0) + v);
            }
            if let Some(v) = stat_u64(&sub, "packets-received") {
                packets_received = Some(packets_received.unwrap_or(0) + v);
            }
            if let Some(v) = stat_u64(&sub, "packets-lost") {
                packets_lost = Some(packets_lost.unwrap_or(0) + v);
            }
            if jitter_ms.is_none() {
                if let Some(v) = stat_f64(&sub, "jitter") {
                    jitter_ms = Some(v * 1000.0);
                }
            }
        }
    }

    let packet_loss_percent = match (packets_sent.or(packets_received), packets_lost) {
        (Some(total), Some(lost)) if total.saturating_add(lost) > 0 => {
            Some((lost as f64 * 100.0) / (total.saturating_add(lost) as f64))
        }
        _ => None,
    };

    (packet_loss_percent, jitter_ms, packets_sent.or(packets_received))
}

/// Derive sender egress proof: how many packets we sent and what the remote
/// peer reported back via RTCP receiver reports (remote-inbound-rtp).
fn parse_egress_sample(stats: &gst::StructureRef) -> EgressSample {
    let mut sample = EgressSample::default();
    let mut remote_packets_lost: u64 = 0;

    for field in stats.fields() {
        let sub = match stats.get::<gst::Structure>(field.as_str()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Outbound RTP: packets we have handed to the network + feedback counters.
        if let Some(sent) = stat_u64(&sub, "packets-sent") {
            sample.packets_sent = sample.packets_sent.saturating_add(sent);
            sample.nack_count = sample
                .nack_count
                .saturating_add(stat_u64(&sub, "nack-count").unwrap_or(0));
            sample.pli_count = sample
                .pli_count
                .saturating_add(stat_u64(&sub, "pli-count").unwrap_or(0));
            sample.fir_count = sample
                .fir_count
                .saturating_add(stat_u64(&sub, "fir-count").unwrap_or(0));
        }

        // Remote-inbound RTP: the remote peer's RTCP report about our stream.
        // Its presence (round-trip-time / fraction-lost) proves the remote is
        // receiving and acknowledging media.
        let has_rtt = stat_f64(&sub, "round-trip-time").is_some();
        let frac = stat_f64(&sub, "fraction-lost");
        if has_rtt || frac.is_some() {
            // Avoid mis-classifying our own outbound stat (which has packets-sent).
            if stat_u64(&sub, "packets-sent").is_none() {
                sample.remote_feedback_seen = true;
                if let Some(f) = frac {
                    sample.fraction_lost = Some(f);
                }
                remote_packets_lost =
                    remote_packets_lost.saturating_add(stat_u64(&sub, "packets-lost").unwrap_or(0));
            }
        }
    }

    // Estimate packets the remote actually received: sent minus remote-reported loss.
    // Only meaningful once we have seen remote feedback at least once.
    if sample.remote_feedback_seen {
        sample.remote_packets_received = sample.packets_sent.saturating_sub(remote_packets_lost);
    }

    sample
}

/// Bounded, cooldown-gated ICE restart for a single viewer. Triggered when the
/// viewer's ICE connection goes `Disconnected` (e.g. the remote peer's Wi-Fi /
/// cellular network changed). Creates a fresh offer with `ice-restart=true` and
/// re-sends it through the viewer's signaling channel.
fn maybe_ice_restart(
    webrtcbin: gst::Element,
    viewers: Arc<Mutex<HashMap<String, ViewerPeer>>>,
    viewer_id: String,
    room_name: String,
) {
    let max_attempts = env_u64("ICE_RESTART_MAX_ATTEMPTS", 3) as u32;
    let cooldown_secs = env_u64("ICE_RESTART_COOLDOWN_SECS", 5);
    let now = now_epoch_secs();

    let proceed = {
        let mut guard = match viewers.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        match guard.get_mut(&viewer_id) {
            Some(peer) => {
                if peer.ice_restart_attempts >= max_attempts {
                    warn!(
                        " [{}] ICE restart budget exhausted for viewer {} ({} attempts); leaving recovery to pipeline watchdog",
                        room_name, viewer_id, peer.ice_restart_attempts
                    );
                    false
                } else if now.saturating_sub(peer.last_ice_restart_epoch) < cooldown_secs {
                    debug!(
                        " [{}] ICE restart for viewer {} suppressed by cooldown ({}s)",
                        room_name, viewer_id, cooldown_secs
                    );
                    false
                } else {
                    peer.ice_restart_attempts += 1;
                    peer.last_ice_restart_epoch = now;
                    info!(
                        " [{}] Triggering ICE restart for viewer {} (attempt {}/{})",
                        room_name, viewer_id, peer.ice_restart_attempts, max_attempts
                    );
                    true
                }
            }
            None => false,
        }
    };

    if !proceed {
        return;
    }

    let webrtcbin_for_promise = webrtcbin.clone();
    let viewers_for_promise = viewers.clone();
    let viewer_id_for_promise = viewer_id.clone();
    let room_for_promise = room_name.clone();
    let promise = gst::Promise::with_change_func(move |reply| {
        if let Ok(Some(reply)) = reply {
            if let Ok(offer_val) = reply.value("offer") {
                if let Ok(offer) = offer_val.get::<gst_webrtc::WebRTCSessionDescription>() {
                    let local_promise = gst::Promise::new();
                    webrtcbin_for_promise
                        .emit_by_name::<()>("set-local-description", &[&offer, &local_promise]);
                    let sdp_str = offer.sdp().as_text().unwrap_or_default();
                    let msg = json!({ "sdp": { "type": "offer", "sdp": sdp_str } });
                    if let Ok(guard) = viewers_for_promise.lock() {
                        if let Some(peer) = guard.get(&viewer_id_for_promise) {
                            let _ = peer.sender.send(msg.to_string());
                        }
                    }
                    info!(
                        " [{}] ICE-restart offer sent to viewer {}",
                        room_for_promise, viewer_id_for_promise
                    );
                }
            }
        } else {
            error!(
                " [{}] ICE-restart offer creation failed for viewer {}",
                room_for_promise, viewer_id_for_promise
            );
        }
    });

    let options = gst::Structure::builder("offer-options")
        .field("ice-restart", true)
        .build();
    webrtcbin.emit_by_name::<()>("create-offer", &[&Some(options), &promise]);
}

fn parse_candidate_pair_attempts_from_stats(stats: &gst::StructureRef) -> Vec<CandidatePairAttemptTelemetry> {
    let mut attempts = Vec::new();

    for field in stats.fields() {
        let sub = match stats.get::<gst::Structure>(field.as_str()) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if !is_candidate_pair(&sub) {
            continue;
        }

        let pair_id = stat_string(&sub, "id").unwrap_or_else(|| field.as_str().to_string());
        let local_id = stat_string(&sub, "local-candidate-id");
        let remote_id = stat_string(&sub, "remote-candidate-id");

        attempts.push(CandidatePairAttemptTelemetry {
            pair_id,
            state: stat_string(&sub, "state"),
            local_candidate_type: candidate_type_for_id(stats, local_id.as_deref()),
            remote_candidate_type: candidate_type_for_id(stats, remote_id.as_deref()),
            nominated: stat_bool(&sub, "nominated"),
            writable: stat_bool(&sub, "writable"),
            readable: stat_bool(&sub, "readable"),
            bytes_sent: stat_u64(&sub, "bytes-sent").unwrap_or(0),
            bytes_received: stat_u64(&sub, "bytes-received").unwrap_or(0),
        });
    }

    attempts
}

/// Single shared stats poller for an entire camera stream. Iterates every
/// active viewer once per second and issues `get-stats` on each viewer's
/// webrtcbin, instead of spawning one detached thread per viewer. Exits when
/// the stream stops (`running` cleared) or there are no viewers left after the
/// stream is no longer running.
fn start_stats_poller(
    viewers: Arc<Mutex<HashMap<String, ViewerPeer>>>,
    room_name: String,
    running: Arc<AtomicBool>,
    started_flag: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(1));

            if !running.load(Ordering::SeqCst) {
                break;
            }

            // Snapshot (viewer_id, webrtcbin, monitor) so we don't hold the lock
            // across the async get-stats promises.
            let targets: Vec<(String, gst::Element, Arc<Mutex<ConnectionMonitor>>)> = {
                let lock = match viewers.lock() {
                    Ok(v) => v,
                    Err(_) => break,
                };
                lock.iter()
                    .filter_map(|(id, peer)| {
                        match (peer.webrtcbin.clone(), peer.monitor.clone()) {
                            (Some(wb), Some(m)) => Some((id.clone(), wb, m)),
                            _ => None,
                        }
                    })
                    .collect()
            };

            for (viewer_id, webrtcbin, monitor) in targets {
                poll_viewer_stats_once(&webrtcbin, monitor, &viewer_id, &room_name);
            }
        }
        // Allow a future viewer to restart the poller.
        started_flag.store(false, Ordering::SeqCst);
    });
}

/// Issue a single `get-stats` on one viewer's webrtcbin and fold the parsed
/// telemetry into its ConnectionMonitor.
fn poll_viewer_stats_once(
    webrtcbin: &gst::Element,
    monitor: Arc<Mutex<ConnectionMonitor>>,
    viewer_id: &str,
    room_name: &str,
) {
    {
            let viewer_id_for_promise = viewer_id.to_string();
            let room_for_promise = room_name.to_string();
            let promise = gst::Promise::with_change_func(move |reply| {
                if let Ok(Some(stats)) = reply {
                    let (packet_loss_percent, jitter_ms, packets_observed) =
                        parse_media_quality_from_stats(stats);
                    let candidate_pair_attempts =
                        parse_candidate_pair_attempts_from_stats(stats);
                    let egress = parse_egress_sample(stats);
                    if let Ok(mut m) = monitor.lock() {
                        m.on_media_transport_stats(packet_loss_percent, jitter_ms, packets_observed);
                        m.on_candidate_pair_attempts(candidate_pair_attempts);
                        m.on_egress_stats(
                            egress.packets_sent,
                            egress.remote_packets_received,
                            egress.fraction_lost,
                            egress.nack_count,
                            egress.pli_count,
                            egress.fir_count,
                        );
                    }
                    if let Some((
                        selected_pair_id,
                        local_type,
                        remote_type,
                        transport,
                        nomination_role,
                        current_rtt_ms,
                        bytes_sent,
                        bytes_received,
                        packets_sent,
                        packets_received,
                        consent_requests,
                        consent_failures,
                        retransmission_indicators,
                    )) = parse_selected_pair_from_stats(stats)
                    {
                        if let Ok(mut m) = monitor.lock() {
                            m.on_candidate_pair_stats(
                                selected_pair_id,
                                local_type,
                                remote_type,
                                transport,
                                nomination_role,
                                current_rtt_ms,
                                bytes_sent,
                                bytes_received,
                                packets_sent,
                                packets_received,
                                consent_requests,
                                consent_failures,
                                retransmission_indicators,
                            );
                        }
                    } else {
                        debug!(
                            " [{}] get-stats did not include selected candidate pair yet for {}",
                            room_for_promise,
                            viewer_id_for_promise
                        );
                    }
                }
            });

            webrtcbin.emit_by_name::<()>("get-stats", &[&None::<gst::Pad>, &promise]);
    }
}

/// Viewer peer information for WebRTC connections
#[derive(Clone)]
pub struct ViewerPeer {
    pub id: String,
    pub webrtcbin: Option<gst::Element>,
    pub tee_pad: Option<gst::Pad>,
    pub sender: tokio::sync::mpsc::UnboundedSender<String>,
    /// Per-connection diagnostic monitor
    pub monitor: Option<Arc<Mutex<ConnectionMonitor>>>,
    /// Number of ICE restarts attempted for this viewer (bounded recovery).
    pub ice_restart_attempts: u32,
    /// Epoch seconds of the last ICE restart attempt (cooldown gate).
    pub last_ice_restart_epoch: u64,
}

/// Individual camera stream with its own GStreamer pipeline
#[derive(Clone)]
pub struct CameraStream {
    pub camera_uuid: String,
    pub camera_name: String,
    pub room_name: String,
    pub zmq_endpoint: String,
    pub codec: VideoCodec,
    pub codec_detected_from_stream: bool, // Track if codec was detected from actual stream data
    pub pipeline: Option<gst::Pipeline>,
    pub appsrc: Option<gst::Element>,
    pub tee: Option<gst::Element>,
    pub viewers: Arc<Mutex<HashMap<String, ViewerPeer>>>,
    pub status: String,
    pub running: Arc<AtomicBool>,
    pub zmq_context: Option<zmq::Context>,
    pub zmq_consuming: Arc<AtomicBool>, // Track if ZMQ consumption is active
    /// Guards the single shared stats poller so we only spawn it once per stream.
    pub stats_poller_started: Arc<AtomicBool>,
    pub webrtc_stun_server: String,
    pub webrtc_turn_servers: Vec<String>,
    pub timestamp_source: TimestampSource,
}

impl CameraStream {
    fn is_test_source_endpoint(endpoint: &str) -> bool {
        endpoint.starts_with("test://")
    }

    fn test_clip_path() -> PathBuf {
        std::env::var("TEST_CLIP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/app/assets/test_clip.h264"))
    }

    /// Validate CameraStream configuration
    fn validate_config(
        camera_uuid: &str,
        camera_name: &str,
        room_name: &str,
        zmq_endpoint: &str,
        webrtc_stun_server: &str,
    ) -> Result<()> {
        // Validate non-empty fields
        if camera_uuid.trim().is_empty() {
            return Err(anyhow!("camera_uuid cannot be empty"));
        }
        if camera_name.trim().is_empty() {
            return Err(anyhow!("camera_name cannot be empty"));
        }
        if room_name.trim().is_empty() {
            return Err(anyhow!("room_name cannot be empty"));
        }
        if zmq_endpoint.trim().is_empty() {
            return Err(anyhow!("zmq_endpoint cannot be empty"));
        }
        if webrtc_stun_server.trim().is_empty() {
            return Err(anyhow!("webrtc_stun_server cannot be empty"));
        }
        
        // Validate length limits
        if camera_uuid.len() > 256 {
            return Err(anyhow!("camera_uuid too long (max 256 characters)"));
        }
        if camera_name.len() > 256 {
            return Err(anyhow!("camera_name too long (max 256 characters)"));
        }
        if room_name.len() > 256 {
            return Err(anyhow!("room_name too long (max 256 characters)"));
        }
        
        // Validate ZMQ endpoint format
        if !zmq_endpoint.starts_with("ipc://") 
            && !zmq_endpoint.starts_with("tcp://") 
            && !zmq_endpoint.starts_with("inproc://")
            && !Self::is_test_source_endpoint(zmq_endpoint)
        {
            return Err(anyhow!(
                "zmq_endpoint must start with ipc://, tcp://, inproc://, or test://"
            ));
        }
        
        // Validate STUN server format
        if !webrtc_stun_server.starts_with("stun://") {
            return Err(anyhow!("webrtc_stun_server must start with stun://"));
        }
        
        Ok(())
    }
    
    /// Create a new camera stream
    pub fn new(camera_info: CameraInfo, webrtc_stun_server: String, webrtc_turn_servers: Vec<String>) -> Result<Self> {
        // Validate inputs
        Self::validate_config(
            &camera_info.camera_uuid,
            &camera_info.camera_name,
            &camera_info.room_name,
            &camera_info.zmq_endpoint,
            &webrtc_stun_server,
        )?;
        
        info!(" Creating camera stream: {} ({})", camera_info.camera_name, camera_info.camera_uuid);
        info!("   Room: {} | ZMQ: {}", camera_info.room_name, camera_info.zmq_endpoint);

        Ok(CameraStream {
            camera_uuid: camera_info.camera_uuid.clone(),
            camera_name: camera_info.camera_name.clone(),
            room_name: camera_info.room_name.clone(),
            zmq_endpoint: camera_info.zmq_endpoint.clone(),
            codec: camera_info.codec.clone(),
            codec_detected_from_stream: false, // Will be updated when first frame arrives
            pipeline: None,
            appsrc: None,
            tee: None,
            viewers: Arc::new(Mutex::new(HashMap::new())),
            status: "initializing".to_string(),
            running: Arc::new(AtomicBool::new(false)),
            stats_poller_started: Arc::new(AtomicBool::new(false)),
            zmq_context: None,
            zmq_consuming: Arc::new(AtomicBool::new(false)),
            webrtc_stun_server,
            webrtc_turn_servers,
            timestamp_source: TimestampSource::from_env(),
        })
    }

    /// Start the camera stream pipeline (supports both H.264 and H.265)
    pub fn start(&mut self) -> Result<()> {
        info!(" Starting camera stream: {} (codec: {})", self.room_name, self.codec.to_string());
        info!(
            " [{}] Timestamp source mode: {}",
            self.room_name,
            self.timestamp_source.as_str()
        );

        // Build GStreamer pipeline - Dynamic codec support for H.264/H.265 data
        let video_caps = self.codec.get_gstreamer_caps();
        let parser_element = self.codec.get_parser_element();
        
        let pipeline_str = format!(
            "appsrc name=source is-live=true format=time do-timestamp=true ! \
             {} ! \
             {} name=parse ! \
             tee name=videotee allow-not-linked=true \
             videotee. ! queue ! fakesink sync=false",
            video_caps, parser_element
        );

        info!(" Pipeline ({}): {}", self.codec.to_string(), pipeline_str);

        let pipeline = gst::parse_launch(&pipeline_str)?
            .downcast::<gst::Pipeline>()
            .map_err(|_| anyhow!("Failed to downcast to Pipeline"))?;

        let appsrc = pipeline
            .by_name("source")
            .ok_or_else(|| anyhow!("Could not find appsrc element"))?;

        let tee = pipeline
            .by_name("videotee")
            .ok_or_else(|| anyhow!("Could not find tee element"))?;
        
        // Add probes on ALL elements in the main pipeline to find where flow stops
        let room_name_probe = self.room_name.clone();
        let codec_clone = self.codec.clone();
        
        // Probe after codec parser - COUNT PARSED FRAMES  
        if let Some(parse) = pipeline.by_name("parse") {
            if let Some(pad) = parse.static_pad("src") {
                let room = room_name_probe.clone();
                let codec_for_probe = codec_clone.clone();
                let frame_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
                let frame_count_probe = frame_count.clone();
                let last_log_time = std::sync::Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
                let last_log_time_probe = last_log_time.clone();
                let parsed_log_interval = std::time::Duration::from_secs(env_u64("PARSED_FRAMES_LOG_INTERVAL_SECS", 30));
                
                pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, probe_info| {
                    if let Some(gst::PadProbeData::Buffer(ref buffer)) = probe_info.data {
                        metrics::record_stream_output_frame(&room, buffer.size());
                        let count = frame_count_probe.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        debug!(" [{}] Parsed {} frame #{}: {} bytes", room, codec_for_probe.to_string(), count, buffer.size());
                        
                        // Log parsed-frame rate at a lower frequency to avoid noisy logs.
                        let now = std::time::Instant::now();
                        let mut last_log = last_log_time_probe.lock().unwrap();
                        if now.duration_since(*last_log) > parsed_log_interval {
                            let elapsed = now.duration_since(*last_log).as_secs_f64();
                            let fps = count as f64 / elapsed;
                            debug!(
                                " {} PARSED {} FRAMES: {} frames (rate: {:.1} fps) - Camera actual FPS",
                                room,
                                codec_for_probe.to_string(),
                                count,
                                fps
                            );
                            frame_count_probe.store(0, std::sync::atomic::Ordering::Relaxed);
                            *last_log = now;
                        }
                    }
                    gst::PadProbeReturn::Ok
                });
                info!(" Added {} frame counter after parse", self.codec.to_string());
            }
        } else {
            error!(" Could not find {} parser element 'parse' - frames won't be counted!", self.codec.get_parser_element());
        }
        
        
        // Add probe on tee to monitor data flow
        let room_name_for_probe = self.room_name.clone();
        let tee_src_pad = tee.static_pad("src_0")
            .or_else(|| tee.iterate_src_pads().into_iter().find_map(|p| p.ok()));
        
        if let Some(pad) = tee_src_pad {
            pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, probe_info| {
                if let Some(gst::PadProbeData::Buffer(ref buffer)) = probe_info.data {
                    debug!(" [{}] Main tee data flowing: {} bytes", room_name_for_probe, buffer.size());
                }
                gst::PadProbeReturn::Ok
            });
            info!(" Added probe to main tee for monitoring");
        } else {
            warn!(" Could not add probe to tee - no src pad found yet");
        }

        // Store a clone before downcasting
        let appsrc_clone = appsrc.clone();
        
        // Configure appsrc for codec-specific raw data
        let appsrc_elem = appsrc.downcast::<gst_app::AppSrc>()
            .map_err(|_| anyhow!("Failed to downcast appsrc to AppSrc"))?;
        let caps = gst::Caps::from_str(self.codec.get_gstreamer_caps())
            .map_err(|_| anyhow!("Failed to create caps from string: {}", self.codec.get_gstreamer_caps()))?;
        appsrc_elem.set_caps(Some(&caps));
        appsrc_elem.set_property("is-live", true);
        appsrc_elem.set_property("format", gst::Format::Time);
        // We set PTS/DTS in the ZMQ loop according to TIMESTAMP_SOURCE.
        appsrc_elem.set_property("do-timestamp", false);
        appsrc_elem.set_property("stream-type", gst_app::AppStreamType::Stream);
        appsrc_elem.set_property("min-latency", 0i64);
        appsrc_elem.set_property("max-latency", 1000000000i64); // 1 second
        
        // Add probe on appsrc src pad to see if data flows OUT of appsrc
        let room_name_appsrc_probe = self.room_name.clone();
        let codec_appsrc_probe = self.codec.clone();
        if let Some(appsrc_src_pad) = appsrc_elem.static_pad("src") {
            let appsrc_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let appsrc_count_probe = appsrc_count.clone();
            appsrc_src_pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, probe_info| {
                if let Some(gst::PadProbeData::Buffer(ref buffer)) = probe_info.data {
                    let count = appsrc_count_probe.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if count % 30 == 0 {  // Log every 30th frame (roughly 1 per second at 30fps)
                        debug!(" [{}] AppSrc {} frame #{}: {} bytes", room_name_appsrc_probe, codec_appsrc_probe.to_string(), count, buffer.size());
                    }
                }
                gst::PadProbeReturn::Ok
            });
            info!(" Added probe to appsrc src pad for {} monitoring", self.codec.to_string());
        }

        // Set up bus watch for error handling
        let pipeline_weak = pipeline.downgrade();
        let room_name_clone = self.room_name.clone();
        let viewers_for_watchdog = Arc::clone(&self.viewers);
        let viewers_for_watchdog2 = Arc::clone(&self.viewers);
        let running_for_watchdog = Arc::clone(&self.running);
        let zmq_consuming_for_watchdog = Arc::clone(&self.zmq_consuming);
        let pipeline_for_watchdog = pipeline.clone();
        let recovery_config = RecoveryConfig::from_env();
        let restart_cooldown_marker = Arc::new(Mutex::new(0u64));
        let restart_cooldown_marker_bus = Arc::clone(&restart_cooldown_marker);
        let dtls_warning_tracker = Arc::new(Mutex::new(DtlsWarningTracker::new()));
        let dtls_warning_tracker_bus = Arc::clone(&dtls_warning_tracker);
        
        let bus = pipeline.bus()
            .ok_or_else(|| anyhow!("Failed to get pipeline bus"))?;

        // Arm the shared running flag only after fallible startup steps above succeed.
        // This ensures start() does not return an error while leaving `running=true`.
        // It must still be set before spawning bus/watchdog workers so they don't
        // observe `false` and exit immediately.
        self.running.store(true, Ordering::SeqCst);
        
        bus.add_watch(move |_, msg| {
                use gst::MessageView;
                match msg.view() {
                    MessageView::Error(err) => {
                        error!(" Pipeline error for {}: {}", room_name_clone, err.error());
                        if let Some(debug) = err.debug() {
                            error!("Debug: {}", debug);
                        }

                        // Route the error-driven restart through the SAME cooldown
                        // marker as the DTLS watchdog so we never thrash the
                        // pipeline with back-to-back Null->Playing cycles.
                        if let Some(pipeline) = pipeline_weak.upgrade() {
                            let now_secs = now_epoch_secs();
                            let mut last_restart_secs = restart_cooldown_marker_bus
                                .lock()
                                .expect("Failed to lock restart cooldown marker");
                            if now_secs.saturating_sub(*last_restart_secs)
                                >= recovery_config.restart_cooldown_secs
                            {
                                warn!(" Attempting cooldown-gated restart of pipeline for {}", room_name_clone);
                                let _ = pipeline.set_state(gst::State::Null);
                                let _ = pipeline.set_state(gst::State::Playing);
                                *last_restart_secs = now_secs;
                                if let Ok(mut tracker) = dtls_warning_tracker_bus.lock() {
                                    tracker.reset();
                                }
                            } else {
                                debug!(
                                    " [{}] Error restart suppressed by cooldown ({}s)",
                                    room_name_clone,
                                    recovery_config.restart_cooldown_secs
                                );
                            }
                        }
                    }
                    MessageView::Eos(..) => {
                        warn!(" End of stream for camera: {}", room_name_clone);
                    }
                    MessageView::StateChanged(state) => {
                        // Check if the state change is from the pipeline itself
                        let is_pipeline_state = state.src()
                            .and_then(|src| pipeline_weak.upgrade()
                                .map(|p| src == p.upcast_ref::<gst::Object>()))
                            .unwrap_or(false);
                        
                        if is_pipeline_state {
                            debug!(" Pipeline {} state: {:?} → {:?}", 
                                   room_name_clone, state.old(), state.current());
                        }
                    }
                    MessageView::Warning(warn) => {
                        let warning_text = warn.error().to_string();
                        let debug_text = warn.debug().map(|d| d.to_string());

                        warn!(" Pipeline warning for {}: {}", room_name_clone, warning_text);
                        if let Some(debug) = &debug_text {
                            warn!("Debug: {}", debug);
                        }

                        if warning_matches_dtls_runtime_pattern(&warning_text, debug_text.as_deref()) {
                            let warning_count = {
                                let mut tracker = dtls_warning_tracker_bus
                                    .lock()
                                    .expect("Failed to lock DTLS warning tracker");
                                tracker.register_warning(&recovery_config)
                            };

                            warn!(
                                " [{}] DTLS warning pattern detected ({}/{} in {}s)",
                                room_name_clone,
                                warning_count,
                                recovery_config.dtls_warning_threshold,
                                recovery_config.dtls_warning_window_secs
                            );

                            // Forward warning to all active viewer monitors
                            if let Ok(viewers) = viewers_for_watchdog.lock() {
                                for peer in viewers.values() {
                                    if let Some(monitor) = &peer.monitor {
                                        if let Ok(mut m) = monitor.lock() {
                                            m.on_dtls_warning();
                                            let warning_lower = warning_text.to_ascii_lowercase();
                                            if warning_lower.contains("fingerprint") {
                                                m.on_dtls_failure_reason(
                                                    DtlsFailureReason::FingerprintMismatch,
                                                    Some(warning_text.clone()),
                                                );
                                            } else if warning_lower.contains("certificate")
                                                || warning_lower.contains("cert")
                                            {
                                                m.on_dtls_failure_reason(
                                                    DtlsFailureReason::CertificateInvalid,
                                                    Some(warning_text.clone()),
                                                );
                                            } else if warning_lower.contains("timeout") {
                                                m.on_dtls_failure_reason(
                                                    DtlsFailureReason::Timeout,
                                                    Some(warning_text.clone()),
                                                );
                                            }
                                        }
                                    }
                                }
                            }

                            if warning_count >= recovery_config.dtls_warning_threshold {
                                if let Some(pipeline) = pipeline_weak.upgrade() {
                                    let now_secs = now_epoch_secs();
                                    let mut last_restart_secs = restart_cooldown_marker_bus
                                        .lock()
                                        .expect("Failed to lock restart cooldown marker");

                                    if now_secs.saturating_sub(*last_restart_secs)
                                        >= recovery_config.restart_cooldown_secs
                                    {
                                        warn!(
                                            " [{}] Triggering auto-restart due to repeated DTLS warnings",
                                            room_name_clone
                                        );
                                        let _ = pipeline.set_state(gst::State::Null);
                                        let _ = pipeline.set_state(gst::State::Playing);
                                        *last_restart_secs = now_secs;

                                        let mut tracker = dtls_warning_tracker_bus
                                            .lock()
                                            .expect("Failed to lock DTLS warning tracker for reset");
                                        tracker.reset();
                                    } else {
                                        debug!(
                                            " [{}] DTLS restart suppressed by cooldown ({}s)",
                                            room_name_clone,
                                            recovery_config.restart_cooldown_secs
                                        );
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
                gst::glib::Continue(true)
            })
            .expect("Failed to add bus watch");

        // Watchdog: restart pipeline when input remains active but output stalls.
        let room_name_watchdog = self.room_name.clone();
        let recovery_config_watchdog = recovery_config;
        let restart_cooldown_marker_watchdog = Arc::clone(&restart_cooldown_marker);
        let viewers_for_watchdog = viewers_for_watchdog2;
        thread::spawn(move || {
            let mut stalled_windows = 0u64;
            let mut last_input_total = 0u64;
            let mut last_output_frames_total = 0u64;
            let mut last_output_packets_total = 0u64;

            while running_for_watchdog.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_secs(
                    recovery_config_watchdog.watchdog_check_interval_secs,
                ));

                if !running_for_watchdog.load(Ordering::SeqCst) {
                    break;
                }

                if !zmq_consuming_for_watchdog.load(Ordering::SeqCst) {
                    stalled_windows = 0;
                    continue;
                }

                let viewer_count = viewers_for_watchdog
                    .lock()
                    .map(|viewers| viewers.len())
                    .unwrap_or(0);

                if viewer_count == 0 {
                    stalled_windows = 0;
                    continue;
                }

                let metrics_snapshot = match metrics::get_stream_metrics(&room_name_watchdog) {
                    Some(snapshot) => snapshot,
                    None => continue,
                };

                let input_delta = metrics_snapshot
                    .input_frames_total
                    .saturating_sub(last_input_total);
                let output_frames_delta = metrics_snapshot
                    .output_frames_total
                    .saturating_sub(last_output_frames_total);
                let output_packets_delta = metrics_snapshot
                    .output_packets_total
                    .saturating_sub(last_output_packets_total);

                last_input_total = metrics_snapshot.input_frames_total;
                last_output_frames_total = metrics_snapshot.output_frames_total;
                last_output_packets_total = metrics_snapshot.output_packets_total;

                let input_active = metrics_snapshot.input_fps >= recovery_config_watchdog.watchdog_min_input_fps
                    || input_delta > 0;
                let output_stalled = metrics_snapshot.output_real_fps <= 0.01
                    && metrics_snapshot.output_packet_rate <= 0.01
                    && output_frames_delta == 0
                    && output_packets_delta == 0;

                if input_active && output_stalled {
                    stalled_windows = stalled_windows.saturating_add(1);
                    warn!(
                        " [{}] Watchdog detected stalled output (window {}/{}) input_fps={:.2} output_real_fps={:.2} output_packet_rate={:.2}",
                        room_name_watchdog,
                        stalled_windows,
                        recovery_config_watchdog.watchdog_stalled_windows,
                        metrics_snapshot.input_fps,
                        metrics_snapshot.output_real_fps,
                        metrics_snapshot.output_packet_rate
                    );
                } else {
                    stalled_windows = 0;
                }

                if stalled_windows < recovery_config_watchdog.watchdog_stalled_windows {
                    continue;
                }

                let now_secs = now_epoch_secs();
                let mut last_restart_secs = restart_cooldown_marker_watchdog
                    .lock()
                    .expect("Failed to lock restart cooldown marker (watchdog)");

                if now_secs.saturating_sub(*last_restart_secs)
                    < recovery_config_watchdog.restart_cooldown_secs
                {
                    debug!(
                        " [{}] Watchdog restart suppressed by cooldown ({}s)",
                        room_name_watchdog,
                        recovery_config_watchdog.restart_cooldown_secs
                    );
                    continue;
                }

                warn!(
                    " [{}] Triggering auto-restart: input active but output stalled for {} windows",
                    room_name_watchdog,
                    stalled_windows
                );

                let _ = pipeline_for_watchdog.set_state(gst::State::Null);
                let _ = pipeline_for_watchdog.set_state(gst::State::Playing);
                *last_restart_secs = now_secs;
                stalled_windows = 0;

                if let Ok(mut tracker) = dtls_warning_tracker.lock() {
                    tracker.reset();
                }
            }

            info!(" [{}] Recovery watchdog thread exited", room_name_watchdog);
        });

        // Start the pipeline
        if let Err(e) = pipeline.set_state(gst::State::Playing) {
            self.running.store(false, Ordering::SeqCst);
            return Err(anyhow!("Failed to set pipeline to Playing: {}", e));
        }
        info!(" Pipeline started for camera: {}", self.room_name);

        // Store pipeline elements
        self.pipeline = Some(pipeline);
        self.appsrc = Some(appsrc_clone);
        self.tee = Some(tee);

        // Pipeline is ready but ZMQ consumption will start only when first viewer connects
        self.status = "ready".to_string();
        info!(" Camera pipeline ready (ZMQ consumption will start when viewers connect): {}", self.room_name);

        Ok(())
    }

    /// Start ZMQ consumption when first viewer connects
    pub fn start_zmq_consumption(&mut self) -> Result<()> {
        if self.zmq_consuming.load(Ordering::SeqCst) {
            debug!(" ZMQ consumption already active for: {}", self.room_name);
            return Ok(());
        }

        let appsrc = self.appsrc.as_ref()
            .ok_or_else(|| anyhow!("AppSrc not available for ZMQ consumption"))?
            .clone()
            .downcast::<gst_app::AppSrc>()
            .map_err(|_| anyhow!("Failed to downcast to AppSrc"))?;

        if Self::is_test_source_endpoint(&self.zmq_endpoint) {
            info!(" Starting synthetic test source for: {} (first viewer connected)", self.room_name);
        } else {
            info!(" Starting ZMQ consumption for: {} (first viewer connected)", self.room_name);
        }
        self.zmq_consuming.store(true, Ordering::SeqCst);
        let start_result = if Self::is_test_source_endpoint(&self.zmq_endpoint) {
            self.start_test_pattern_processing(appsrc)
        } else {
            self.start_zmq_processing(appsrc)
        };
        if let Err(error) = start_result {
            self.zmq_consuming.store(false, Ordering::SeqCst);
            return Err(error);
        }
        self.status = "consuming".to_string();
        
        Ok(())
    }

    /// Stop ZMQ consumption when last viewer disconnects
    pub fn stop_zmq_consumption(&mut self) -> Result<()> {
        if !self.zmq_consuming.load(Ordering::SeqCst) {
            debug!(" ZMQ consumption already stopped for: {}", self.room_name);
            return Ok(());
        }

        info!(" Stopping ZMQ consumption for: {} (no viewers remaining)", self.room_name);
        self.zmq_consuming.store(false, Ordering::SeqCst);
        self.status = "ready".to_string();
        
        Ok(())
    }

    /// Internal ZMQ processing thread (now controlled by consumption state)
    fn start_zmq_processing(&mut self, appsrc: gst_app::AppSrc) -> Result<()> {
        let zmq_endpoint = self.zmq_endpoint.clone();
        let room_name = self.room_name.clone();
        let codec = self.codec.clone();
        let running = self.running.clone();
        let zmq_consuming = self.zmq_consuming.clone();
        let timestamp_source = self.timestamp_source;
        
        // Create a channel to communicate codec changes back to the main thread
        let (codec_tx, codec_rx) = mpsc::channel::<VideoCodec>();

        info!(" Starting ZMQ processing for {}: {}", room_name, zmq_endpoint);

        // Create ZMQ context and socket
        let zmq_context = zmq::Context::new();
        let socket = zmq_context.socket(zmq::SUB)?;
        
        // Subscribe to VideoFrame topic (RTSP adapter format)
        socket.set_subscribe(b"VideoFrame")?;
        socket.connect(&zmq_endpoint)?;
        socket.set_rcvtimeo(100)?; // 100ms timeout for non-blocking receives

        self.zmq_context = Some(zmq_context);

        // Spawn ZMQ processing thread
        let zmq_endpoint_for_thread = zmq_endpoint.clone();
        let codec_for_thread = codec.clone();
        thread::spawn(move || {
            info!(" ZMQ processing thread started for: {}", room_name);
            let mut frame_count = 0u64;
            let mut last_log_time = Instant::now();
            let log_interval = Duration::from_secs(env_u64("ZMQ_PROGRESS_LOG_INTERVAL_SECS", 30));
            let mut consecutive_errors = 0u32;
            const MAX_CONSECUTIVE_ERRORS: u32 = 20;
            let stream_started_at = Instant::now();
            let mut first_zmq_timestamp_ms: Option<u64> = None;

            // Emit idle heartbeat only when no frames were received for a while.
            let heartbeat_interval = Duration::from_secs(env_u64("ZMQ_IDLE_HEARTBEAT_SECS", 60));
            let mut last_frame_received_at = Instant::now();
            let mut last_heartbeat = Instant::now();

            while running.load(Ordering::SeqCst) && zmq_consuming.load(Ordering::SeqCst) {
                // Try to receive multipart message (RTSP adapter format)
                match socket.recv_multipart(zmq::DONTWAIT) {
                    Ok(parts) => {
                        consecutive_errors = 0; // Reset error counter
                        
                        if parts.len() == 5 {
                            // Parse multipart message: topic, stream_id, frame_details, timestamp, frame_data
                            let frame_data = &parts[4]; // The actual video codec data is in the 5th part
                            let frame_details = String::from_utf8_lossy(&parts[2]);
                            let zmq_timestamp_ms = std::str::from_utf8(&parts[3])
                                .ok()
                                .and_then(|value| value.parse::<u64>().ok());
                            
                            // Detect codec from frame details and video data
                            let detected_codec = VideoCodec::detect_from_zmq_data(&frame_details, frame_data);
                            
                            // Check if codec has changed from what we initially detected
                            if detected_codec != codec_for_thread {
                                warn!(" [{}] Codec changed from {} to {} - updating pipeline", 
                                      room_name, codec_for_thread.to_string(), detected_codec.to_string());
                                let _ = codec_tx.send(detected_codec.clone());
                            }
                            
                            if !frame_data.is_empty() {
                                // Create GStreamer buffer from video codec data and push to appsrc
                                let mut buffer = gst::Buffer::from_slice(frame_data.to_vec());

                                let selected_timestamp_ms = match timestamp_source {
                                    TimestampSource::System => {
                                        Some(stream_started_at.elapsed().as_millis() as u64)
                                    }
                                    TimestampSource::Zmq => {
                                        if let Some(ts) = zmq_timestamp_ms {
                                            let base = first_zmq_timestamp_ms.get_or_insert(ts);
                                            Some(ts.saturating_sub(*base))
                                        } else {
                                            warn!(
                                                " [{}] Missing/invalid ZMQ timestamp; falling back to system-relative timestamp",
                                                room_name
                                            );
                                            Some(stream_started_at.elapsed().as_millis() as u64)
                                        }
                                    }
                                };

                                if let Some(ts_ms) = selected_timestamp_ms {
                                    if let Some(buffer_mut) = buffer.get_mut() {
                                        let ts = gst::ClockTime::from_mseconds(ts_ms);
                                        buffer_mut.set_pts(ts);
                                        buffer_mut.set_dts(ts);
                                    }
                                }
                                
                                match appsrc.push_buffer(buffer) {
                                    Ok(_) => {
                                        metrics::record_stream_input_frame(&room_name, frame_data.len());
                                        frame_count += 1;
                                        last_frame_received_at = Instant::now();
                                        
                                        // Keep this as debug because stream_metrics already provides periodic INFO summaries.
                                        let now = Instant::now();
                                        if now.duration_since(last_log_time) > log_interval {
                                            let elapsed = now.duration_since(last_log_time).as_secs_f64();
                                            let rate = frame_count as f64 / elapsed;
                                            
                                            // Try to decode metadata for better logging
                                            let stream_id = String::from_utf8_lossy(&parts[1]);
                                            let frame_details = String::from_utf8_lossy(&parts[2]);
                                            
                                            debug!(
                                                " [{}] Processed {} {} frames (rate: {:.2} fps) - Stream: {}",
                                                room_name,
                                                frame_count,
                                                codec_for_thread.to_string(),
                                                rate,
                                                stream_id
                                            );
                                            debug!(" [{}] Frame details: {}", room_name, frame_details);
                                            
                                            last_log_time = now;
                                            frame_count = 0;
                                        }
                                    }
                                    Err(e) => {
                                        error!(" Failed to push video buffer to appsrc for {}: {}", room_name, e);
                                        consecutive_errors += 1;
                                    }
                                }
                            } else {
                                debug!(" [{}] Received empty frame data", room_name);
                            }
                        } else {
                            warn!(" [{}] Received invalid multipart message with {} parts (expected 5)", 
                                  room_name, parts.len());
                        }
                    }
                    Err(zmq::Error::EAGAIN) => {
                        // No message available, sleep briefly
                        thread::sleep(Duration::from_millis(1));

                        // Emit heartbeat only when the stream has truly been idle.
                        let now = Instant::now();
                        if now.duration_since(last_heartbeat) > heartbeat_interval
                            && now.duration_since(last_frame_received_at) > heartbeat_interval
                        {
                            debug!(
                                " [{}] ZMQ thread alive, waiting for data on: {}",
                                room_name,
                                zmq_endpoint_for_thread
                            );
                            last_heartbeat = now;
                        }
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors <= 5 {
                            warn!(" ZMQ receive error for {}: {}", room_name, e);
                        }
                        
                        if consecutive_errors > MAX_CONSECUTIVE_ERRORS {
                            error!(" Too many consecutive ZMQ errors for {}, stopping thread", room_name);
                            break;
                        }
                        
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
            
            info!(" ZMQ processing thread ended for: {}", room_name);
        });

        // Spawn a thread to handle codec changes
        let room_name_codec = self.room_name.clone();
        thread::spawn(move || {
            while let Ok(new_codec) = codec_rx.recv() {
                warn!(" [{}] Received codec change notification: {}", room_name_codec, new_codec.to_string());
                // Note: In a full implementation, you might want to restart the pipeline here
                // For now, we just log the change as the initial codec detection should be accurate
                info!(" [{}] Codec change detected but pipeline will continue with initial codec", room_name_codec);
                info!(" [{}] To support dynamic codec switching, consider restarting the camera stream", room_name_codec);
            }
        });

        Ok(())
    }

    fn start_test_pattern_processing(&mut self, appsrc: gst_app::AppSrc) -> Result<()> {
        let room_name = self.room_name.clone();
        let running = Arc::clone(&self.running);
        let zmq_consuming = Arc::clone(&self.zmq_consuming);
        let codec = self.codec.clone();

        let clip_path = Self::test_clip_path();
        if !clip_path.is_file() {
            return Err(anyhow!(
                "Test clip not found at {} (set TEST_CLIP_PATH or bake /app/assets/test_clip.h264 in the image)",
                clip_path.display()
            ));
        }

        info!(
            " Using baked test clip for {}: {}",
            room_name,
            clip_path.display()
        );

        let clip_location = clip_path.to_string_lossy().into_owned();
        let test_pipeline_desc = format!(
            "filesrc location={clip_location} ! \
             h264parse config-interval=1 ! \
             video/x-h264,stream-format=byte-stream,alignment=au ! \
             appsink name=test_sink emit-signals=true sync=true max-buffers=8 drop=false"
        );

        thread::spawn(move || {
            info!(" Synthetic test source thread started for {}", room_name);

            while running.load(Ordering::SeqCst) && zmq_consuming.load(Ordering::SeqCst) {
                let pipeline = match gst::parse_launch(&test_pipeline_desc) {
                    Ok(element) => match element.downcast::<gst::Pipeline>() {
                        Ok(pipeline) => pipeline,
                        Err(_) => {
                            error!(" [{}] Test source pipeline was not a gst::Pipeline", room_name);
                            thread::sleep(Duration::from_secs(1));
                            continue;
                        }
                    },
                    Err(e) => {
                        error!(" [{}] Failed to build test source pipeline: {}", room_name, e);
                        thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };

                let appsink = match pipeline.by_name("test_sink") {
                    Some(sink) => match sink.downcast::<AppSink>() {
                        Ok(appsink) => appsink,
                        Err(_) => {
                            error!(" [{}] Failed to downcast test appsink", room_name);
                            let _ = pipeline.set_state(gst::State::Null);
                            thread::sleep(Duration::from_millis(250));
                            continue;
                        }
                    },
                    None => {
                        error!(" [{}] Missing test_sink in synthetic source pipeline", room_name);
                        let _ = pipeline.set_state(gst::State::Null);
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                };

                let appsrc_clone = appsrc.clone();
                let running_for_cb = Arc::clone(&running);
                let consuming_for_cb = Arc::clone(&zmq_consuming);
                let room_for_cb = room_name.clone();
                let codec_for_cb = codec.clone();
                appsink.set_callbacks(
                    AppSinkCallbacks::builder()
                        .new_sample(move |sink| {
                            if !running_for_cb.load(Ordering::SeqCst)
                                || !consuming_for_cb.load(Ordering::SeqCst)
                            {
                                return Err(gst::FlowError::Eos);
                            }

                            let sample = sink.pull_sample().map_err(|_| gst::FlowError::Error)?;
                            let in_buf = sample.buffer().ok_or(gst::FlowError::Error)?;
                            if in_buf.size() == 0 {
                                return Ok(gst::FlowSuccess::Ok);
                            }

                            let out_buf = in_buf.copy();
                            metrics::record_stream_input_frame(&room_for_cb, out_buf.size());
                            appsrc_clone.push_buffer(out_buf).map_err(|err| {
                                debug!(
                                    " [{}] Failed to push synthetic {} buffer: {}",
                                    room_for_cb,
                                    codec_for_cb.to_string(),
                                    err
                                );
                                gst::FlowError::Eos
                            })?;
                            Ok(gst::FlowSuccess::Ok)
                        })
                        .build(),
                );

                if let Err(e) = pipeline.set_state(gst::State::Playing) {
                    error!(" [{}] Failed to start synthetic source pipeline: {}", room_name, e);
                    let _ = pipeline.set_state(gst::State::Null);
                    thread::sleep(Duration::from_millis(250));
                    continue;
                }

                let bus = match pipeline.bus() {
                    Some(bus) => bus,
                    None => {
                        error!(" [{}] Synthetic source pipeline missing bus", room_name);
                        let _ = pipeline.set_state(gst::State::Null);
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                };

                let mut should_restart = false;
                while running.load(Ordering::SeqCst) && zmq_consuming.load(Ordering::SeqCst) {
                    if let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(250)) {
                        use gst::MessageView;
                        match msg.view() {
                            MessageView::Eos(..) => {
                                should_restart = true;
                                break;
                            }
                            MessageView::Error(err) => {
                                error!(
                                    " [{}] Synthetic source pipeline error: {} {:?}",
                                    room_name,
                                    err.error(),
                                    err.debug()
                                );
                                break;
                            }
                            _ => {}
                        }
                    }
                }

                let _ = pipeline.set_state(gst::State::Null);

                if !running.load(Ordering::SeqCst) || !zmq_consuming.load(Ordering::SeqCst) {
                    break;
                }

                if should_restart {
                    debug!(" [{}] Restarting 60-frame synthetic clip", room_name);
                } else {
                    thread::sleep(Duration::from_millis(250));
                }
            }

            info!(" Synthetic test source thread ended for {}", room_name);
        });

        Ok(())
    }

    /// Stop the camera stream
    pub fn stop(&mut self) -> Result<()> {
        info!(" Stopping camera stream: {}", self.room_name);
        
        // Signal stop to ZMQ thread
        self.running.store(false, Ordering::SeqCst);
        self.zmq_consuming.store(false, Ordering::SeqCst);
        
        // Clean up all viewers
        {
            let viewers = self.viewers.lock()
                .expect("Failed to acquire viewers lock for cleanup");
            let viewer_ids: Vec<String> = viewers.keys().cloned().collect();
            drop(viewers); // Release the lock before calling remove_viewer_internal
            
            for viewer_id in viewer_ids {
                let mut viewers = self.viewers.lock()
                    .expect("Failed to acquire viewers lock for removal");
                self.remove_viewer_internal(&viewer_id, &mut viewers);
            }
        }
        
        // Stop GStreamer pipeline
        if let Some(pipeline) = &self.pipeline {
            pipeline.set_state(gst::State::Null)?;
            info!(" Pipeline stopped for: {}", self.room_name);
        }
        
        self.status = "stopped".to_string();
        info!(" Camera stream stopped: {}", self.room_name);
        
        Ok(())
    }

    /// Add a viewer to this camera stream
    pub fn add_viewer(&self, viewer_id: &str, sender: tokio::sync::mpsc::UnboundedSender<String>) -> Result<()> {
        let mut viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for adding viewer");
        let current_count = viewers.len();
        
        info!(" [{}] Adding viewer {} (current: {})", self.room_name, viewer_id, current_count);
        
        viewers.insert(viewer_id.to_string(), ViewerPeer {
            id: viewer_id.to_string(),
            webrtcbin: None,
            tee_pad: None,
            sender,
            monitor: Some(Arc::new(Mutex::new(ConnectionMonitor::new(
                viewer_id,
                &self.camera_uuid,
                &self.camera_name,
                &self.room_name,
            )))),
            ice_restart_attempts: 0,
            last_ice_restart_epoch: 0,
        });
        
        let new_count = viewers.len();
        let viewer_list: Vec<String> = viewers.keys().cloned().collect();
        
        info!(" [{}] Viewer {} added successfully", self.room_name, viewer_id);
        info!(" [{}] VIEWER COUNT: {} → {} (viewers: {})", 
              self.room_name, current_count, new_count, viewer_list.join(", "));
        
        // Log viewer statistics
        self.log_viewer_statistics(&viewers);
        
        Ok(())
    }

    /// Log detailed viewer statistics for this camera
    fn log_viewer_statistics(&self, viewers: &std::collections::HashMap<String, ViewerPeer>) {
        let viewer_count = viewers.len();
        let viewer_ids: Vec<String> = viewers.keys().cloned().collect();
        
        info!(" [{}] === VIEWER STATISTICS ===", self.room_name);
        info!(" [{}] Camera: {} ({})", self.room_name, self.camera_name, self.camera_uuid);
        info!(" [{}] Total Viewers: {}", self.room_name, viewer_count);
        info!(" [{}] ZMQ Consuming: {}", self.room_name, self.zmq_consuming.load(Ordering::SeqCst));
        info!(" [{}] Pipeline Status: {}", self.room_name, self.status);
        
        if viewer_count > 0 {
            info!(" [{}] Active Viewers:", self.room_name);
            for (i, viewer_id) in viewer_ids.iter().enumerate() {
                info!(" [{}]   {}. {}", self.room_name, i + 1, viewer_id);
            }
        } else {
            info!(" [{}] No active viewers", self.room_name);
        }
        info!(" [{}] ========================", self.room_name);
    }

    /// Remove a viewer from this camera stream
    pub fn remove_viewer(&self, viewer_id: &str) {
        let mut viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for removing viewer");
        let current_count = viewers.len();
        
        info!(" [{}] Removing viewer {} (current: {})", self.room_name, viewer_id, current_count);
        
        self.remove_viewer_internal(viewer_id, &mut viewers);
        
        let new_count = viewers.len();
        let viewer_list: Vec<String> = viewers.keys().cloned().collect();
        
        info!(" [{}] Viewer {} removed successfully", self.room_name, viewer_id);
        info!(" [{}] VIEWER COUNT: {} → {} (viewers: {})", 
              self.room_name, current_count, new_count, 
              if viewer_list.is_empty() { "none".to_string() } else { viewer_list.join(", ") });
        
        // Log viewer statistics
        self.log_viewer_statistics(&viewers);
    }

    fn remove_viewer_internal(&self, viewer_id: &str, viewers: &mut std::sync::MutexGuard<'_, HashMap<String, ViewerPeer>>) {
        if let Some(viewer) = viewers.remove(viewer_id) {
            // Finalize the ConnectionMonitor and print diagnostic report
            if let Some(monitor_arc) = viewer.monitor {
                if let Ok(mut m) = monitor_arc.lock() {
                    m.on_viewer_teardown();
                }
                match Arc::try_unwrap(monitor_arc) {
                    Ok(mutex) => match mutex.into_inner() {
                        Ok(monitor) => {
                            let report = monitor.finalize();
                            report.print();
                            report.emit_json_report();
                        }
                        Err(e) => { log::warn!(" [MONITOR][{}] Mutex poisoned: {}", viewer_id, e); }
                    },
                    Err(_arc) => {
                        log::warn!(" [MONITOR][{}] Could not finalize: Arc still has other owners", viewer_id);
                    }
                }
            }

            // Clean up WebRTC pipeline
            if let Some(webrtcbin) = viewer.webrtcbin {
                if let Some(pipeline) = &self.pipeline {
                    let _ = webrtcbin.set_state(gst::State::Null);
                    let _ = pipeline.remove(&webrtcbin);
                }
            }

            // Release tee pad
            if let Some(tee_pad) = viewer.tee_pad {
                if let Some(tee) = &self.tee {
                    tee.release_request_pad(&tee_pad);
                }
            }

            info!(" Viewer {} removed from camera: {} (remaining: {})",
                  viewer_id, self.room_name, viewers.len());
        }
    }

    /// Get current viewer count
    pub fn get_viewer_count(&self) -> usize {
        self.viewers.lock()
            .expect("Failed to acquire viewers lock for count")
            .len()
    }

    /// Check if ZMQ consumption is active
    pub fn is_zmq_consuming(&self) -> bool {
        self.zmq_consuming.load(Ordering::SeqCst)
    }

    /// Get camera status
    pub fn get_status(&self) -> String {
        self.status.clone()
    }

    /// Get detailed viewer statistics for this camera
    pub fn get_viewer_stats(&self) -> serde_json::Value {
        let viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for stats");
        let viewer_list: Vec<String> = viewers.keys().cloned().collect();
        
        serde_json::json!({
            "camera_id": self.room_name,
            "camera_name": self.camera_name,
            "viewer_count": viewers.len(),
            "viewers": viewer_list,
            "status": self.status,
            "zmq_consuming": self.zmq_consuming.load(Ordering::SeqCst),
            "pipeline_running": self.running.load(Ordering::SeqCst)
        })
    }

    /// Get viewer IDs for this camera
    pub fn get_viewer_ids(&self) -> Vec<String> {
        let viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for IDs");
        viewers.keys().cloned().collect()
    }

    /// Check if a specific viewer is connected to this camera
    pub fn has_viewer(&self, viewer_id: &str) -> bool {
        let viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for check");
        viewers.contains_key(viewer_id)
    }

    /// Create WebRTC pipeline for viewer via enhanced signaling server
    pub fn create_signaling_viewer_pipeline(&self, viewer_id: &str, app_state: std::sync::Arc<crate::state::AppState>) -> Result<()> {
        info!(" Creating WebRTC pipeline for signaling viewer {} on camera: {}", viewer_id, self.room_name);
        
        // Create a signaling sender that routes messages through the app_state to the signaling server
        let (signaling_sender, mut signaling_receiver) = tokio::sync::mpsc::unbounded_channel::<String>();
        
        // Spawn a thread to handle messages from the camera manager and route them through the signaling server
        let viewer_id_clone = viewer_id.to_string();
        let viewer_id_for_monitor = viewer_id.to_string();
        let app_state_clone = app_state.clone();
        let viewers_for_signaling_monitor = self.viewers.clone();
        thread::spawn(move || {
            while let Some(message) = signaling_receiver.blocking_recv() {
                // Parse the message to determine if it's an SDP offer or ICE candidate
                if let Ok(msg) = serde_json::from_str::<serde_json::Value>(&message) {
                    if let Some(client) = app_state_clone.get_socketio_signaling_client() {
                        if let Some(sdp) = msg.get("sdp") {
                            if let Some(sdp_str) = sdp.get("sdp").and_then(|s| s.as_str()) {
                                let offer_data = serde_json::json!({
                                    "viewer_id": viewer_id_clone,
                                    "data": {
                                        "type": "offer",
                                        "sdp": sdp_str
                                    }
                                });
                                if let Err(e) = client.emit("webrtc_offer", offer_data) {
                                    error!(" Failed to send SDP offer via signaling: {}", e);
                                    if let Ok(viewers) = viewers_for_signaling_monitor.lock() {
                                        if let Some(peer) = viewers.get(&viewer_id_for_monitor) {
                                            if let Some(monitor) = &peer.monitor {
                                                if let Ok(mut m) = monitor.lock() {
                                                    m.on_signaling_channel_dropped();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else if let Some(ice) = msg.get("ice") {
                            if let (Some(candidate), Some(sdp_m_line_index)) = (
                                ice.get("candidate").and_then(|c| c.as_str()),
                                ice.get("sdpMLineIndex").and_then(|i| i.as_u64())
                            ) {
                                let ice_data = serde_json::json!({
                                    "viewer_id": viewer_id_clone,
                                    "data": {
                                        "candidate": candidate,
                                        "sdpMLineIndex": sdp_m_line_index as u32
                                    }
                                });
                                if let Err(e) = client.emit("webrtc_ice_candidate", ice_data) {
                                    error!(" Failed to send ICE candidate via signaling: {}", e);
                                    if let Ok(viewers) = viewers_for_signaling_monitor.lock() {
                                        if let Some(peer) = viewers.get(&viewer_id_for_monitor) {
                                            if let Some(monitor) = &peer.monitor {
                                                if let Ok(mut m) = monitor.lock() {
                                                    m.on_gateway_candidate_sent(false);
                                                    m.on_signaling_channel_dropped();
                                                }
                                            }
                                        }
                                    }
                                } else if let Ok(viewers) = viewers_for_signaling_monitor.lock() {
                                    if let Some(peer) = viewers.get(&viewer_id_for_monitor) {
                                        if let Some(monitor) = &peer.monitor {
                                            if let Ok(mut m) = monitor.lock() {
                                                m.on_gateway_candidate_sent(true);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        
        // Register the viewer with the signaling sender
        self.add_viewer(viewer_id, signaling_sender)?;
        
        // Then create the WebRTC pipeline
        self.create_viewer_webrtc_pipeline(viewer_id)
    }

    /// Create WebRTC pipeline branch for a specific viewer (legacy method)
    pub fn create_viewer_webrtc_pipeline(&self, viewer_id: &str) -> Result<()> {
        info!(" Creating WebRTC pipeline for viewer {} on camera: {}", viewer_id, self.room_name);
        
        let mut viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for WebRTC pipeline creation");
        let viewer = viewers.get_mut(viewer_id)
            .ok_or_else(|| anyhow!("Viewer {} not found", viewer_id))?;
        
        let pipeline = self.pipeline.as_ref()
            .ok_or_else(|| anyhow!("Pipeline not available"))?;
            
        let tee = self.tee.as_ref()
            .ok_or_else(|| anyhow!("Tee element not available"))?;
        
        // Create WebRTC pipeline elements
        let (queue, parser, payloader, capsfilter, webrtcbin) = 
            self.create_webrtc_elements(viewer_id)?;
        
        // Add and link elements
        pipeline.add_many(&[&queue, &parser, &payloader, &capsfilter, &webrtcbin])?;
        gst::Element::link_many(&[&queue, &parser, &payloader, &capsfilter])?;
        
        // Link capsfilter to webrtcbin
        self.link_capsfilter_to_webrtcbin(&capsfilter, &webrtcbin)?;
        
        // Connect tee to queue with blocking probe
        let tee_pad = self.connect_tee_to_queue(tee, &queue)?;
        
        // Sync all elements to pipeline state
        self.sync_elements_state(&[&queue, &parser, &payloader, &capsfilter, &webrtcbin])?;
        
        // Unblock tee pad and force reconfiguration
        self.unblock_and_reconfigure_tee(tee, &tee_pad)?;
        
        // Add buffer probes for monitoring
        self.add_buffer_probes(viewer_id, &tee_pad, &queue, &webrtcbin)?;
        
        // Set up WebRTC signal handlers
        self.setup_webrtc_signals(&webrtcbin, viewer_id)?;

        // Update viewer record
        viewer.webrtcbin = Some(webrtcbin.clone());
        viewer.tee_pad = Some(tee_pad);
        
        info!(" [{}] WebRTC pipeline created for viewer: {}", self.room_name, viewer_id);
        
        // Drop the lock before triggering negotiation to avoid deadlocks
        drop(viewers);

        // Ensure the single shared stats poller is running for this stream
        // (replaces the previous one-thread-per-viewer model).
        self.ensure_stats_poller();

        // Trigger WebRTC negotiation
        self.trigger_webrtc_negotiation(webrtcbin, viewer_id)?;
        
        Ok(())
    }
    
    /// Create all WebRTC elements for a viewer.
    ///
    /// The construction logic lives in `crate::pipeline::webrtc` so this monolith
    /// no longer owns GStreamer element wiring details.
    fn create_webrtc_elements(&self, viewer_id: &str) -> Result<(gst::Element, gst::Element, gst::Element, gst::Element, gst::Element)> {
        let elements = crate::pipeline::webrtc::build_webrtc_elements(
            &self.codec,
            &self.webrtc_stun_server,
            &self.webrtc_turn_servers,
            viewer_id,
        )?;
        Ok((
            elements.queue,
            elements.parser,
            elements.payloader,
            elements.capsfilter,
            elements.webrtcbin,
        ))
    }
    
    /// Link capsfilter to webrtcbin using pads
    fn link_capsfilter_to_webrtcbin(&self, capsfilter: &gst::Element, webrtcbin: &gst::Element) -> Result<()> {
        let capsfilter_src = capsfilter.static_pad("src")
            .ok_or_else(|| anyhow!("Failed to get capsfilter src pad"))?;
        let webrtc_sink = webrtcbin.request_pad_simple("sink_%u")
            .ok_or_else(|| anyhow!("Failed to get webrtcbin sink pad"))?;
        capsfilter_src.link(&webrtc_sink)?;
        Ok(())
    }
    
    /// Connect tee to queue with blocking probe to prevent race conditions
    fn connect_tee_to_queue(&self, tee: &gst::Element, queue: &gst::Element) -> Result<gst::Pad> {
        let tee_pad = tee.request_pad_simple("src_%u")
            .ok_or_else(|| anyhow!("Failed to get tee pad"))?;
        let queue_sink = queue.static_pad("sink")
            .ok_or_else(|| anyhow!("Failed to get queue sink pad"))?;
        
        debug!(" [{}] Linking tee pad {} to queue sink {}", 
               self.room_name, tee_pad.name(), queue_sink.name());
        
        tee_pad.link(&queue_sink)?;
        
        debug!(" [{}] Tee pad {} linked to queue sink {}", 
               self.room_name, tee_pad.name(), queue_sink.name());
        
        Ok(tee_pad)
    }
    
    /// Sync all elements to pipeline state
    fn sync_elements_state(&self, elements: &[&gst::Element]) -> Result<()> {
        for elem in elements {
            elem.sync_state_with_parent()?;
            debug!(" [{}] IMMEDIATE sync element {} to state: {:?}", 
                   self.room_name, elem.name(), elem.current_state());
        }
        Ok(())
    }
    
    /// Unblock tee pad and force reconfiguration to start data flow
    fn unblock_and_reconfigure_tee(&self, tee: &gst::Element, _tee_pad: &gst::Pad) -> Result<()> {
        // Force tee to push to new pad by triggering reconfiguration
        let _ = tee.set_state(gst::State::Paused);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _ = tee.set_state(gst::State::Playing);
        
        debug!(" [{}] Forced tee reconfiguration to activate new pad", self.room_name);
        debug!(" [{}] Tee state: {:?}, pad count: {}", 
               self.room_name, tee.current_state(), 
               tee.iterate_src_pads().into_iter().filter_map(Result::ok).count());
        
        Ok(())
    }
    
    /// Add buffer probes for monitoring data flow
    fn add_buffer_probes(&self, viewer_id: &str, tee_pad: &gst::Pad, queue: &gst::Element, webrtcbin: &gst::Element) -> Result<()> {
        let queue_sink = queue.static_pad("sink")
            .ok_or_else(|| anyhow!("Failed to get queue sink pad"))?;
        
        // Probe 1: Tee output pad
        let viewer_id_tee_probe = viewer_id.to_string();
        let room_name_tee_probe = self.room_name.clone();
        let buffer_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let buffer_count_probe = buffer_count.clone();
        tee_pad.add_probe(gst::PadProbeType::BUFFER, move |_pad, probe_info| {
            if let Some(gst::PadProbeData::Buffer(ref buffer)) = probe_info.data {
                let count = buffer_count_probe.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if count % 30 == 0 {
                    debug!(" [{}] TEE → Queue for viewer {} (#{}: {} bytes)", 
                           room_name_tee_probe, viewer_id_tee_probe, count, buffer.size());
                }
            }
            gst::PadProbeReturn::Ok
        });
        
        // Probe 2: Queue input pad
        let viewer_id_queue_probe = viewer_id.to_string();
        let room_name_queue_probe = self.room_name.clone();
        let queue_buffer_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let queue_buffer_count_probe = queue_buffer_count.clone();
        queue_sink.add_probe(gst::PadProbeType::BUFFER, move |_pad, probe_info| {
            if let Some(gst::PadProbeData::Buffer(ref buffer)) = probe_info.data {
                metrics::record_stream_output_packet(&room_name_queue_probe, buffer.size());
                let count = queue_buffer_count_probe.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if count % 30 == 0 {
                    debug!(" [{}] Queue received for viewer {} (#{}: {} bytes)", 
                           room_name_queue_probe, viewer_id_queue_probe, count, buffer.size());
                }
            }
            gst::PadProbeReturn::Ok
        });
        
        // Probe 3: WebRTCbin input pad
        let webrtc_sink = webrtcbin.pads()
            .into_iter()
            .find(|pad| pad.direction() == gst::PadDirection::Sink)
            .ok_or_else(|| anyhow!("Failed to find webrtcbin sink pad"))?;
        
        let viewer_id_probe = viewer_id.to_string();
        let room_name_probe = self.room_name.clone();
        let viewers_for_frame_monitor = self.viewers.clone();
        let viewer_id_for_frame_monitor = viewer_id.to_string();
        let webrtc_buffer_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let webrtc_buffer_count_probe = webrtc_buffer_count.clone();
        webrtc_sink.add_probe(gst::PadProbeType::BUFFER, move |_pad, probe_info| {
            if let Some(gst::PadProbeData::Buffer(ref buffer)) = probe_info.data {
                let count = webrtc_buffer_count_probe.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Ok(viewers) = viewers_for_frame_monitor.lock() {
                    if let Some(peer) = viewers.get(&viewer_id_for_frame_monitor) {
                        if let Some(monitor) = &peer.monitor {
                            if let Ok(mut m) = monitor.lock() {
                                m.on_first_rtp_packet();
                                if !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT) {
                                    m.on_first_keyframe();
                                }
                                m.on_frame_sent();
                            }
                        }
                    }
                }
                if count % 30 == 0 {
                    debug!(" [{}] WebRTCbin received buffer for viewer {} (#{}: {} bytes)", 
                           room_name_probe, viewer_id_probe, count, buffer.size());
                }
            }
            gst::PadProbeReturn::Ok
        });
        
        Ok(())
    }
    
    /// Trigger WebRTC negotiation in a separate thread
    /// Start the single shared stats poller for this stream if it is not
    /// already running. Idempotent across viewers.
    fn ensure_stats_poller(&self) {
        if self
            .stats_poller_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            debug!(" [{}] Starting shared stats poller", self.room_name);
            start_stats_poller(
                self.viewers.clone(),
                self.room_name.clone(),
                self.running.clone(),
                self.stats_poller_started.clone(),
            );
        }
    }

    fn trigger_webrtc_negotiation(&self, webrtcbin: gst::Element, viewer_id: &str) -> Result<()> {
        info!(" [{}] Triggering WebRTC negotiation for viewer: {}", self.room_name, viewer_id);
        
        let webrtcbin_clone = webrtcbin.clone();
        let viewers_clone = self.viewers.clone();
        let viewer_id_clone = viewer_id.to_string();
        let room_name_clone = self.room_name.clone();
        
        std::thread::spawn(move || {
            // Small delay to ensure pipeline is fully ready
            std::thread::sleep(Duration::from_millis(100));
            
            info!(" [{}] Creating SDP offer for viewer: {}", room_name_clone, viewer_id_clone);
            debug!(" [{}] WebRTCbin state before offer: {:?}", room_name_clone, webrtcbin_clone.current_state());
            
            let webrtcbin_for_promise = webrtcbin_clone.clone();
            
            let promise = gst::Promise::with_change_func(move |reply| {
                if let Ok(Some(reply)) = reply {
                    if let Ok(offer_val) = reply.value("offer") {
                        if let Ok(offer) = offer_val.get::<gst_webrtc::WebRTCSessionDescription>() {
                            info!(" [{}] SDP offer created for viewer: {}", room_name_clone, viewer_id_clone);
                            
                            // Set local description
                            let local_promise = gst::Promise::new();
                            webrtcbin_for_promise.emit_by_name::<()>("set-local-description", &[&offer, &local_promise]);
                            
                            // Send offer to viewer
                            let sdp_str = offer.sdp().as_text().unwrap();
                            let msg = json!({
                                "sdp": {
                                    "type": "offer",
                                    "sdp": sdp_str
                                }
                            });
                            
                            let viewers = viewers_clone.lock()
                                .expect("Failed to acquire viewers lock for SDP offer");
                            if let Some(viewer) = viewers.get(&viewer_id_clone) {
                                if let Some(monitor) = &viewer.monitor {
                                    if let Ok(mut m) = monitor.lock() {
                                        if let Some(fp) = parse_sdp_fingerprint(&sdp_str) {
                                            m.on_local_fingerprint(fp);
                                        }
                                    }
                                }
                                let _ = viewer.sender.send(msg.to_string());
                                info!(" [{}] SDP offer sent to viewer: {}", room_name_clone, viewer_id_clone);
                            }
                        }
                    }
                } else {
                    error!(" [{}] Failed to create SDP offer for viewer: {}", room_name_clone, viewer_id_clone);
                }
            });
            
            // Create the offer
            webrtcbin_clone.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
        });
        
        Ok(())
    }

    fn setup_webrtc_signals(&self, webrtcbin: &gst::Element, viewer_id: &str) -> Result<()> {
        let viewers = self.viewers.clone();
        let viewer_id_clone = viewer_id.to_string();
        let viewer_id_for_ice_state = viewer_id.to_string();
        let room_name_for_ice_state = self.room_name.clone();
        let viewers_for_monitor = self.viewers.clone();
        let viewer_id_for_monitor = viewer_id.to_string();
        let viewer_id_for_srtp = viewer_id.to_string();
        let viewers_for_srtp = self.viewers.clone();
        let viewers_for_pc_state = self.viewers.clone();
        let viewer_id_for_pc_state = viewer_id.to_string();
        let viewers_for_ice_restart = self.viewers.clone();
        let viewer_id_for_ice_restart = viewer_id.to_string();
        let room_for_ice_restart = self.room_name.clone();

        debug!(" [{}] Setting up WebRTC signals for viewer: {}", self.room_name, viewer_id);

        // ── ICE connection state ─────────────────────────────────────────
        webrtcbin.connect_notify(Some("ice-connection-state"), move |element, _| {
            let state = element.property::<gst_webrtc::WebRTCICEConnectionState>("ice-connection-state");

            // Update ConnectionMonitor
            if let Ok(viewers) = viewers_for_monitor.lock() {
                if let Some(peer) = viewers.get(&viewer_id_for_monitor) {
                    if let Some(monitor) = &peer.monitor {
                        if let Ok(mut m) = monitor.lock() {
                            match state {
                                gst_webrtc::WebRTCICEConnectionState::Checking     => m.on_ice_checking(),
                                gst_webrtc::WebRTCICEConnectionState::Connected    => {
                                    m.on_ice_connected();
                                    m.print_early_report();
                                }
                                gst_webrtc::WebRTCICEConnectionState::Completed    => {
                                    m.on_ice_completed();
                                    m.print_early_report(); // no-op if already printed on Connected
                                }
                                gst_webrtc::WebRTCICEConnectionState::Failed       => {
                                    m.on_ice_failed();
                                    m.print_early_report(); // print immediately on failure
                                }
                                gst_webrtc::WebRTCICEConnectionState::Disconnected => m.on_ice_disconnected(),
                                _ => {}
                            }
                        }
                    }
                }
            }

            match state {
                gst_webrtc::WebRTCICEConnectionState::Connected
                | gst_webrtc::WebRTCICEConnectionState::Completed => {
                    info!(
                        " [{}] ICE connection SUCCESS for viewer {} (state={:?})",
                        room_name_for_ice_state,
                        viewer_id_for_ice_state,
                        state
                    );
                    // A healthy connection clears the ICE-restart budget so a
                    // later disconnect gets a fresh set of recovery attempts.
                    if let Ok(mut guard) = viewers_for_ice_restart.lock() {
                        if let Some(peer) = guard.get_mut(&viewer_id_for_ice_restart) {
                            peer.ice_restart_attempts = 0;
                        }
                    }
                }
                gst_webrtc::WebRTCICEConnectionState::Failed => {
                    error!(
                        " [{}] ICE connection FAILURE for viewer {} (state={:?})",
                        room_name_for_ice_state,
                        viewer_id_for_ice_state,
                        state
                    );
                }
                gst_webrtc::WebRTCICEConnectionState::Disconnected => {
                    warn!(
                        " [{}] ICE connection interrupted for viewer {} (state={:?}); attempting ICE restart",
                        room_name_for_ice_state,
                        viewer_id_for_ice_state,
                        state
                    );
                    maybe_ice_restart(
                        element.clone(),
                        viewers_for_ice_restart.clone(),
                        viewer_id_for_ice_restart.clone(),
                        room_for_ice_restart.clone(),
                    );
                }
                gst_webrtc::WebRTCICEConnectionState::Closed => {
                    warn!(
                        " [{}] ICE connection interrupted for viewer {} (state={:?})",
                        room_name_for_ice_state,
                        viewer_id_for_ice_state,
                        state
                    );
                }
                _ => {
                    debug!(
                        " [{}] ICE connection state changed for viewer {} -> {:?}",
                        room_name_for_ice_state,
                        viewer_id_for_ice_state,
                        state
                    );
                }
            }
        });

        // ── ICE gathering state ──────────────────────────────────────────
        let viewers_for_gathering = self.viewers.clone();
        let viewer_id_for_gathering = viewer_id.to_string();
        webrtcbin.connect_notify(Some("ice-gathering-state"), move |element, _| {
            let state = element.property::<gst_webrtc::WebRTCICEGatheringState>("ice-gathering-state");
            if state == gst_webrtc::WebRTCICEGatheringState::Complete {
                if let Ok(viewers) = viewers_for_gathering.lock() {
                    if let Some(peer) = viewers.get(&viewer_id_for_gathering) {
                        if let Some(monitor) = &peer.monitor {
                            if let Ok(mut m) = monitor.lock() {
                                m.on_ice_gathering_complete();
                            }
                        }
                    }
                }
            }
        });

        // ── SRTP: pad-added fires when DTLS completes and media pad is live ─
        webrtcbin.connect("pad-added", false, move |_args| {
            if let Ok(viewers) = viewers_for_srtp.lock() {
                if let Some(peer) = viewers.get(&viewer_id_for_srtp) {
                    if let Some(monitor) = &peer.monitor {
                        if let Ok(mut m) = monitor.lock() {
                            m.on_srtp_keys_ready();
                        }
                    }
                }
            }
            None
        });

        // connection-state transitions are the most reliable high-level indicator that
        // DTLS completed in webrtcbin. Map this into monitor state.
        webrtcbin.connect_notify(Some("connection-state"), move |element, _| {
            let state = element.property::<gst_webrtc::WebRTCPeerConnectionState>("connection-state");
            if let Ok(viewers) = viewers_for_pc_state.lock() {
                if let Some(peer) = viewers.get(&viewer_id_for_pc_state) {
                    if let Some(monitor) = &peer.monitor {
                        if let Ok(mut m) = monitor.lock() {
                            match state {
                                gst_webrtc::WebRTCPeerConnectionState::Connecting => m.on_dtls_connecting(),
                                gst_webrtc::WebRTCPeerConnectionState::Connected => {
                                    m.on_dtls_connected();
                                    m.on_srtp_keys_ready();
                                }
                                gst_webrtc::WebRTCPeerConnectionState::Failed => m.on_dtls_failed(),
                                _ => {}
                            }
                        }
                    }
                }
            }
        });

        // ── on-ice-candidate: capture local candidates ───────────────────
        webrtcbin.connect("on-ice-candidate", false, move |args| {
            let mlineindex = args.get(1).and_then(|v| v.get::<u32>().ok()).unwrap_or(0);
            let candidate = match args.get(2).and_then(|v| v.get::<String>().ok()) {
                Some(c) => c,
                None => {
                    warn!(" on-ice-candidate signal missing candidate value; skipping");
                    return None;
                }
            };

            debug!(" [{}] Sending ICE candidate to viewer", viewer_id_clone);

            let viewers = viewers.lock()
                .expect("Failed to acquire viewers lock for ICE candidate");
            if let Some(viewer) = viewers.get(&viewer_id_clone) {
                // Record in monitor
                if let Some(monitor) = &viewer.monitor {
                    if let Ok(mut m) = monitor.lock() {
                        m.on_local_candidate(&candidate);
                    }
                }
                let msg = serde_json::json!({
                    "ice": {
                        "candidate": candidate,
                        "sdpMLineIndex": mlineindex
                    }
                });
                let _ = viewer.sender.send(msg.to_string());
            }
            None
        });

        Ok(())
    }

    /// Handle SDP answer from viewer
    pub fn handle_viewer_sdp_answer(&self, viewer_id: &str, sdp: &serde_json::Value) -> Result<()> {
        let viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for SDP answer");
        let viewer = viewers.get(viewer_id)
            .ok_or_else(|| anyhow!("Viewer {} not found", viewer_id))?;
            
        let webrtcbin = viewer.webrtcbin.as_ref()
            .ok_or_else(|| anyhow!("WebRTC bin not found for viewer {}", viewer_id))?;
        
        if sdp["type"] == "answer" {
            let sdp_text = match sdp["sdp"].as_str() {
                Some(s) if !s.is_empty() => s,
                _ => {
                    warn!(" Viewer {} sent an answer with missing/empty SDP; ignoring", viewer_id);
                    return Ok(());
                }
            };
            info!(" Received SDP answer from viewer: {}", viewer_id);

            // Track remote candidates embedded in SDP answer (non-trickle/initial batch).
            if let Some(monitor) = &viewer.monitor {
                if let Ok(mut m) = monitor.lock() {
                    if let Some(remote_fp) = parse_sdp_fingerprint(sdp_text) {
                        m.on_remote_fingerprint(remote_fp);
                    }
                    let mut remote_sdp_candidate_count = 0u64;
                    for line in sdp_text.lines() {
                        if let Some(candidate) = line.strip_prefix("a=candidate:") {
                            m.on_remote_candidate(candidate.trim());
                            remote_sdp_candidate_count = remote_sdp_candidate_count.saturating_add(1);
                        }
                    }
                    m.on_viewer_sdp_candidates_received(remote_sdp_candidate_count);
                    m.on_dtls_connecting();
                }
            }
            
            let sdp_msg = match gst_sdp::SDPMessage::parse_buffer(sdp_text.as_bytes()) {
                Ok(m) => m,
                Err(e) => {
                    warn!(" Viewer {} sent an unparseable SDP answer: {:?}; ignoring", viewer_id, e);
                    return Ok(());
                }
            };
            let answer = gst_webrtc::WebRTCSessionDescription::new(
                gst_webrtc::WebRTCSDPType::Answer,
                sdp_msg,
            );
            
            let promise = gst::Promise::new();
            webrtcbin.emit_by_name::<()>("set-remote-description", &[&answer, &promise]);
            
            info!(" SDP answer processed for viewer: {}", viewer_id);
        }
        
        Ok(())
    }

    /// Apply viewer-side decode/render telemetry reported by the headless
    /// viewer agent into the per-viewer ConnectionMonitor.
    pub fn apply_viewer_telemetry(
        &self,
        viewer_id: &str,
        frames_decoded: u64,
        decode_fps: f64,
        first_decoded_ms: Option<u64>,
    ) -> Result<()> {
        let viewers = self.viewers.lock()
            .map_err(|_| anyhow!("viewers lock poisoned"))?;
        if let Some(peer) = viewers.get(viewer_id) {
            if let Some(monitor) = &peer.monitor {
                if let Ok(mut m) = monitor.lock() {
                    m.on_viewer_telemetry(frames_decoded, decode_fps, first_decoded_ms);
                }
            }
        }
        Ok(())
    }

    /// Handle ICE candidate from viewer
    pub fn handle_viewer_ice_candidate(&self, viewer_id: &str, ice: &serde_json::Value) -> Result<()> {
        let viewers = self.viewers.lock()
            .expect("Failed to acquire viewers lock for ICE candidate");
        let viewer = viewers.get(viewer_id)
            .ok_or_else(|| anyhow!("Viewer {} not found", viewer_id))?;
            
        let webrtcbin = viewer.webrtcbin.as_ref()
            .ok_or_else(|| anyhow!("WebRTC bin not found for viewer {}", viewer_id))?;
        
        let candidate = match ice["candidate"].as_str() {
            Some(c) if !c.is_empty() => c,
            _ => {
                warn!(" Viewer {} sent an ICE candidate with missing/empty candidate; ignoring", viewer_id);
                return Ok(());
            }
        };
        // Browsers omit sdpMLineIndex for end-of-candidates; default to 0.
        let mline_index = ice["sdpMLineIndex"].as_u64().unwrap_or(0) as u32;

        if let Some(monitor) = &viewer.monitor {
            if let Ok(mut m) = monitor.lock() {
                m.on_remote_candidate(candidate);
                m.on_viewer_trickle_candidate_received();
            }
        }
        
        webrtcbin.emit_by_name::<()>("add-ice-candidate", &[&mline_index, &candidate]);
        
        debug!(" ICE candidate added for viewer: {}", viewer_id);
        
        Ok(())
    }
}