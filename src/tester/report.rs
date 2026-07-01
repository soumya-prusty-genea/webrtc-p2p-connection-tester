use std::fmt;

// ─── Overall verdict ─────────────────────────────────────────────────────────

/// Single machine-readable verdict for a gateway->viewer connection test.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionVerdict {
    /// Media was proven to flow end-to-end (egress confirmed and/or viewer decoded frames).
    Pass,
    /// Connection succeeded but quality is impaired or end-to-end delivery is unconfirmed.
    Degraded,
    /// Connection failed before media could be delivered.
    Fail,
}

impl fmt::Display for ConnectionVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionVerdict::Pass => write!(f, "PASS"),
            ConnectionVerdict::Degraded => write!(f, "DEGRADED"),
            ConnectionVerdict::Fail => write!(f, "FAIL"),
        }
    }
}

// ─── Sender egress (proof media left the box and the remote acknowledged it) ──

#[derive(Debug, Clone, Default)]
pub struct EgressStats {
    /// RTP packets the gateway has handed to the network (outbound-rtp.packets-sent).
    pub packets_sent: u64,
    /// Packets the remote peer reported receiving (remote-inbound-rtp.packets-received).
    pub remote_packets_received: u64,
    /// Remote-reported fraction lost (0.0..1.0) from RTCP receiver reports.
    pub fraction_lost: Option<f64>,
    pub nack_count: u64,
    pub pli_count: u64,
    pub fir_count: u64,
    /// Seconds since the last fresh RTCP feedback from the remote peer.
    pub last_remote_feedback_age_secs: Option<u64>,
    /// True when packets-sent advanced in the most recent observation window.
    pub egress_active: bool,
}

// ─── Viewer-side report (populated by the headless viewer agent) ──────────────

#[derive(Debug, Clone, Default)]
pub struct ViewerSideReport {
    pub frames_decoded: u64,
    pub decode_fps: f64,
    pub first_decoded_ms: Option<u64>,
    /// True when the viewer confirmed it decoded/rendered at least one frame.
    pub displayed: bool,
}

// ─── Test Status ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum TestStatus {
    Pass,
    Warn,
    Fail,
    Unknown,
}

impl fmt::Display for TestStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestStatus::Pass    => write!(f, "PASS"),
            TestStatus::Warn    => write!(f, "WARN"),
            TestStatus::Fail    => write!(f, "FAIL"),
            TestStatus::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

// ─── NAT Type ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum NatType {
    OpenInternet,
    FullCone,
    PortRestricted,
    SymmetricNat,
    UdpBlocked,
    Unknown,
}

impl fmt::Display for NatType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NatType::OpenInternet   => write!(f, "Open Internet (no NAT)"),
            NatType::FullCone       => write!(f, "Full Cone NAT"),
            NatType::PortRestricted => write!(f, "Port-Restricted Cone NAT"),
            NatType::SymmetricNat   => write!(f, "Symmetric NAT"),
            NatType::UdpBlocked     => write!(f, "UDP Blocked"),
            NatType::Unknown        => write!(f, "Unknown"),
        }
    }
}

// ─── ICE Candidate ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum IceCandidateType {
    Host,
    ServerReflexive,
    Relay,
    Unknown,
}

impl fmt::Display for IceCandidateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IceCandidateType::Host            => write!(f, "host"),
            IceCandidateType::ServerReflexive => write!(f, "srflx"),
            IceCandidateType::Relay           => write!(f, "relay"),
            IceCandidateType::Unknown         => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IceCandidateInfo {
    pub candidate_type: IceCandidateType,
    pub protocol: String,
    pub address: String,
    pub port: u16,
}

impl IceCandidateInfo {
    /// Parse from an SDP candidate string like:
    /// `candidate:... UDP 2122252543 192.168.1.1 54321 typ host`
    pub fn parse(candidate_str: &str) -> Option<Self> {
        let parts: Vec<&str> = candidate_str.split_whitespace().collect();
        // SDP candidate: foundation component protocol priority address port typ type ...
        if parts.len() < 8 {
            return None;
        }
        let protocol = parts[2].to_lowercase();
        let address  = parts[4].to_string();
        let port     = parts[5].parse::<u16>().unwrap_or(0);
        let typ_idx  = parts.iter().position(|&s| s == "typ")?;
        let ctype_str = parts.get(typ_idx + 1)?;
        let candidate_type = match *ctype_str {
            "host"  => IceCandidateType::Host,
            "srflx" => IceCandidateType::ServerReflexive,
            "relay" => IceCandidateType::Relay,
            _       => IceCandidateType::Unknown,
        };
        Some(IceCandidateInfo { candidate_type, protocol, address, port })
    }
}

// ─── ICE / DTLS Outcomes ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum IceConnectionOutcome {
    Connected,
    Completed,
    Failed,
    Disconnected,
    InProgress,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DtlsOutcome {
    InProgress,
    Connected,
    Failed,
    Timeout,
    NotStarted,
}

impl fmt::Display for DtlsOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DtlsOutcome::InProgress => write!(f, "InProgress"),
            DtlsOutcome::Connected => write!(f, "Connected"),
            DtlsOutcome::Failed => write!(f, "Failed"),
            DtlsOutcome::Timeout => write!(f, "Timeout"),
            DtlsOutcome::NotStarted => write!(f, "NotStarted"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct IceCandidateSummary {
    pub host: usize,
    pub srflx: usize,
    pub relay: usize,
    pub unknown: usize,
}

impl IceCandidateSummary {
    pub fn total(&self) -> usize {
        self.host + self.srflx + self.relay + self.unknown
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IcePairState {
    Gathering,
    Checking,
    Nominated,
    Connected,
    Completed,
    Failed,
    Disconnected,
}

impl fmt::Display for IcePairState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IcePairState::Gathering => write!(f, "Gathering"),
            IcePairState::Checking => write!(f, "Checking"),
            IcePairState::Nominated => write!(f, "Nominated"),
            IcePairState::Connected => write!(f, "Connected"),
            IcePairState::Completed => write!(f, "Completed"),
            IcePairState::Failed => write!(f, "Failed"),
            IcePairState::Disconnected => write!(f, "Disconnected"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CandidatePairTransition {
    pub state: IcePairState,
    pub at_ms: u64,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CandidatePairTelemetry {
    pub selected_pair_id: Option<String>,
    pub local_candidate_type: IceCandidateType,
    pub remote_candidate_type: IceCandidateType,
    pub transport: Option<String>,
    pub nomination_role: Option<String>,
    pub current_rtt_ms: Option<f64>,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub consent_requests: u64,
    pub consent_failures: u64,
    pub retransmission_indicators: u64,
    pub transitions: Vec<CandidatePairTransition>,
}

#[derive(Debug, Clone)]
pub struct CandidatePairAttemptTelemetry {
    pub pair_id: String,
    pub state: Option<String>,
    pub local_candidate_type: IceCandidateType,
    pub remote_candidate_type: IceCandidateType,
    pub nominated: Option<bool>,
    pub writable: Option<bool>,
    pub readable: Option<bool>,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DtlsFailureReason {
    None,
    FingerprintMismatch,
    CertificateInvalid,
    Timeout,
    HandshakeAborted,
    TransportInterrupted,
    Unknown,
}

impl fmt::Display for DtlsFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DtlsFailureReason::None => write!(f, "None"),
            DtlsFailureReason::FingerprintMismatch => write!(f, "FingerprintMismatch"),
            DtlsFailureReason::CertificateInvalid => write!(f, "CertificateInvalid"),
            DtlsFailureReason::Timeout => write!(f, "Timeout"),
            DtlsFailureReason::HandshakeAborted => write!(f, "HandshakeAborted"),
            DtlsFailureReason::TransportInterrupted => write!(f, "TransportInterrupted"),
            DtlsFailureReason::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DtlsPhaseMarker {
    pub phase: String,
    pub at_ms: u64,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DtlsDiagnostics {
    pub markers: Vec<DtlsPhaseMarker>,
    pub negotiated_cipher: Option<String>,
    pub local_fingerprint: Option<String>,
    pub remote_fingerprint: Option<String>,
    pub fingerprint_match: Option<bool>,
    pub certificate_valid: Option<bool>,
    pub failure_reason: DtlsFailureReason,
    pub failure_evidence: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MediaStageTimeline {
    pub srtp_keys_ready_ms: Option<u64>,
    pub first_rtp_packet_ms: Option<u64>,
    pub first_keyframe_ms: Option<u64>,
    pub first_decoded_frame_ms: Option<u64>,
    pub first_rendered_frame_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct IceGatheringPhaseReport {
    pub total_local_candidates: usize,
    pub host_candidates: usize,
    pub srflx_candidates: usize,
    pub relay_candidates: usize,
    pub stun_responded_with_srflx: bool,
    pub turn_relay_allocated: bool,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CandidateExchangePhaseReport {
    pub gateway_candidates_sent_total: u64,
    pub gateway_candidates_send_failures: u64,
    pub gateway_candidates_delivered: bool,
    pub viewer_trickle_candidates_received: u64,
    pub viewer_sdp_embedded_candidates_received: u64,
    pub viewer_sent_candidates_back: bool,
    pub trickle_ice_working: bool,
    pub signaling_channel_dropped: bool,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IceConnectivityPhaseReport {
    pub candidate_pair_nominated: bool,
    pub nat_type: NatType,
    pub selected_local_candidate_type: IceCandidateType,
    pub selected_remote_candidate_type: IceCandidateType,
    pub ice_failed: bool,
    pub stuck_in_checking: bool,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DtlsHandshakePhaseReport {
    pub handshake_completed: bool,
    pub remote_fingerprint_valid: Option<bool>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MediaFlowPhaseReport {
    pub rtp_packets_observed: u64,
    pub packet_loss_percent: Option<f64>,
    pub jitter_ms: Option<f64>,
    pub bitrate_kbps: Option<f64>,
    pub stream_freeze_or_drop_detected: bool,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RootCauseBranch {
    pub score: u8,
    pub confidence: f32,
    pub summary: String,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RootCauseTree {
    pub network: RootCauseBranch,
    pub nat_turn: RootCauseBranch,
    pub signaling: RootCauseBranch,
    pub ice: RootCauseBranch,
    pub dtls: RootCauseBranch,
    pub srtp_media: RootCauseBranch,
    pub overall: String,
}

// ─── Probe Results ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StunProbeResult {
    pub server: String,
    pub status: TestStatus,
    pub reflexive_address: Option<String>,
    pub reflexive_port: Option<u16>,
    pub rtt_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TurnProbeResult {
    pub server: String,
    pub status: TestStatus,
    pub rtt_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SignalingProbeResult {
    pub url: String,
    pub status: TestStatus,
    pub rtt_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NetworkProbeReport {
    pub stun_results: Vec<StunProbeResult>,
    pub nat_type: NatType,
    pub turn_results: Vec<TurnProbeResult>,
    pub signaling_result: SignalingProbeResult,
}

impl NetworkProbeReport {
    pub fn print_summary(&self) {
        log::info!("══════════════ Network Probe Report ══════════════");
        log::info!("  NAT Type : {}", self.nat_type);
        for s in &self.stun_results {
            log::info!("  STUN [{}] : {} (reflexive={}:{}  rtt={}ms)",
                s.server,
                s.status,
                s.reflexive_address.as_deref().unwrap_or("-"),
                s.reflexive_port.map(|p| p.to_string()).as_deref().unwrap_or("-"),
                s.rtt_ms.map(|r| r.to_string()).as_deref().unwrap_or("-"),
            );
            if let Some(e) = &s.error {
                log::warn!("       error: {}", e);
            }
        }
        for t in &self.turn_results {
            log::info!("  TURN [{}] : {} (rtt={}ms)",
                t.server,
                t.status,
                t.rtt_ms.map(|r| r.to_string()).as_deref().unwrap_or("-"),
            );
            if let Some(e) = &t.error {
                log::warn!("       error: {}", e);
            }
        }
        log::info!("  Signaling [{}] : {} (rtt={}ms)",
            self.signaling_result.url,
            self.signaling_result.status,
            self.signaling_result.rtt_ms.map(|r| r.to_string()).as_deref().unwrap_or("-"),
        );
        log::info!("══════════════════════════════════════════════════");
    }
}

// ─── Per-Connection Diagnostic Report ────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DiagnosisEntry {
    pub severity: TestStatus,
    pub category: String,
    pub message: String,
    pub fix: String,
}

#[derive(Debug, Clone)]
pub struct ConnectionDiagnosticReport {
    pub viewer_id: String,
    pub camera_id: String,
    pub camera_name: String,
    pub room_name: String,

    // Phase timing (milliseconds)
    pub ice_gathering_ms: Option<u64>,
    pub ice_connection_ms: Option<u64>,
    pub dtls_handshake_ms: Option<u64>,
    pub srtp_ready_ms: Option<u64>,
    pub first_frame_ms: Option<u64>,

    // Counts
    pub local_candidate_count: usize,
    pub remote_candidate_count: usize,
    pub local_candidates: IceCandidateSummary,
    pub remote_candidates: IceCandidateSummary,
    pub candidate_path_assessment: String,
    pub dtls_warning_count: u32,
    pub frames_sent: u64,
    pub candidate_pair: Option<CandidatePairTelemetry>,
    pub candidate_pair_attempts: Vec<CandidatePairAttemptTelemetry>,
    pub dtls_details: DtlsDiagnostics,
    pub media_timeline: MediaStageTimeline,
    pub root_cause_tree: RootCauseTree,
    pub phase3: IceGatheringPhaseReport,
    pub phase4: CandidateExchangePhaseReport,
    pub phase5: IceConnectivityPhaseReport,
    pub phase6: DtlsHandshakePhaseReport,
    pub phase7: MediaFlowPhaseReport,

    // Outcomes
    pub session_ended_by_viewer: bool,
    pub ice_outcome: IceConnectionOutcome,
    pub dtls_outcome: DtlsOutcome,

    // Sender egress proof + viewer-side confirmation
    pub egress: EgressStats,
    pub viewer_report: Option<ViewerSideReport>,

    // Diagnosis
    pub entries: Vec<DiagnosisEntry>,

    // Full candidate lists with addresses (for structured terminal reporting)
    pub local_candidate_list: Vec<IceCandidateInfo>,
    pub remote_candidate_list: Vec<IceCandidateInfo>,

    // Full network probe (STUN reflexive ports, NAT evidence, signaling)
    pub network_probe: Option<NetworkProbeReport>,
}

impl ConnectionDiagnosticReport {
    /// True when ICE reached a usable connected/completed state.
    fn ice_ok(&self) -> bool {
        matches!(
            self.ice_outcome,
            IceConnectionOutcome::Connected | IceConnectionOutcome::Completed
        )
    }

    /// Compute a single PASS/DEGRADED/FAIL verdict for this connection.
    ///
    /// Proof of end-to-end delivery is, in order of strength:
    /// 1. The viewer agent confirmed it decoded frames (`viewer_report.displayed`).
    /// 2. The remote peer acknowledged received packets via RTCP
    ///    (`egress.remote_packets_received > 0`).
    /// Local frame counts alone are NOT treated as proof, mirroring the
    /// silent-stall lesson from cloud-live-streamer.
    pub fn verdict(&self) -> ConnectionVerdict {
        if self.session_ended_by_viewer && self.frames_sent > 0 {
            return ConnectionVerdict::Pass;
        }

        let handshake_ok = self.ice_ok() && self.dtls_outcome == DtlsOutcome::Connected;
        if !handshake_ok {
            return ConnectionVerdict::Fail;
        }

        let viewer_displayed = self
            .viewer_report
            .as_ref()
            .map(|v| v.displayed)
            .unwrap_or(false);
        let remote_acked = self.egress.remote_packets_received > 0;
        let high_loss = self
            .egress
            .fraction_lost
            .map(|l| l > 0.10)
            .unwrap_or(false);

        if viewer_displayed || remote_acked {
            if high_loss || self.phase7.stream_freeze_or_drop_detected {
                ConnectionVerdict::Degraded
            } else {
                ConnectionVerdict::Pass
            }
        } else if self.frames_sent > 0 {
            // Media was generated locally but never confirmed by the remote/viewer.
            ConnectionVerdict::Degraded
        } else {
            ConnectionVerdict::Fail
        }
    }

    /// Build a machine-readable JSON document for this connection test.
    pub fn to_json(&self) -> serde_json::Value {
        let verdict = self.verdict();
        serde_json::json!({
            "viewer_id": self.viewer_id,
            "camera_id": self.camera_id,
            "camera_name": self.camera_name,
            "room_name": self.room_name,
            "verdict": verdict.to_string(),
            "timing_ms": {
                "ice_gathering": self.ice_gathering_ms,
                "ice_connection": self.ice_connection_ms,
                "dtls_handshake": self.dtls_handshake_ms,
                "srtp_ready": self.srtp_ready_ms,
                "first_frame": self.first_frame_ms,
            },
            "ice": {
                "outcome": format!("{:?}", self.ice_outcome),
                "local_candidates": {
                    "host": self.local_candidates.host,
                    "srflx": self.local_candidates.srflx,
                    "relay": self.local_candidates.relay,
                    "total": self.local_candidate_count,
                },
                "remote_candidates_total": self.remote_candidate_count,
                "path_assessment": self.candidate_path_assessment,
                "nat_type": self.phase5.nat_type.to_string(),
            },
            "dtls": {
                "outcome": self.dtls_outcome.to_string(),
                "warning_count": self.dtls_warning_count,
                "failure_reason": self.dtls_details.failure_reason.to_string(),
            },
            "egress": {
                "packets_sent": self.egress.packets_sent,
                "remote_packets_received": self.egress.remote_packets_received,
                "fraction_lost": self.egress.fraction_lost,
                "nack_count": self.egress.nack_count,
                "pli_count": self.egress.pli_count,
                "fir_count": self.egress.fir_count,
                "last_remote_feedback_age_secs": self.egress.last_remote_feedback_age_secs,
                "egress_active": self.egress.egress_active,
            },
            "viewer": self.viewer_report.as_ref().map(|v| serde_json::json!({
                "frames_decoded": v.frames_decoded,
                "decode_fps": v.decode_fps,
                "first_decoded_ms": v.first_decoded_ms,
                "displayed": v.displayed,
            })),
            "media": {
                "frames_sent_local": self.frames_sent,
                "rtp_packets_observed": self.phase7.rtp_packets_observed,
                "packet_loss_percent": self.phase7.packet_loss_percent,
                "jitter_ms": self.phase7.jitter_ms,
                "bitrate_kbps": self.phase7.bitrate_kbps,
                "freeze_or_drop_detected": self.phase7.stream_freeze_or_drop_detected,
            },
            "root_cause": self.root_cause_tree.overall,
            "session_ended_by_viewer": self.session_ended_by_viewer,
            "diagnosis": self.entries.iter().map(|e| serde_json::json!({
                "severity": e.severity.to_string(),
                "category": e.category,
                "message": e.message,
                "fix": e.fix,
            })).collect::<Vec<_>>(),
        })
    }

    /// Emit the JSON report: always logs a single-line JSON record, and when
    /// `WEBRTC_TEST_REPORT_DIR` is set, also writes `<dir>/<room>_<viewer>.json`.
    pub fn emit_json_report(&self) {
        let json = self.to_json();
        match serde_json::to_string(&json) {
            Ok(line) => log::info!(" [test_report] {}", line),
            Err(e) => log::warn!(" [test_report] failed to serialize report: {}", e),
        }

        if let Ok(dir) = std::env::var("WEBRTC_TEST_REPORT_DIR") {
            if !dir.trim().is_empty() {
                let safe_room = self.room_name.replace(['/', ' '], "_");
                let safe_viewer = self.viewer_id.replace(['/', ' '], "_");
                let path = std::path::Path::new(&dir)
                    .join(format!("{}_{}.json", safe_room, safe_viewer));
                if let Err(e) = std::fs::create_dir_all(&dir) {
                    log::warn!(" [test_report] cannot create {}: {}", dir, e);
                    return;
                }
                match serde_json::to_string_pretty(&json) {
                    Ok(pretty) => {
                        if let Err(e) = std::fs::write(&path, pretty) {
                            log::warn!(" [test_report] cannot write {}: {}", path.display(), e);
                        } else {
                            log::info!(" [test_report] wrote {}", path.display());
                        }
                    }
                    Err(e) => log::warn!(" [test_report] serialize error: {}", e),
                }
            }
        }
    }

    pub fn print(&self) {
        let verdict = self.verdict();
        let (vicon, vtext) = match &verdict {
            ConnectionVerdict::Pass     => ("✓", "PASS"),
            ConnectionVerdict::Degraded => ("~", "DEGRADED"),
            ConnectionVerdict::Fail     => ("✗", "FAIL"),
        };
        let thin = "─".repeat(66);

        // ── Header ───────────────────────────────────────────────────────────
        log::info!("╔══════════════════════ WebRTC Connection Test ══════════════════════╗");
        log::info!("║  Camera  : {} ({})", self.camera_name, self.camera_id);
        log::info!("║  Room    : {}   Viewer : {}", self.room_name, self.viewer_id);
        log::info!("╠══ VERDICT ═════════════════════════════════════════════════════════╣");
        log::info!("║  {} {}", vicon, vtext);

        // ── Network Probe ─────────────────────────────────────────────────────
        log::info!("╠══ Network Probe ════════════════════════════════════════════════════╣");
        if let Some(probe) = &self.network_probe {
            log::info!("║  NAT Type : {}", probe.nat_type);

            for stun in &probe.stun_results {
                let icon = if stun.status == TestStatus::Pass { "✓" } else { "✗" };
                let reflexive = match (&stun.reflexive_address, stun.reflexive_port) {
                    (Some(ip), Some(port)) => format!("{}:{}", ip, port),
                    _ => "-".to_string(),
                };
                let rtt = stun.rtt_ms.map(|r| format!("{}ms", r)).unwrap_or_else(|| "timeout".to_string());
                log::info!("║  STUN {}  [{}]  reflexive={}  rtt={}", icon, stun.server, reflexive, rtt);
                if let Some(err) = &stun.error {
                    log::info!("║       → {}", err);
                }
            }

            // NAT evidence: show what the same-socket comparison revealed
            if probe.stun_results.len() >= 2 {
                let s1 = &probe.stun_results[0];
                let s2 = &probe.stun_results[1];
                match (s1.reflexive_port, s2.reflexive_port) {
                    (Some(p1), Some(p2)) if p1 == p2 => {
                        log::info!("║  ↳ same port ({}) on both servers → endpoint-independent (Cone) mapping", p1);
                    }
                    (Some(p1), Some(p2)) => {
                        log::info!("║  ↳ ports differ: {} vs {} → port-dependent mapping → Symmetric NAT!", p1, p2);
                        log::warn!("║  ⚠  Symmetric NAT: direct WebRTC requires TURN relay to traverse this!");
                    }
                    _ => {}
                }
                match (&s1.reflexive_address, &s2.reflexive_address) {
                    (Some(ip1), Some(ip2)) if ip1 != ip2 => {
                        log::warn!("║  ⚠  Public IP differs per STUN server: {} vs {} — multi-homed/CGNAT?", ip1, ip2);
                    }
                    _ => {}
                }
            }

            // Signaling
            let sig_icon = if probe.signaling_result.status == TestStatus::Pass { "✓" } else { "✗" };
            let sig_rtt = probe.signaling_result.rtt_ms.map(|r| format!("{}ms", r)).unwrap_or_else(|| "timeout".to_string());
            log::info!("║  Signaling {} [{}]  rtt={}", sig_icon, probe.signaling_result.url, sig_rtt);
            if let Some(err) = &probe.signaling_result.error {
                log::info!("║       → {}", err);
            }

            // TURN servers
            for turn in &probe.turn_results {
                let t_icon = match turn.status { TestStatus::Pass => "✓", TestStatus::Warn => "~", _ => "✗" };
                let t_rtt = turn.rtt_ms.map(|r| format!("{}ms", r)).unwrap_or_else(|| "timeout".to_string());
                log::info!("║  TURN {}  [{}]  rtt={}", t_icon, turn.server, t_rtt);
                if let Some(err) = &turn.error {
                    log::info!("║       → {}", err);
                }
            }
            if probe.turn_results.is_empty() {
                log::info!("║  TURN  ─  (not configured — set WEBRTC_TURN_SERVERS for relay fallback)");
            }
        } else {
            log::info!("║  (network probe not available — ran before viewer connected?)");
        }

        // ── ICE Gathering ─────────────────────────────────────────────────────
        let gather_ms = self.ice_gathering_ms.map(|ms| format!("{}ms", ms)).unwrap_or_else(|| "?".to_string());
        log::info!("╠══ ICE Gathering  [{}]  ═══════════════════════════════════════════╣", gather_ms);

        log::info!("║  Local candidates : {}", self.local_candidate_list.len());
        for c in &self.local_candidate_list {
            log::info!("║    {:<8}  {:<4}  {}:{}", c.candidate_type, c.protocol.to_uppercase(), c.address, c.port);
        }
        if self.local_candidate_list.is_empty() {
            log::warn!("║  ✗ No local candidates gathered — verify WEBRTC_STUN_SERVER and network interface");
        } else {
            if self.local_candidates.srflx == 0 {
                log::warn!("║  ⚠  No server-reflexive (srflx) — STUN may be unreachable or UDP is filtered");
            }
            if self.local_candidates.relay == 0 {
                log::info!("║  ─  No relay candidates — TURN not configured (set WEBRTC_TURN_SERVERS)");
            }
        }

        log::info!("║  Remote candidates : {}", self.remote_candidate_list.len());
        for c in &self.remote_candidate_list {
            log::info!("║    {:<8}  {:<4}  {}:{}", c.candidate_type, c.protocol.to_uppercase(), c.address, c.port);
        }
        if self.remote_candidate_list.is_empty() {
            log::info!("║    (none observed locally — remote may use non-trickle or arrived after ICE)");
        }

        // ── ICE Connectivity ──────────────────────────────────────────────────
        let conn_ms = self.ice_connection_ms.map(|ms| format!("{}ms", ms)).unwrap_or_else(|| "?".to_string());
        let ice_outcome_str = format!("{:?}", self.ice_outcome);
        log::info!("╠══ ICE Connectivity  [{}]  [{}]  ══════════════════════════════╣", conn_ms, ice_outcome_str);

        if self.candidate_pair_attempts.is_empty() {
            log::info!("║  No candidate pair telemetry collected (webrtcbin get-stats not yet called?)");
        } else {
            let succeeded = self.candidate_pair_attempts.iter().filter(|p| {
                let s = p.state.as_deref().unwrap_or("").to_ascii_lowercase();
                s == "succeeded" || p.nominated == Some(true)
            }).count();
            let failed = self.candidate_pair_attempts.iter().filter(|p| {
                p.state.as_deref().unwrap_or("").eq_ignore_ascii_case("failed")
            }).count();
            let other = self.candidate_pair_attempts.len().saturating_sub(succeeded + failed);
            log::info!("║  {} pairs tried · {} succeeded · {} failed · {} other",
                self.candidate_pair_attempts.len(), succeeded, failed, other);
            log::info!("║  {}", thin);
            log::info!("║   #  State           Local     Remote    Nom   Traffic");
            log::info!("║  {}", thin);
            for (i, pair) in self.candidate_pair_attempts.iter().enumerate() {
                let state_str = pair.state.as_deref().unwrap_or("unknown");
                let state_icon = match state_str.to_ascii_lowercase().as_str() {
                    "succeeded"   => "✓",
                    "failed"      => "✗",
                    "in-progress" => "⋯",
                    _             => " ",
                };
                let nom = if pair.nominated == Some(true) { "✓" } else { "─" };
                let traffic = if pair.bytes_sent > 0 || pair.bytes_received > 0 {
                    format!("↑{}  ↓{}", fmt_bytes(pair.bytes_sent), fmt_bytes(pair.bytes_received))
                } else {
                    String::new()
                };
                log::info!("║  {:>2}  {} {:<12}  {:<9} {:<9} {}     {}",
                    i + 1,
                    state_icon,
                    state_str,
                    pair.local_candidate_type.to_string(),
                    pair.remote_candidate_type.to_string(),
                    nom,
                    traffic,
                );
            }
            log::info!("║  {}", thin);
        }

        if let Some(pair) = &self.candidate_pair {
            let rtt = pair.current_rtt_ms.map(|r| format!("{:.1}ms", r)).unwrap_or_else(|| "-".to_string());
            log::info!("║  Selected : local={}  remote={}  transport={}  rtt={}",
                pair.local_candidate_type,
                pair.remote_candidate_type,
                pair.transport.as_deref().unwrap_or("-"),
                rtt,
            );
            if pair.bytes_sent > 0 || pair.bytes_received > 0 {
                log::info!("║    Traffic: ↑{}  ↓{}  pkts: ↑{} ↓{}",
                    fmt_bytes(pair.bytes_sent), fmt_bytes(pair.bytes_received),
                    pair.packets_sent, pair.packets_received);
            }
            if pair.consent_failures > 0 {
                log::warn!("║  ⚠  Consent failures: {}/{}", pair.consent_failures, pair.consent_requests);
            }
            if pair.retransmission_indicators > 0 {
                log::warn!("║  ⚠  Retransmission events: {}", pair.retransmission_indicators);
            }
        } else if matches!(self.ice_outcome, IceConnectionOutcome::Connected | IceConnectionOutcome::Completed) {
            log::info!("║  Selected : (pair telemetry not yet collected)");
        }

        if let Some(reason) = &self.phase5.failure_reason {
            log::warn!("║  ✗ ICE failure: {}", reason);
        }

        // ── DTLS ──────────────────────────────────────────────────────────────
        let dtls_ms = self.dtls_handshake_ms.map(|ms| format!("{}ms", ms)).unwrap_or_else(|| "?".to_string());
        log::info!("╠══ DTLS  [{}]  ══════════════════════════════════════════════════════╣", dtls_ms);
        let dtls_icon = if self.dtls_outcome == DtlsOutcome::Connected { "✓" } else { "✗" };
        log::info!("║  {} DTLS outcome: {}", dtls_icon, self.dtls_outcome);
        if let Some(cipher) = &self.dtls_details.negotiated_cipher {
            log::info!("║    Cipher : {}", cipher);
        }
        if self.dtls_warning_count > 0 {
            log::warn!("║  ⚠  {} DTLS runtime warnings during session", self.dtls_warning_count);
        }
        if let Some(reason) = &self.phase6.failure_reason {
            log::warn!("║  ✗ DTLS failure: {}", reason);
        }

        // ── Media ─────────────────────────────────────────────────────────────
        log::info!("╠══ Media ════════════════════════════════════════════════════════════╣");
        let tl = &self.media_timeline;
        log::info!("║  Timeline : keys={}ms  rtp={}ms  keyframe={}ms  decoded={}ms",
            tl.srtp_keys_ready_ms.unwrap_or(0),
            tl.first_rtp_packet_ms.unwrap_or(0),
            tl.first_keyframe_ms.unwrap_or(0),
            tl.first_decoded_frame_ms.unwrap_or(0));
        let e = &self.egress;
        let loss_str = e.fraction_lost.map(|l| format!("{:.1}%", l * 100.0)).unwrap_or_else(|| "-".to_string());
        log::info!("║  Egress   : sent={}  remote_acked={}  loss={}  nack={}  pli={}  active={}",
            e.packets_sent, e.remote_packets_received, loss_str, e.nack_count, e.pli_count, e.egress_active);
        match &self.viewer_report {
            Some(v) => {
                let disp = if v.displayed { "✓ displayed" } else { "not displayed" };
                log::info!("║  Viewer   : frames={}  fps={:.1}  first={}ms  {}",
                    v.frames_decoded, v.decode_fps, v.first_decoded_ms.unwrap_or(0), disp);
            }
            None => log::info!("║  Viewer   : no viewer-agent telemetry received"),
        }
        if let Some(reason) = &self.phase7.failure_reason {
            log::warn!("║  ⚠  Media: {}", reason);
        }

        // ── Diagnosis ─────────────────────────────────────────────────────────
        log::info!("╠══ Diagnosis ════════════════════════════════════════════════════════╣");
        if self.entries.is_empty() {
            log::info!("║  ✓ No issues detected");
        } else {
            for entry in &self.entries {
                let icon = match entry.severity { TestStatus::Fail => "✗", TestStatus::Warn => "⚠", _ => "ℹ" };
                log::info!("║  {} [{}] {}", icon, entry.category, entry.message);
                log::info!("║      Fix : {}", entry.fix);
            }
        }
        log::info!("║  Root cause : {}", self.root_cause_tree.overall);

        // ── Footer ────────────────────────────────────────────────────────────
        log::info!("╠══ Outcome ══════════════════════════════════════════════════════════╣");
        log::info!("║  ICE   : {:?}", self.ice_outcome);
        log::info!("║  DTLS  : {}", self.dtls_outcome);
        log::info!("║  Path  : {}", self.candidate_path_assessment);
        if self.session_ended_by_viewer {
            log::info!("║  Session ended by viewer (clean teardown)");
        }
        log::info!("╠══ VERDICT ═════════════════════════════════════════════════════════╣");
        log::info!("║  {} {}", vicon, vtext);
        log::info!("╚════════════════════════════════════════════════════════════════════╝");
    }
}

/// Format a byte count as a human-readable string (B / KB / MB).
fn fmt_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}
