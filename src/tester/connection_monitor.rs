use std::time::Instant;

use crate::metrics;
use crate::tester::report::{
    CandidateExchangePhaseReport, CandidatePairAttemptTelemetry, CandidatePairTelemetry,
    CandidatePairTransition,
    ConnectionDiagnosticReport, DiagnosisEntry, DtlsDiagnostics, DtlsFailureReason,
    DtlsHandshakePhaseReport, DtlsOutcome, DtlsPhaseMarker, EgressStats, IceCandidateInfo,
    IceCandidateSummary, IceCandidateType, IceConnectionOutcome, IceConnectivityPhaseReport,
    IceGatheringPhaseReport, IcePairState, MediaFlowPhaseReport, MediaStageTimeline, NatType,
    NetworkProbeReport, RootCauseBranch, RootCauseTree, TestStatus, ViewerSideReport,
};

/// Tracks all observable phases of a single WebRTC connection lifecycle.
pub struct ConnectionMonitor {
    // Identity
    viewer_id: String,
    camera_id: String,
    camera_name: String,
    room_name: String,

    // Timestamps
    start: Instant,
    ice_gathering_start: Option<Instant>,
    ice_gathering_end: Option<Instant>,
    ice_checking_start: Option<Instant>,
    ice_connected_at: Option<Instant>,
    dtls_start: Option<Instant>,
    dtls_done: Option<Instant>,
    srtp_ready_at: Option<Instant>,
    first_rtp_packet_at: Option<Instant>,
    first_keyframe_at: Option<Instant>,
    first_decoded_frame_at: Option<Instant>,
    first_rendered_frame_at: Option<Instant>,
    first_frame_at: Option<Instant>,
    last_frame_sent_at: Option<Instant>,

    // Counts / outcomes
    local_candidates: Vec<IceCandidateInfo>,
    remote_candidates: Vec<IceCandidateInfo>,
    ice_outcome: IceConnectionOutcome,
    dtls_outcome: DtlsOutcome,
    dtls_warning_count: u32,
    frames_sent: u64,
    candidate_pair: Option<CandidatePairTelemetry>,
    dtls_markers: Vec<DtlsPhaseMarker>,
    negotiated_cipher: Option<String>,
    local_fingerprint: Option<String>,
    remote_fingerprint: Option<String>,
    fingerprint_match: Option<bool>,
    certificate_valid: Option<bool>,
    dtls_failure_reason: DtlsFailureReason,
    dtls_failure_evidence: Option<String>,
    early_report_printed: bool,
    gateway_candidates_sent_total: u64,
    gateway_candidates_send_failures: u64,
    viewer_trickle_candidates_received: u64,
    viewer_sdp_embedded_candidates_received: u64,
    signaling_channel_dropped: bool,
    media_packet_loss_percent: Option<f64>,
    media_jitter_ms: Option<f64>,
    media_packets_observed: u64,
    candidate_pair_attempts: Vec<CandidatePairAttemptTelemetry>,
    session_ended_by_viewer: bool,

    // Sender egress proof (from webrtcbin get-stats: outbound-rtp + remote-inbound-rtp).
    egress_packets_sent: u64,
    egress_remote_packets_received: u64,
    egress_fraction_lost: Option<f64>,
    egress_nack_count: u64,
    egress_pli_count: u64,
    egress_fir_count: u64,
    egress_last_feedback_at: Option<Instant>,
    egress_active: bool,

    // Viewer-side telemetry reported by the headless viewer agent over signaling.
    viewer_frames_decoded: u64,
    viewer_decode_fps: f64,
    viewer_first_decoded_ms: Option<u64>,
    viewer_displayed: bool,

    // Per-viewer network probe results (populated async after viewer connects)
    pub network_probe: Option<NetworkProbeReport>,
}

impl ConnectionMonitor {
    pub fn new(
        viewer_id: &str,
        camera_id: &str,
        camera_name: &str,
        room_name: &str,
    ) -> Self {
        Self {
            viewer_id: viewer_id.to_string(),
            camera_id: camera_id.to_string(),
            camera_name: camera_name.to_string(),
            room_name: room_name.to_string(),
            start: Instant::now(),
            ice_gathering_start: Some(Instant::now()),
            ice_gathering_end: None,
            ice_checking_start: None,
            ice_connected_at: None,
            dtls_start: None,
            dtls_done: None,
            srtp_ready_at: None,
            first_rtp_packet_at: None,
            first_keyframe_at: None,
            first_decoded_frame_at: None,
            first_rendered_frame_at: None,
            first_frame_at: None,
            last_frame_sent_at: None,
            local_candidates: Vec::new(),
            remote_candidates: Vec::new(),
            ice_outcome: IceConnectionOutcome::InProgress,
            dtls_outcome: DtlsOutcome::NotStarted,
            dtls_warning_count: 0,
            frames_sent: 0,
            candidate_pair: None,
            dtls_markers: Vec::new(),
            negotiated_cipher: None,
            local_fingerprint: None,
            remote_fingerprint: None,
            fingerprint_match: None,
            certificate_valid: None,
            dtls_failure_reason: DtlsFailureReason::None,
            dtls_failure_evidence: None,
            early_report_printed: false,
            gateway_candidates_sent_total: 0,
            gateway_candidates_send_failures: 0,
            viewer_trickle_candidates_received: 0,
            viewer_sdp_embedded_candidates_received: 0,
            signaling_channel_dropped: false,
            media_packet_loss_percent: None,
            media_jitter_ms: None,
            media_packets_observed: 0,
            candidate_pair_attempts: Vec::new(),
            session_ended_by_viewer: false,
            egress_packets_sent: 0,
            egress_remote_packets_received: 0,
            egress_fraction_lost: None,
            egress_nack_count: 0,
            egress_pli_count: 0,
            egress_fir_count: 0,
            egress_last_feedback_at: None,
            egress_active: false,
            viewer_frames_decoded: 0,
            viewer_decode_fps: 0.0,
            viewer_first_decoded_ms: None,
            viewer_displayed: false,
            network_probe: None,
        }
    }

    // ─── Sender egress proof ──────────────────────────────────────────────────

    /// Record a sender-side egress sample derived from `webrtcbin get-stats`.
    /// `packets_sent` is the cumulative outbound-rtp count; remote_* fields come
    /// from the remote peer's RTCP receiver reports (remote-inbound-rtp).
    pub fn on_egress_stats(
        &mut self,
        packets_sent: u64,
        remote_packets_received: u64,
        fraction_lost: Option<f64>,
        nack_count: u64,
        pli_count: u64,
        fir_count: u64,
    ) {
        // egress_active reflects whether packets advanced since the last sample.
        self.egress_active = packets_sent > self.egress_packets_sent;
        if packets_sent > 0 {
            self.egress_packets_sent = packets_sent;
        }

        // Any growth in remote-acknowledged packets is proof of fresh feedback.
        if remote_packets_received > self.egress_remote_packets_received {
            self.egress_last_feedback_at = Some(Instant::now());
        }
        self.egress_remote_packets_received = remote_packets_received
            .max(self.egress_remote_packets_received);

        if fraction_lost.is_some() {
            self.egress_fraction_lost = fraction_lost;
        }
        self.egress_nack_count = nack_count.max(self.egress_nack_count);
        self.egress_pli_count = pli_count.max(self.egress_pli_count);
        self.egress_fir_count = fir_count.max(self.egress_fir_count);
    }

    // ─── Viewer-side telemetry (from the headless viewer agent) ───────────────

    pub fn on_viewer_telemetry(
        &mut self,
        frames_decoded: u64,
        decode_fps: f64,
        first_decoded_ms: Option<u64>,
    ) {
        self.viewer_frames_decoded = frames_decoded.max(self.viewer_frames_decoded);
        self.viewer_decode_fps = decode_fps;
        if self.viewer_first_decoded_ms.is_none() {
            self.viewer_first_decoded_ms = first_decoded_ms;
        }
        if self.viewer_frames_decoded > 0 {
            self.viewer_displayed = true;
            // Reflect viewer-confirmed decode in the media timeline.
            self.on_first_decoded_frame();
            self.on_first_rendered_frame();
        }
    }

    fn elapsed_ms(&self, t: Option<Instant>) -> Option<u64> {
        t.map(|i| i.duration_since(self.start).as_millis() as u64)
    }

    fn push_pair_transition(&mut self, state: IcePairState, reason: Option<String>) {
        if self.candidate_pair.is_none() {
            self.candidate_pair = Some(CandidatePairTelemetry {
                selected_pair_id: None,
                local_candidate_type: IceCandidateType::Unknown,
                remote_candidate_type: IceCandidateType::Unknown,
                transport: None,
                nomination_role: None,
                current_rtt_ms: None,
                bytes_sent: 0,
                bytes_received: 0,
                packets_sent: 0,
                packets_received: 0,
                consent_requests: 0,
                consent_failures: 0,
                retransmission_indicators: 0,
                transitions: Vec::new(),
            });
        }
        let at_ms = self.elapsed_ms(Some(Instant::now())).unwrap_or(0);
        if let Some(pair) = &mut self.candidate_pair {
            pair.transitions.push(CandidatePairTransition {
                state,
                at_ms,
                reason,
            });
        }
    }

    // ─── ICE gathering ───────────────────────────────────────────────────────

    pub fn on_local_candidate(&mut self, candidate_str: &str) {
        if let Some(info) = IceCandidateInfo::parse(candidate_str) {
            self.local_candidates.push(info);
        }
    }

    pub fn on_remote_candidate(&mut self, candidate_str: &str) {
        if let Some(info) = IceCandidateInfo::parse(candidate_str) {
            self.remote_candidates.push(info);
        }
    }

    pub fn on_ice_gathering_complete(&mut self) {
        if self.ice_gathering_end.is_none() {
            self.ice_gathering_end = Some(Instant::now());
        }
        self.push_pair_transition(IcePairState::Gathering, Some("gathering complete".to_string()));
    }

    // ─── ICE connection state ─────────────────────────────────────────────────

    pub fn on_ice_checking(&mut self) {
        if self.ice_checking_start.is_none() {
            self.ice_checking_start = Some(Instant::now());
        }
        self.push_pair_transition(IcePairState::Checking, None);
    }

    pub fn on_ice_connected(&mut self) {
        if self.ice_connected_at.is_none() {
            self.ice_connected_at = Some(Instant::now());
        }
        self.ice_outcome = IceConnectionOutcome::Connected;
        // DTLS begins right after ICE connects
        if self.dtls_start.is_none() {
            self.dtls_start = Some(Instant::now());
            self.dtls_outcome = DtlsOutcome::InProgress;
        }
        self.push_pair_transition(IcePairState::Connected, None);
    }

    pub fn on_ice_completed(&mut self) {
        if self.ice_connected_at.is_none() {
            self.ice_connected_at = Some(Instant::now());
        }
        self.ice_outcome = IceConnectionOutcome::Completed;
        if self.dtls_start.is_none() {
            self.dtls_start = Some(Instant::now());
            self.dtls_outcome = DtlsOutcome::InProgress;
        }
        self.push_pair_transition(IcePairState::Completed, None);
    }

    pub fn on_ice_failed(&mut self) {
        self.ice_outcome = IceConnectionOutcome::Failed;
        self.push_pair_transition(IcePairState::Failed, None);
    }

    pub fn on_ice_disconnected(&mut self) {
        self.ice_outcome = IceConnectionOutcome::Disconnected;
        self.push_pair_transition(IcePairState::Disconnected, None);
    }

    // ─── DTLS ─────────────────────────────────────────────────────────────────

    pub fn on_dtls_connecting(&mut self) {
        if self.dtls_start.is_none() {
            self.dtls_start = Some(Instant::now());
        }
        self.dtls_outcome = DtlsOutcome::InProgress;
        self.dtls_markers.push(DtlsPhaseMarker {
            phase: "connecting".to_string(),
            at_ms: self.elapsed_ms(Some(Instant::now())).unwrap_or(0),
            detail: None,
        });
    }

    pub fn on_dtls_connected(&mut self) {
        if self.dtls_done.is_none() {
            self.dtls_done = Some(Instant::now());
        }
        self.dtls_outcome = DtlsOutcome::Connected;
        self.dtls_markers.push(DtlsPhaseMarker {
            phase: "connected".to_string(),
            at_ms: self.elapsed_ms(Some(Instant::now())).unwrap_or(0),
            detail: None,
        });
    }

    pub fn on_dtls_failed(&mut self) {
        if self.dtls_done.is_none() {
            self.dtls_done = Some(Instant::now());
        }
        self.dtls_outcome = DtlsOutcome::Failed;
        if self.dtls_failure_reason == DtlsFailureReason::None {
            self.dtls_failure_reason = DtlsFailureReason::Unknown;
        }
        self.dtls_markers.push(DtlsPhaseMarker {
            phase: "failed".to_string(),
            at_ms: self.elapsed_ms(Some(Instant::now())).unwrap_or(0),
            detail: self.dtls_failure_evidence.clone(),
        });
    }

    pub fn on_dtls_warning(&mut self) {
        self.dtls_warning_count += 1;
    }

    pub fn on_dtls_failure_reason(&mut self, reason: DtlsFailureReason, evidence: Option<String>) {
        self.dtls_failure_reason = reason;
        self.dtls_failure_evidence = evidence;
    }

    pub fn on_dtls_fingerprint_validation(
        &mut self,
        local: Option<String>,
        remote: Option<String>,
        matched: Option<bool>,
    ) {
        self.local_fingerprint = local;
        self.remote_fingerprint = remote;
        self.fingerprint_match = matched;
        if matched == Some(false) {
            self.dtls_failure_reason = DtlsFailureReason::FingerprintMismatch;
            self.dtls_failure_evidence = Some("Local/remote DTLS fingerprint mismatch".to_string());
            self.dtls_outcome = DtlsOutcome::Failed;
        }
    }

    pub fn on_local_fingerprint(&mut self, local: String) {
        self.local_fingerprint = Some(local);
    }

    pub fn on_remote_fingerprint(&mut self, remote: String) {
        // Local and remote SDP fingerprints are expected to differ because they
        // belong to different peer certificates. Equality comparison between
        // them is invalid and caused false DTLS mismatch reports.
        self.remote_fingerprint = Some(remote);
        self.fingerprint_match = None;
    }

    pub fn on_dtls_certificate_validation(&mut self, valid: bool, evidence: Option<String>) {
        self.certificate_valid = Some(valid);
        if !valid {
            self.dtls_failure_reason = DtlsFailureReason::CertificateInvalid;
            self.dtls_failure_evidence = evidence;
            self.dtls_outcome = DtlsOutcome::Failed;
        }
    }

    pub fn on_candidate_pair_stats(
        &mut self,
        selected_pair_id: Option<String>,
        local_candidate_type: IceCandidateType,
        remote_candidate_type: IceCandidateType,
        transport: Option<String>,
        nomination_role: Option<String>,
        current_rtt_ms: Option<f64>,
        bytes_sent: u64,
        bytes_received: u64,
        packets_sent: u64,
        packets_received: u64,
        consent_requests: u64,
        consent_failures: u64,
        retransmission_indicators: u64,
    ) {
        if self.candidate_pair.is_none() {
            self.candidate_pair = Some(CandidatePairTelemetry {
                selected_pair_id,
                local_candidate_type,
                remote_candidate_type,
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
                transitions: Vec::new(),
            });
            return;
        }

        if let Some(pair) = &mut self.candidate_pair {
            pair.selected_pair_id = selected_pair_id;
            pair.local_candidate_type = local_candidate_type;
            pair.remote_candidate_type = remote_candidate_type;
            pair.transport = transport;
            pair.nomination_role = nomination_role;
            pair.current_rtt_ms = current_rtt_ms;
            pair.bytes_sent = bytes_sent;
            pair.bytes_received = bytes_received;
            pair.packets_sent = packets_sent;
            pair.packets_received = packets_received;
            pair.consent_requests = consent_requests;
            pair.consent_failures = consent_failures;
            pair.retransmission_indicators = retransmission_indicators;
        }

        let should_mark_nominated = self
            .candidate_pair
            .as_ref()
            .map(|pair| {
                let nominated = pair
                    .nomination_role
                    .as_deref()
                    .map(|v| {
                        let value = v.to_ascii_lowercase();
                        value == "true" || value == "1" || value == "yes"
                    })
                    .unwrap_or(false);
                nominated && !pair.transitions.iter().any(|t| t.state == IcePairState::Nominated)
            })
            .unwrap_or(false);

        if should_mark_nominated {
            self.push_pair_transition(IcePairState::Nominated, Some("selected pair nominated".to_string()));
        }
    }

    pub fn on_gateway_candidate_sent(&mut self, delivered: bool) {
        self.gateway_candidates_sent_total = self.gateway_candidates_sent_total.saturating_add(1);
        if !delivered {
            self.gateway_candidates_send_failures = self.gateway_candidates_send_failures.saturating_add(1);
        }
    }

    pub fn on_viewer_trickle_candidate_received(&mut self) {
        self.viewer_trickle_candidates_received = self.viewer_trickle_candidates_received.saturating_add(1);
    }

    pub fn on_viewer_sdp_candidates_received(&mut self, count: u64) {
        self.viewer_sdp_embedded_candidates_received =
            self.viewer_sdp_embedded_candidates_received.saturating_add(count);
    }

    pub fn on_signaling_channel_dropped(&mut self) {
        self.signaling_channel_dropped = true;
    }

    pub fn on_media_transport_stats(
        &mut self,
        packet_loss_percent: Option<f64>,
        jitter_ms: Option<f64>,
        packets_observed: Option<u64>,
    ) {
        self.media_packet_loss_percent = packet_loss_percent;
        self.media_jitter_ms = jitter_ms;
        if let Some(packets) = packets_observed {
            self.media_packets_observed = packets;
        }
    }

    pub fn on_candidate_pair_attempts(&mut self, attempts: Vec<CandidatePairAttemptTelemetry>) {
        self.candidate_pair_attempts = attempts;
    }

    pub fn on_viewer_teardown(&mut self) {
        self.session_ended_by_viewer = true;
    }

    /// Store per-viewer network probe results (called from async task spawned at viewer connect).
    pub fn set_network_probe(&mut self, report: NetworkProbeReport) {
        self.network_probe = Some(report);
    }

    /// Called when the network probe finishes AFTER the early ICE report was already printed.
    /// Prints just the network probe section so the user still sees it.
    pub fn print_updated_network_report(&self) {
        // Only emit this if the early ICE report was already printed without network data
        if !self.early_report_printed {
            return; // early report hasn't fired yet; it will include probe data when it does
        }
        let probe = match &self.network_probe {
            Some(p) => p,
            None => return,
        };
        log::info!("╔══════════ Network Probe Results (async) ═══════════╗");
        log::info!("║  NAT Type       : {}", probe.nat_type);
        for s in &probe.stun_results {
            let reflex = match (&s.reflexive_address, s.reflexive_port) {
                (Some(ip), Some(p)) => format!("{}:{}", ip, p),
                _ => "-".to_string(),
            };
            log::info!("║  STUN [{}]  {} | reflexive={} | rtt={}ms",
                s.server, s.status, reflex,
                s.rtt_ms.map(|r| r.to_string()).unwrap_or_else(|| "-".to_string()),
            );
            if let Some(e) = &s.error { log::warn!("║    error: {}", e); }
        }
        for t in &probe.turn_results {
            log::info!("║  TURN [{}]  {} | rtt={}ms",
                t.server, t.status,
                t.rtt_ms.map(|r| r.to_string()).unwrap_or_else(|| "-".to_string()),
            );
            if let Some(e) = &t.error { log::warn!("║    error: {}", e); }
        }
        log::info!("║  Signaling [{}]  {} | rtt={}ms",
            probe.signaling_result.url,
            probe.signaling_result.status,
            probe.signaling_result.rtt_ms.map(|r| r.to_string()).unwrap_or_else(|| "-".to_string()),
        );
        if let Some(e) = &probe.signaling_result.error { log::warn!("║    error: {}", e); }

        if probe.stun_results.iter().all(|r| r.status == TestStatus::Fail) {
            log::error!("║  ⚠ All STUN probes FAILED — UDP egress may be blocked");
        }
        if !probe.turn_results.is_empty() && probe.turn_results.iter().all(|r| r.status == TestStatus::Fail) {
            log::warn!("║  ⚠ All TURN probes FAILED — relay fallback unavailable");
        }
        match &probe.nat_type {
            NatType::SymmetricNat => log::warn!("║  ⚠ Symmetric NAT — TURN relay is required"),
            NatType::UdpBlocked   => log::error!("║  ⚠ UDP BLOCKED — WebRTC will not function"),
            _ => {}
        }
        log::info!("╚════════════════════════════════════════════════════╝");
    }

    // ─── SRTP / media ────────────────────────────────────────────────────────

    pub fn on_srtp_keys_ready(&mut self) {
        if self.srtp_ready_at.is_none() {
            self.srtp_ready_at = Some(Instant::now());
            // If DTLS done wasn't already recorded, record it now
            if self.dtls_done.is_none() {
                self.dtls_done = Some(Instant::now());
                self.dtls_outcome = DtlsOutcome::Connected;
            }
        }
    }

    pub fn on_first_rtp_packet(&mut self) {
        if self.first_rtp_packet_at.is_none() {
            self.first_rtp_packet_at = Some(Instant::now());
        }
    }

    pub fn on_first_keyframe(&mut self) {
        if self.first_keyframe_at.is_none() {
            self.first_keyframe_at = Some(Instant::now());
        }
    }

    pub fn on_first_decoded_frame(&mut self) {
        if self.first_decoded_frame_at.is_none() {
            self.first_decoded_frame_at = Some(Instant::now());
        }
    }

    pub fn on_first_rendered_frame(&mut self) {
        if self.first_rendered_frame_at.is_none() {
            self.first_rendered_frame_at = Some(Instant::now());
        }
    }

    pub fn on_frame_sent(&mut self) {
        if self.first_frame_at.is_none() {
            self.first_frame_at = Some(Instant::now());
        }
        self.last_frame_sent_at = Some(Instant::now());
        self.frames_sent += 1;
    }

    // ─── Early diagnostic (called right after ICE completes) ─────────────────

    /// Print an immediate connection-quality summary without consuming the monitor.
    /// Called as soon as ICE reaches Connected/Completed/Failed.
    pub fn print_early_report(&mut self) {
        if self.early_report_printed {
            return;
        }
        self.early_report_printed = true;

        let dur_ms = |from: Option<Instant>, to: Option<Instant>| -> String {
            match (from, to) {
                (Some(f), Some(t)) if t >= f => format!("{}ms", t.duration_since(f).as_millis()),
                _ => "n/a".to_string(),
            }
        };
        let elapsed_ms = |t: Option<Instant>| -> String {
            t.map(|i| format!("{}ms", i.duration_since(self.start).as_millis()))
             .unwrap_or_else(|| "n/a".to_string())
        };

        let local_relay = self.local_candidates.iter().filter(|c| c.candidate_type == IceCandidateType::Relay).count();
        let local_srflx = self.local_candidates.iter().filter(|c| c.candidate_type == IceCandidateType::ServerReflexive).count();
        let local_host  = self.local_candidates.iter().filter(|c| c.candidate_type == IceCandidateType::Host).count();

        let ice_status = match &self.ice_outcome {
            IceConnectionOutcome::Connected | IceConnectionOutcome::Completed => "✓ SUCCESS",
            IceConnectionOutcome::Failed     => "✗ FAILED",
            IceConnectionOutcome::Disconnected => "! DISCONNECTED",
            IceConnectionOutcome::InProgress   => "… IN PROGRESS",
        };

        log::info!("╔══════════ WebRTC Connection Analysis ══════════════╗");
        log::info!("║  Viewer : {}", self.viewer_id);
        log::info!("║  Camera : {} ({})  Room: {}", self.camera_name, self.camera_id, self.room_name);
        log::info!("╠══ ICE Phase ════════════════════════════════════════╣");
        log::info!("║  Status         : {}", ice_status);
        log::info!("║  Gathering time : {}", dur_ms(self.ice_gathering_start, self.ice_gathering_end));
        log::info!("║  Connect time   : {}", dur_ms(self.ice_checking_start, self.ice_connected_at));
        log::info!("║  Total → ICE    : {}", elapsed_ms(self.ice_connected_at));
        log::info!("║  Local  cands   : {} total  (host={} srflx={} relay={})",
            self.local_candidates.len(), local_host, local_srflx, local_relay);
        log::info!("║  Remote cands   : {}", self.remote_candidates.len());

        // ICE quick verdict
        if self.ice_outcome == IceConnectionOutcome::Failed {
            if local_relay == 0 && local_srflx == 0 {
                log::error!("║  ⚠ Only host candidates gathered — STUN unreachable or UDP blocked");
            } else if local_relay == 0 {
                log::warn!("║  ⚠ No TURN relay candidates — TURN server may be unreachable");
            } else {
                log::error!("║  ⚠ ICE failed despite relay candidates — check firewall/TURN credentials");
            }
        } else {
            if local_relay > 0 {
                log::info!("║  ✓ TURN relay available — connection can cross symmetric NAT");
            }
            if local_srflx > 0 {
                log::info!("║  ✓ Server-reflexive candidates — NAT traversal possible without relay");
            }
        }

        // Network probe section (populated async; may not be ready yet)
        if let Some(probe) = &self.network_probe {
            log::info!("╠══ Network Probe (per-viewer) ═══════════════════════╣");
            log::info!("║  NAT Type       : {}", probe.nat_type);
            for s in &probe.stun_results {
                let reflex = match (&s.reflexive_address, s.reflexive_port) {
                    (Some(ip), Some(p)) => format!("{}:{}", ip, p),
                    _ => "-".to_string(),
                };
                log::info!("║  STUN [{}]", s.server);
                log::info!("║    → {} | reflexive={} | rtt={}ms",
                    s.status,
                    reflex,
                    s.rtt_ms.map(|r| r.to_string()).unwrap_or_else(|| "-".to_string()),
                );
                if let Some(e) = &s.error {
                    log::warn!("║    → error: {}", e);
                }
            }
            for t in &probe.turn_results {
                log::info!("║  TURN [{}]", t.server);
                log::info!("║    → {} | rtt={}ms",
                    t.status,
                    t.rtt_ms.map(|r| r.to_string()).unwrap_or_else(|| "-".to_string()),
                );
                if let Some(e) = &t.error {
                    log::warn!("║    → error: {}", e);
                }
            }
            log::info!("║  Signaling [{}]", probe.signaling_result.url);
            log::info!("║    → {} | rtt={}ms",
                probe.signaling_result.status,
                probe.signaling_result.rtt_ms.map(|r| r.to_string()).unwrap_or_else(|| "-".to_string()),
            );
            if let Some(e) = &probe.signaling_result.error {
                log::warn!("║    → error: {}", e);
            }

            // Network-level verdicts
            let stun_all_fail = probe.stun_results.iter().all(|r| r.status == TestStatus::Fail);
            let turn_all_fail = !probe.turn_results.is_empty()
                && probe.turn_results.iter().all(|r| r.status == TestStatus::Fail);
            if stun_all_fail {
                log::error!("║  ⚠ All STUN probes FAILED — UDP egress may be blocked");
            }
            if turn_all_fail {
                log::warn!("║  ⚠ All TURN probes FAILED — relay fallback unavailable");
            }
            if probe.nat_type == NatType::SymmetricNat {
                log::warn!("║  ⚠ Symmetric NAT detected — TURN relay required for connectivity");
            }
            if probe.nat_type == NatType::UdpBlocked {
                log::error!("║  ⚠ UDP appears BLOCKED — WebRTC will not work without TCP/TURN fallback");
            }
        } else {
            log::info!("║  Network probe  : running in background (will appear in final report)");
        }

        log::info!("╚════════════════════════════════════════════════════╝");
    }

    // ─── Finalize ────────────────────────────────────────────────────────────

    /// Consume the monitor, compute durations, run diagnosis, return report.
    pub fn finalize(self) -> ConnectionDiagnosticReport {
        let elapsed_ms = |t: Option<Instant>| {
            t.map(|i| i.duration_since(self.start).as_millis() as u64)
        };
        let duration_ms = |from: Option<Instant>, to: Option<Instant>| -> Option<u64> {
            match (from, to) {
                (Some(f), Some(t)) if t > f => Some(t.duration_since(f).as_millis() as u64),
                _ => None,
            }
        };

        let ice_gathering_ms  = duration_ms(self.ice_gathering_start, self.ice_gathering_end);
        let ice_connection_ms = duration_ms(self.ice_checking_start, self.ice_connected_at);
        let dtls_handshake_ms = duration_ms(self.dtls_start, self.dtls_done);
        let srtp_ready_ms     = elapsed_ms(self.srtp_ready_at);
        let first_frame_ms    = elapsed_ms(self.first_frame_at);
        let media_timeline = MediaStageTimeline {
            srtp_keys_ready_ms: elapsed_ms(self.srtp_ready_at),
            first_rtp_packet_ms: elapsed_ms(self.first_rtp_packet_at),
            first_keyframe_ms: elapsed_ms(self.first_keyframe_at),
            first_decoded_frame_ms: elapsed_ms(self.first_decoded_frame_at),
            first_rendered_frame_ms: elapsed_ms(self.first_rendered_frame_at),
        };

        let mut effective_dtls_outcome = self.dtls_outcome.clone();
        let dtls_inferred_from_media =
            self.srtp_ready_at.is_some() || self.first_rtp_packet_at.is_some() || self.first_frame_at.is_some();
        if effective_dtls_outcome != DtlsOutcome::Connected && dtls_inferred_from_media {
            effective_dtls_outcome = DtlsOutcome::Connected;
        }

        let mut dtls_details = DtlsDiagnostics {
            markers: self.dtls_markers,
            negotiated_cipher: self.negotiated_cipher,
            local_fingerprint: self.local_fingerprint,
            remote_fingerprint: self.remote_fingerprint,
            fingerprint_match: self.fingerprint_match,
            certificate_valid: self.certificate_valid,
            failure_reason: self.dtls_failure_reason,
            failure_evidence: self.dtls_failure_evidence,
        };

        // Never report a DTLS failure reason when DTLS ended connected unless
        // we have hard proof of a strict validation failure.
        if effective_dtls_outcome == DtlsOutcome::Connected {
            match dtls_details.failure_reason {
                DtlsFailureReason::CertificateInvalid | DtlsFailureReason::FingerprintMismatch => {}
                _ => {
                    dtls_details.failure_reason = DtlsFailureReason::None;
                    dtls_details.failure_evidence = None;
                }
            }
        }

        let root_cause_tree = build_root_cause_tree(
            &self.ice_outcome,
            &effective_dtls_outcome,
            self.network_probe.as_ref(),
            self.candidate_pair.as_ref(),
            &dtls_details,
            &media_timeline,
            self.session_ended_by_viewer,
        );
        // Clone raw candidate lists for the structured terminal report before summarising.
        let local_candidate_list = self.local_candidates.clone();
        let remote_candidate_list = self.remote_candidates.clone();
        let local_candidates = summarize_candidates(&self.local_candidates);
        let remote_candidates = summarize_candidates(&self.remote_candidates);
        let candidate_path_assessment = assess_candidate_path(
            &self.ice_outcome,
            &local_candidates,
            &remote_candidates,
        );

        let entries = build_diagnosis(
            &self.ice_outcome,
            &effective_dtls_outcome,
            self.dtls_warning_count,
            ice_gathering_ms,
            ice_connection_ms,
            dtls_handshake_ms,
            self.frames_sent,
            &self.local_candidates,
            &self.remote_candidates,
            self.network_probe.as_ref(),
            self.session_ended_by_viewer,
        );

        let phase3 = build_phase3_ice_gathering(
            &local_candidates,
            self.network_probe.as_ref(),
        );
        let phase4 = build_phase4_candidate_exchange(
            self.gateway_candidates_sent_total,
            self.gateway_candidates_send_failures,
            self.viewer_trickle_candidates_received,
            self.viewer_sdp_embedded_candidates_received,
            self.signaling_channel_dropped,
        );
        let phase5 = build_phase5_ice_checks(
            &self.ice_outcome,
            self.network_probe.as_ref(),
            self.candidate_pair.as_ref(),
            ice_connection_ms,
        );
        let phase6 = build_phase6_dtls(
            &effective_dtls_outcome,
            &dtls_details,
            dtls_handshake_ms,
            self.session_ended_by_viewer,
        );
        let phase7 = build_phase7_media_flow(
            self.frames_sent,
            self.media_packets_observed,
            self.media_packet_loss_percent.or_else(|| {
                self.egress_fraction_lost.map(|l| l * 100.0)
            }),
            self.media_jitter_ms,
            metrics::get_stream_metrics(&self.room_name)
                .map(|snapshot| snapshot.output_packet_bitrate_kbps),
            self.first_frame_at,
            self.last_frame_sent_at,
            &self.ice_outcome,
            &effective_dtls_outcome,
            self.session_ended_by_viewer,
            self.egress_packets_sent,
            self.egress_remote_packets_received,
            self.egress_last_feedback_at.map(|t| t.elapsed().as_secs()),
            self.viewer_displayed,
        );

        ConnectionDiagnosticReport {
            viewer_id: self.viewer_id,
            camera_id: self.camera_id,
            camera_name: self.camera_name,
            room_name: self.room_name,
            ice_gathering_ms,
            ice_connection_ms,
            dtls_handshake_ms,
            srtp_ready_ms,
            first_frame_ms,
            local_candidate_count: self.local_candidates.len(),
            remote_candidate_count: self.remote_candidates.len(),
            local_candidates,
            remote_candidates,
            candidate_path_assessment,
            dtls_warning_count: self.dtls_warning_count,
            frames_sent: self.frames_sent,
            candidate_pair: self.candidate_pair,
            candidate_pair_attempts: self.candidate_pair_attempts,
            dtls_details,
            media_timeline,
            root_cause_tree,
            phase3,
            phase4,
            phase5,
            phase6,
            phase7,
            session_ended_by_viewer: self.session_ended_by_viewer,
            ice_outcome: self.ice_outcome,
            dtls_outcome: effective_dtls_outcome,
            egress: EgressStats {
                packets_sent: self.egress_packets_sent,
                remote_packets_received: self.egress_remote_packets_received,
                fraction_lost: self.egress_fraction_lost,
                nack_count: self.egress_nack_count,
                pli_count: self.egress_pli_count,
                fir_count: self.egress_fir_count,
                last_remote_feedback_age_secs: self
                    .egress_last_feedback_at
                    .map(|t| t.elapsed().as_secs()),
                egress_active: self.egress_active,
            },
            viewer_report: if self.viewer_frames_decoded > 0
                || self.viewer_displayed
                || self.viewer_first_decoded_ms.is_some()
            {
                Some(ViewerSideReport {
                    frames_decoded: self.viewer_frames_decoded,
                    decode_fps: self.viewer_decode_fps,
                    first_decoded_ms: self.viewer_first_decoded_ms,
                    displayed: self.viewer_displayed,
                })
            } else {
                None
            },
            entries,
            local_candidate_list,
            remote_candidate_list,
            network_probe: self.network_probe,
        }
    }
}

fn build_phase3_ice_gathering(
    local: &IceCandidateSummary,
    network_probe: Option<&NetworkProbeReport>,
) -> IceGatheringPhaseReport {
    let stun_responded_with_srflx = if local.srflx > 0 {
        true
    } else {
        network_probe
            .map(|probe| {
                probe
                    .stun_results
                    .iter()
                    .any(|s| s.status == TestStatus::Pass && s.reflexive_address.is_some())
            })
            .unwrap_or(false)
    };

    let turn_relay_allocated = if local.relay > 0 {
        true
    } else {
        network_probe
            .map(|probe| probe.turn_results.iter().any(|t| t.status == TestStatus::Pass))
            .unwrap_or(false)
    };

    let failure_reason = if local.total() == 0 {
        Some("No candidates gathered at all".to_string())
    } else if local.srflx == 0 && !stun_responded_with_srflx {
        Some("STUN unreachable or no server-reflexive address returned".to_string())
    } else if local.relay == 0 && !turn_relay_allocated {
        Some("TURN did not allocate relay candidates (unreachable/auth failed)".to_string())
    } else if local.srflx == 0 && local.relay == 0 && local.host > 0 {
        Some("Only host candidates available; NAT traversal likely to fail".to_string())
    } else {
        None
    };

    IceGatheringPhaseReport {
        total_local_candidates: local.total(),
        host_candidates: local.host,
        srflx_candidates: local.srflx,
        relay_candidates: local.relay,
        stun_responded_with_srflx,
        turn_relay_allocated,
        failure_reason,
    }
}

fn build_phase4_candidate_exchange(
    gateway_candidates_sent_total: u64,
    gateway_candidates_send_failures: u64,
    viewer_trickle_candidates_received: u64,
    viewer_sdp_embedded_candidates_received: u64,
    signaling_channel_dropped: bool,
) -> CandidateExchangePhaseReport {
    let gateway_candidates_delivered =
        gateway_candidates_sent_total > 0 && gateway_candidates_send_failures < gateway_candidates_sent_total;
    let viewer_sent_candidates_back =
        viewer_trickle_candidates_received > 0 || viewer_sdp_embedded_candidates_received > 0;
    let trickle_ice_working = viewer_trickle_candidates_received > 0 && gateway_candidates_delivered;

    let failure_reason = if signaling_channel_dropped {
        Some("Signaling channel dropped while exchanging ICE candidates".to_string())
    } else if !gateway_candidates_delivered {
        Some("Gateway candidates were not delivered to viewer via signaling".to_string())
    } else if !viewer_sent_candidates_back {
        Some("Viewer did not send ICE candidates back".to_string())
    } else if !trickle_ice_working {
        Some("Trickle ICE appears degraded or not active; fallback likely relied on SDP-only candidates".to_string())
    } else {
        None
    };

    CandidateExchangePhaseReport {
        gateway_candidates_sent_total,
        gateway_candidates_send_failures,
        gateway_candidates_delivered,
        viewer_trickle_candidates_received,
        viewer_sdp_embedded_candidates_received,
        viewer_sent_candidates_back,
        trickle_ice_working,
        signaling_channel_dropped,
        failure_reason,
    }
}

fn build_phase5_ice_checks(
    ice_outcome: &IceConnectionOutcome,
    network_probe: Option<&NetworkProbeReport>,
    candidate_pair: Option<&CandidatePairTelemetry>,
    ice_connection_ms: Option<u64>,
) -> IceConnectivityPhaseReport {
    let candidate_pair_nominated = candidate_pair
        .map(|pair| {
            pair.transitions.iter().any(|t| t.state == IcePairState::Nominated)
                || pair
                    .nomination_role
                    .as_deref()
                    .map(|v| {
                        let value = v.to_ascii_lowercase();
                        value == "true" || value == "1" || value == "yes"
                    })
                    .unwrap_or(false)
        })
        .unwrap_or(false);

    let nat_type = network_probe.map(|p| p.nat_type.clone()).unwrap_or(NatType::Unknown);
    let selected_local_candidate_type = candidate_pair
        .map(|pair| pair.local_candidate_type.clone())
        .unwrap_or(IceCandidateType::Unknown);
    let selected_remote_candidate_type = candidate_pair
        .map(|pair| pair.remote_candidate_type.clone())
        .unwrap_or(IceCandidateType::Unknown);
    let ice_failed = *ice_outcome == IceConnectionOutcome::Failed;
    let stuck_in_checking = *ice_outcome == IceConnectionOutcome::InProgress
        || (*ice_outcome == IceConnectionOutcome::Failed
            && matches!(ice_connection_ms, Some(ms) if ms > 15_000));

    let failure_reason = if ice_failed {
        if nat_type == NatType::SymmetricNat
            && selected_local_candidate_type != IceCandidateType::Relay
            && selected_remote_candidate_type != IceCandidateType::Relay
        {
            Some("Symmetric NAT without TURN relay candidate pair".to_string())
        } else if selected_local_candidate_type != IceCandidateType::Relay
            && selected_remote_candidate_type != IceCandidateType::Relay
        {
            Some("TURN relay not selected; connectivity checks likely blocked by NAT/firewall".to_string())
        } else {
            Some("ICE failed; verify UDP firewall rules and candidate pair reachability".to_string())
        }
    } else if stuck_in_checking {
        Some("ICE remained in checking and did not complete within timeout window".to_string())
    } else {
        None
    };

    IceConnectivityPhaseReport {
        candidate_pair_nominated,
        nat_type,
        selected_local_candidate_type,
        selected_remote_candidate_type,
        ice_failed,
        stuck_in_checking,
        failure_reason,
    }
}

fn build_phase6_dtls(
    dtls_outcome: &DtlsOutcome,
    dtls_details: &DtlsDiagnostics,
    dtls_handshake_ms: Option<u64>,
    session_ended_by_viewer: bool,
) -> DtlsHandshakePhaseReport {
    let handshake_completed = *dtls_outcome == DtlsOutcome::Connected;
    let remote_fingerprint_valid = match dtls_details.failure_reason {
        DtlsFailureReason::FingerprintMismatch => Some(false),
        _ => dtls_details.certificate_valid,
    };

    let failure_reason = if handshake_completed || session_ended_by_viewer {
        None
    } else {
        match dtls_details.failure_reason {
            DtlsFailureReason::FingerprintMismatch => Some("Certificate fingerprint mismatch".to_string()),
            DtlsFailureReason::CertificateInvalid => Some("Remote certificate validation failed (possibly clock skew)".to_string()),
            DtlsFailureReason::Timeout => Some("DTLS handshake timeout".to_string()),
            DtlsFailureReason::HandshakeAborted => Some("DTLS handshake aborted".to_string()),
            DtlsFailureReason::TransportInterrupted => Some("ICE/transport interrupted during DTLS handshake".to_string()),
            DtlsFailureReason::Unknown | DtlsFailureReason::None => {
                if matches!(dtls_handshake_ms, Some(ms) if ms > 5_000) {
                    Some("DTLS timeout suspected due to prolonged handshake".to_string())
                } else {
                    Some("DTLS did not complete".to_string())
                }
            }
        }
    };

    DtlsHandshakePhaseReport {
        handshake_completed,
        remote_fingerprint_valid,
        failure_reason,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_phase7_media_flow(
    frames_sent: u64,
    media_packets_observed: u64,
    media_packet_loss_percent: Option<f64>,
    media_jitter_ms: Option<f64>,
    bitrate_kbps: Option<f64>,
    first_frame_at: Option<Instant>,
    last_frame_sent_at: Option<Instant>,
    ice_outcome: &IceConnectionOutcome,
    dtls_outcome: &DtlsOutcome,
    session_ended_by_viewer: bool,
    egress_packets_sent: u64,
    egress_remote_packets_received: u64,
    remote_feedback_age_secs: Option<u64>,
    viewer_displayed: bool,
) -> MediaFlowPhaseReport {
    // Prefer hard proof of delivery: packets the remote peer acknowledged via
    // RTCP, then packets actually sent on the wire, and only fall back to the
    // local sink frame count (which can advance even when nothing leaves the box).
    let rtp_packets_observed = if egress_remote_packets_received > 0 {
        egress_remote_packets_received
    } else if egress_packets_sent > 0 {
        egress_packets_sent
    } else if media_packets_observed > 0 {
        media_packets_observed
    } else {
        frames_sent
    };

    // Stale remote feedback while packets are still being sent indicates a
    // silent egress stall (the remote stopped acknowledging media).
    let remote_feedback_stale = matches!(remote_feedback_age_secs, Some(age) if age >= 5);

    let freeze_detected = if session_ended_by_viewer || viewer_displayed {
        false
    } else if remote_feedback_stale && egress_packets_sent > 0 {
        true
    } else {
        match (first_frame_at, last_frame_sent_at) {
        (Some(_), Some(last)) => last.elapsed().as_secs() >= 5,
        _ => false,
        }
    };

    let remote_never_acked = egress_packets_sent > 0 && egress_remote_packets_received == 0;

    let failure_reason = if rtp_packets_observed == 0 {
        if (*ice_outcome == IceConnectionOutcome::Connected || *ice_outcome == IceConnectionOutcome::Completed)
            && *dtls_outcome == DtlsOutcome::Connected
        {
            Some("No RTP observed after DTLS; encoder pipeline or post-DTLS firewall may be blocking media".to_string())
        } else {
            Some("No RTP observed in test window".to_string())
        }
    } else if remote_never_acked && !viewer_displayed {
        Some("Packets were sent but the remote peer never acknowledged any via RTCP; media likely not reaching the viewer".to_string())
    } else if freeze_detected {
        Some("Media flow froze/dropped before test ended".to_string())
    } else if let Some(loss) = media_packet_loss_percent {
        if loss > 10.0 {
            Some("Packet loss is high; bitrate may exceed available link capacity".to_string())
        } else {
            None
        }
    } else {
        None
    };

    MediaFlowPhaseReport {
        rtp_packets_observed,
        packet_loss_percent: media_packet_loss_percent,
        jitter_ms: media_jitter_ms,
        bitrate_kbps,
        stream_freeze_or_drop_detected: freeze_detected,
        failure_reason,
    }
}

fn branch(score: u8, confidence: f32, summary: &str, evidence: Vec<String>) -> RootCauseBranch {
    RootCauseBranch {
        score,
        confidence,
        summary: summary.to_string(),
        evidence,
    }
}

fn build_root_cause_tree(
    ice_outcome: &IceConnectionOutcome,
    dtls_outcome: &DtlsOutcome,
    network_probe: Option<&NetworkProbeReport>,
    candidate_pair: Option<&CandidatePairTelemetry>,
    dtls_details: &DtlsDiagnostics,
    media_timeline: &MediaStageTimeline,
    session_ended_by_viewer: bool,
) -> RootCauseTree {
    let network = if let Some(probe) = network_probe {
        let blocked = probe.stun_results.iter().all(|r| r.status == TestStatus::Fail);
        if blocked {
            branch(
                90,
                0.9,
                "UDP egress appears blocked",
                vec!["All STUN probes failed".to_string()],
            )
        } else {
            branch(15, 0.6, "No major network-level blocker detected", Vec::new())
        }
    } else {
        branch(25, 0.3, "No network probe evidence", Vec::new())
    };

    let nat_turn = if let Some(probe) = network_probe {
        match probe.nat_type {
            NatType::SymmetricNat => branch(
                75,
                0.8,
                "Symmetric NAT detected; TURN path critical",
                vec!["NAT classified as symmetric".to_string()],
            ),
            NatType::UdpBlocked => branch(
                95,
                0.95,
                "NAT/Firewall blocks UDP",
                vec!["NAT classified as UDP blocked".to_string()],
            ),
            _ => branch(20, 0.5, "NAT/TURN branch not dominant", Vec::new()),
        }
    } else {
        branch(30, 0.3, "No NAT evidence available", Vec::new())
    };

    let signaling = branch(10, 0.4, "No explicit signaling failure captured", Vec::new());

    let ice = match ice_outcome {
        IceConnectionOutcome::Failed => branch(
            90,
            0.85,
            "ICE failed to produce a viable connection",
            vec!["ICE state reached Failed".to_string()],
        ),
        IceConnectionOutcome::Disconnected => {
            if session_ended_by_viewer && media_timeline.first_rtp_packet_ms.is_some() {
                branch(
                    15,
                    0.8,
                    "Session ended by viewer after media started",
                    vec!["Viewer teardown requested".to_string()],
                )
            } else {
                branch(
                    70,
                    0.7,
                    "ICE disconnected after initial connectivity",
                    vec!["ICE state reached Disconnected".to_string()],
                )
            }
        }
        IceConnectionOutcome::Connected | IceConnectionOutcome::Completed => {
            if let Some(pair) = candidate_pair {
                let mut evidence = Vec::new();
                if pair.consent_failures > 0 {
                    evidence.push(format!("Consent failures: {}", pair.consent_failures));
                }
                if pair.retransmission_indicators > 0 {
                    evidence.push(format!("Retransmission indicators: {}", pair.retransmission_indicators));
                }
                branch(20, 0.7, "ICE reached connected state", evidence)
            } else {
                branch(30, 0.5, "ICE connected without selected pair telemetry", Vec::new())
            }
        }
        IceConnectionOutcome::InProgress => branch(40, 0.4, "ICE still in progress at report time", Vec::new()),
    };

    let dtls = if (*dtls_outcome != DtlsOutcome::Connected)
        && (dtls_details.failure_reason == DtlsFailureReason::FingerprintMismatch
        || dtls_details.failure_reason == DtlsFailureReason::CertificateInvalid)
    {
        branch(
            98,
            0.95,
            "DTLS strict validation failed",
            vec![format!("Failure reason: {}", dtls_details.failure_reason)],
        )
    } else {
        match dtls_outcome {
            DtlsOutcome::Failed | DtlsOutcome::Timeout => branch(
                88,
                0.85,
                "DTLS handshake did not complete",
                vec![format!("DTLS outcome: {}", dtls_outcome)],
            ),
            DtlsOutcome::InProgress | DtlsOutcome::NotStarted => branch(
                65,
                0.6,
                "DTLS not completed before teardown",
                vec![format!("DTLS outcome: {}", dtls_outcome)],
            ),
            DtlsOutcome::Connected => branch(10, 0.8, "DTLS connected", Vec::new()),
        }
    };

    let srtp_media = if media_timeline.srtp_keys_ready_ms.is_none() {
        branch(85, 0.8, "SRTP keys were never observed", Vec::new())
    } else if media_timeline.first_rtp_packet_ms.is_none() {
        branch(80, 0.75, "SRTP ready but no RTP flow observed", Vec::new())
    } else if media_timeline.first_keyframe_ms.is_none() {
        branch(60, 0.6, "RTP observed but no keyframe seen", Vec::new())
    } else {
        branch(15, 0.7, "Media reached keyframe stage", Vec::new())
    };

    let mut max_branch = ("network", network.score);
    for (name, score) in [
        ("nat_turn", nat_turn.score),
        ("signaling", signaling.score),
        ("ice", ice.score),
        ("dtls", dtls.score),
        ("srtp_media", srtp_media.score),
    ] {
        if score > max_branch.1 {
            max_branch = (name, score);
        }
    }

    RootCauseTree {
        network,
        nat_turn,
        signaling,
        ice,
        dtls,
        srtp_media,
        overall: format!("Primary suspected branch: {} (score={})", max_branch.0, max_branch.1),
    }
}

fn summarize_candidates(candidates: &[IceCandidateInfo]) -> IceCandidateSummary {
    let mut summary = IceCandidateSummary::default();
    for c in candidates {
        match c.candidate_type {
            IceCandidateType::Host => summary.host += 1,
            IceCandidateType::ServerReflexive => summary.srflx += 1,
            IceCandidateType::Relay => summary.relay += 1,
            IceCandidateType::Unknown => summary.unknown += 1,
        }
    }
    summary
}

fn assess_candidate_path(
    ice_outcome: &IceConnectionOutcome,
    local: &IceCandidateSummary,
    remote: &IceCandidateSummary,
) -> String {
    if *ice_outcome == IceConnectionOutcome::Failed {
        if local.total() == 0 {
            return "FAILED: no local candidates gathered; STUN/TURN config or network interface issue".to_string();
        }
        if local.srflx == 0 && local.relay == 0 {
            return "FAILED: only local host candidates available; NAT traversal impossible without STUN/TURN".to_string();
        }
        if remote.total() == 0 {
            return "FAILED: remote side did not provide candidates; signaling/trickle path likely broken".to_string();
        }
        if local.relay == 0 || remote.relay == 0 {
            return "FAILED: relay candidates missing on one side; TURN path unavailable".to_string();
        }
        return "FAILED: candidates existed but no valid pair reached Connected".to_string();
    }

    if *ice_outcome == IceConnectionOutcome::Connected || *ice_outcome == IceConnectionOutcome::Completed {
        if remote.total() == 0 {
            return "SUCCESS (partial visibility): ICE connected, but remote candidates were not observed locally; likely non-trickle or signaling truncation".to_string();
        }
        if local.relay > 0 || remote.relay > 0 {
            return "SUCCESS: relay-capable path available (TURN). Selected pair may be relay or srflx depending on connectivity checks".to_string();
        }
        if local.srflx > 0 || remote.srflx > 0 {
            return "SUCCESS: server-reflexive path likely used (STUN-assisted NAT traversal)".to_string();
        }
        return "SUCCESS: host-to-host path likely used (same LAN or direct routable path)".to_string();
    }

    "IN PROGRESS: waiting for ICE completion to determine candidate pair success/failure".to_string()
}

// ─── Diagnosis rules ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn build_diagnosis(
    ice_outcome: &IceConnectionOutcome,
    dtls_outcome: &DtlsOutcome,
    dtls_warning_count: u32,
    ice_gathering_ms: Option<u64>,
    ice_connection_ms: Option<u64>,
    dtls_handshake_ms: Option<u64>,
    frames_sent: u64,
    local_candidates: &[IceCandidateInfo],
    remote_candidates: &[IceCandidateInfo],
    network_probe: Option<&NetworkProbeReport>,
    session_ended_by_viewer: bool,
) -> Vec<DiagnosisEntry> {
    let mut entries = Vec::new();

    // ── ICE failure ───────────────────────────────────────────────────────────
    if *ice_outcome == IceConnectionOutcome::Failed {
        let has_relay_local  = local_candidates.iter().any(|c| c.candidate_type == IceCandidateType::Relay);
        let has_relay_remote = remote_candidates.iter().any(|c| c.candidate_type == IceCandidateType::Relay);
        let only_host_local  = !local_candidates.is_empty()
            && local_candidates.iter().all(|c| c.candidate_type == IceCandidateType::Host);

        if only_host_local {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Fail,
                category: "ICE".to_string(),
                message: "ICE failed — only host candidates gathered, no server-reflexive or relay candidates.".to_string(),
                fix: "Verify the STUN server address in WEBRTC_STUN_SERVER is reachable and UDP is not blocked.".to_string(),
            });
        } else if !has_relay_local || !has_relay_remote {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Fail,
                category: "ICE".to_string(),
                message: "ICE failed — TURN relay candidates were not available.".to_string(),
                fix: "Ensure WEBRTC_TURN_SERVERS is configured and the TURN server accepts allocations. Check TURN credentials.".to_string(),
            });
        } else {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Fail,
                category: "ICE".to_string(),
                message: "ICE failed — no candidate pair reached the Connected state.".to_string(),
                fix: "Check firewall rules on both endpoints. Confirm the TURN server allows relayed traffic.".to_string(),
            });
        }

        // Cross-reference network probe
        if let Some(probe) = network_probe {
            let stun_blocked = probe.stun_results.iter().all(|r| r.status == TestStatus::Fail);
            if stun_blocked {
                entries.push(DiagnosisEntry {
                    severity: TestStatus::Fail,
                    category: "Network".to_string(),
                    message: "Startup STUN probe also failed — UDP is likely blocked on this host.".to_string(),
                    fix: "Ensure UDP egress is permitted from this host. Check host-level and cloud security-group firewall rules.".to_string(),
                });
            }
            if probe.nat_type == NatType::SymmetricNat {
                entries.push(DiagnosisEntry {
                    severity: TestStatus::Warn,
                    category: "NAT".to_string(),
                    message: "Symmetric NAT detected — direct peer-to-peer and srflx candidates will not work.".to_string(),
                    fix: "A TURN relay server is required. Confirm WEBRTC_TURN_SERVERS is set and reachable.".to_string(),
                });
            }
            if probe.nat_type == NatType::UdpBlocked {
                entries.push(DiagnosisEntry {
                    severity: TestStatus::Fail,
                    category: "NAT".to_string(),
                    message: "UDP appears completely blocked — NAT type could not be determined.".to_string(),
                    fix: "Open UDP ports 3478 (STUN/TURN) and the media port range in your firewall/security group.".to_string(),
                });
            }
        }
    }

    // ── ICE slow ─────────────────────────────────────────────────────────────
    if let Some(ms) = ice_connection_ms {
        if ms > 5_000 {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Warn,
                category: "ICE".to_string(),
                message: format!("ICE connection took {}ms — this is unusually slow (>5s).", ms),
                fix: "Check network latency and ensure relay candidates are available for faster fallback.".to_string(),
            });
        }
    }

    // ── ICE gathering slow ────────────────────────────────────────────────────
    if let Some(ms) = ice_gathering_ms {
        if ms > 3_000 {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Warn,
                category: "ICE".to_string(),
                message: format!("ICE gathering took {}ms — STUN or TURN server may be slow to respond.", ms),
                fix: "Check STUN/TURN server latency. Consider using a geographically closer server.".to_string(),
            });
        }
    }

    // ── No candidates ─────────────────────────────────────────────────────────
    if local_candidates.is_empty() {
        entries.push(DiagnosisEntry {
            severity: TestStatus::Fail,
            category: "ICE".to_string(),
            message: "No local ICE candidates were gathered at all.".to_string(),
            fix: "Check that the webrtcbin element is configured with a valid STUN server. Verify network interface availability.".to_string(),
        });
    }

    // ── DTLS failure ──────────────────────────────────────────────────────────
    match dtls_outcome {
        DtlsOutcome::Failed => {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Fail,
                category: "DTLS".to_string(),
                message: "DTLS handshake failed — encrypted channel could not be established.".to_string(),
                fix: "Check certificate validity. Ensure clock skew is less than ±30s. Inspect GStreamer bus warnings for certificate errors.".to_string(),
            });
        }
        DtlsOutcome::Timeout => {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Fail,
                category: "DTLS".to_string(),
                message: "DTLS handshake timed out — ICE may have connected but DTLS packets are not flowing.".to_string(),
                fix: "Check that UDP is not blocked after ICE succeeds. Some firewalls block DTLS (port 443 UDP). Try TCP fallback if available.".to_string(),
            });
        }
        DtlsOutcome::InProgress if *ice_outcome == IceConnectionOutcome::Connected
            || *ice_outcome == IceConnectionOutcome::Completed =>
        {
            if !session_ended_by_viewer {
                entries.push(DiagnosisEntry {
                    severity: TestStatus::Warn,
                    category: "DTLS".to_string(),
                    message: "ICE connected but DTLS handshake did not complete before teardown/reporting.".to_string(),
                    fix: "Keep viewer connected longer and inspect webrtcbin connection-state transitions. Check for packet loss or blocked DTLS traffic.".to_string(),
                });
            }
        }
        DtlsOutcome::NotStarted if *ice_outcome == IceConnectionOutcome::Connected
            || *ice_outcome == IceConnectionOutcome::Completed =>
        {
            if !session_ended_by_viewer {
                entries.push(DiagnosisEntry {
                    severity: TestStatus::Warn,
                    category: "DTLS".to_string(),
                    message: "ICE connected but DTLS handshake transition was not observed.".to_string(),
                    fix: "This can happen when the session disconnects early or callbacks miss state changes. Verify connection-state notifications and keep the session open longer.".to_string(),
                });
            }
        }
        _ => {}
    }

    // ── Recurring DTLS warnings ───────────────────────────────────────────────
    if dtls_warning_count >= 3 {
        entries.push(DiagnosisEntry {
            severity: TestStatus::Warn,
            category: "DTLS".to_string(),
            message: format!("{} DTLS runtime warnings were detected during the session.", dtls_warning_count),
            fix: "Intermittent DTLS warnings can indicate packet loss or network jitter. Monitor for auto-restart events.".to_string(),
        });
    }

    // ── DTLS slow ─────────────────────────────────────────────────────────────
    if let Some(ms) = dtls_handshake_ms {
        if ms > 2_000 {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Warn,
                category: "DTLS".to_string(),
                message: format!("DTLS handshake took {}ms — high latency or packet loss suspected.", ms),
                fix: "Check network quality metrics. High RTT or packet loss will cause slow DTLS negotiation.".to_string(),
            });
        }
    }

    // ── No media flow ─────────────────────────────────────────────────────────
    if frames_sent == 0
        && (*ice_outcome == IceConnectionOutcome::Connected
            || *ice_outcome == IceConnectionOutcome::Completed)
        && *dtls_outcome == DtlsOutcome::Connected
    {
        entries.push(DiagnosisEntry {
            severity: TestStatus::Warn,
            category: "Media".to_string(),
            message: "ICE and DTLS succeeded but no media frames were sent to this viewer.".to_string(),
            fix: "Check that the GStreamer pipeline is producing video and the tee/sink pads are correctly linked.".to_string(),
        });
    }

    // ── Signaling reachability ────────────────────────────────────────────────
    if let Some(probe) = network_probe {
        if probe.signaling_result.status == TestStatus::Fail {
            entries.push(DiagnosisEntry {
                severity: TestStatus::Fail,
                category: "Signaling".to_string(),
                message: format!(
                    "Signaling server '{}' was unreachable at startup.",
                    probe.signaling_result.url
                ),
                fix: "Verify SIGNALING_SERVER_URL is correct and the server is running. Check TCP/80/443 firewall rules.".to_string(),
            });
        }
    }

    entries
}
