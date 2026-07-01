# webrtc-connection-tester-v2

An end-to-end **WebRTC connectivity tester** for the gateway → viewer media path.
It runs as a gateway-side SFU that ingests camera frames from ZeroMQ and serves
them to viewers over WebRTC (via a Socket.IO signaling server), while collecting
deep per-connection diagnostics. A companion **headless viewer agent** connects
as a real viewer, decodes the stream, and reports back whether video is actually
displayable — proving the P2P path works, not just that bytes were sent.

The tool answers one question per viewer connection: **can video flow from the
gateway to a (potentially remote) viewer over WebRTC, and is it displayable?**
It emits a machine-readable PASS / DEGRADED / FAIL verdict for each connection.

## Components

| Binary | Path | Role |
| --- | --- | --- |
| `webrtc-connection-tester` | `src/main.rs` | Gateway/SFU: ingests frames, serves viewers, runs diagnostics, network probes, recovery |
| `webrtc-viewer-agent` | `src/viewer_agent.rs` | Headless viewer: connects via signaling, decodes video, reports decode telemetry |
| `test-auth` | `src/test_auth.rs` | Standalone signaling-server JWT auth check |

## What it verifies

1. **Network reachability** (`src/tester/network_probe.rs`): STUN reflexive
   address + RTT, honest NAT-mapping classification (Symmetric vs cone), an
   **authenticated TURN Allocate** (proves a real relay candidate when
   credentials are supplied), and a signaling-server TCP probe.
2. **Connection lifecycle** (`src/tester/connection_monitor.rs`): timestamps and
   outcomes for ICE gathering, candidate exchange, ICE connectivity, the DTLS
   handshake, SRTP readiness, and media flow — with a root-cause tree.
3. **Real egress** (not just local frame counts): `webrtcbin get-stats` is read
   programmatically (`outbound-rtp.packets-sent`, plus the remote peer's RTCP
   `remote-inbound-rtp` feedback) so a silent stall — bytes "sent" locally but
   never acknowledged by the remote — is detected.
4. **Viewer-side display**: the viewer agent decodes frames and reports
   `frames_decoded` / `decode_fps` / `first_decoded_ms` to the gateway over the
   `viewer_telemetry` signaling event, which folds into the connection report.

## Reliability / recovery

- **Bounded ICE restart**: when a viewer's ICE goes `Disconnected` (e.g. the
  remote peer changes Wi-Fi/cellular), the gateway issues a fresh
  `ice-restart` offer, with bounded attempts and a cooldown. A healthy
  reconnect resets the budget.
- **Unified restart authority**: pipeline-error restarts and DTLS-warning
  restarts both go through one cooldown gate, so the pipeline is never thrashed.
- **Loss resilience**: RTCP feedback (NACK/RTX, PLI/FIR, transport-cc) is
  advertised in the RTP caps so webrtcbin retransmits lost packets and honors
  keyframe requests.

## End-to-end test loop

```text
 ZMQ frames ─► [gateway: webrtc-connection-tester] ─► WebRTC ─► [webrtc-viewer-agent]
                         ▲                                              │
                         └────────── viewer_telemetry (decode stats) ◄─┘
                                   (Socket.IO signaling server)
```

1. Start the gateway (`webrtc-connection-tester`) so it registers the stream.
2. Start `webrtc-viewer-agent` with `VIEWER_STREAM_ID` set to the room/stream id.
3. The agent negotiates a `recvonly` session, decodes video, and streams decode
   telemetry back. The gateway prints a per-connection report + verdict on
   teardown.

### Running the viewer agent

```bash
SIGNALING_SERVER_URL=wss://signaling.dev-sequr.io/gateway \
VIEWER_STREAM_ID=<room_name> \
WEBRTC_STUN_SERVER=stun://stun.l.google.com:19302 \
VIEWER_TEST_DURATION_SECS=30 \
cargo run --bin webrtc-viewer-agent
```

Exit code `0` = at least one frame decoded (PASS); `2` = no frames decoded (FAIL).

## Report format

On viewer teardown the gateway logs:

- A human-readable report (`ConnectionDiagnosticReport::print`) including the
  egress line, viewer-agent line, and `>>> VERDICT <<<`.
- A single-line JSON record prefixed `[test_report]` (`emit_json_report`). If
  `WEBRTC_TEST_REPORT_DIR` is set, a pretty `<room>_<viewer>.json` is also written.

Verdict semantics:

- **PASS** — handshake completed and delivery was confirmed (viewer decoded
  frames, or the remote acknowledged packets via RTCP) with acceptable loss.
- **DEGRADED** — connected and media was produced, but end-to-end delivery is
  unconfirmed or quality is impaired (high loss / freeze).
- **FAIL** — ICE/DTLS did not complete, or no media was delivered.

JSON shape (abridged):

```json
{
  "viewer_id": "...", "room_name": "...", "verdict": "PASS",
  "timing_ms": { "ice_connection": 120, "dtls_handshake": 240, "first_frame": 380 },
  "ice": { "outcome": "Connected", "nat_type": "FullCone", "path_assessment": "..." },
  "dtls": { "outcome": "Connected", "failure_reason": "None" },
  "egress": { "packets_sent": 5400, "remote_packets_received": 5380, "fraction_lost": 0.003, "egress_active": true },
  "viewer": { "frames_decoded": 720, "decode_fps": 24.0, "displayed": true },
  "media": { "rtp_packets_observed": 5380, "freeze_or_drop_detected": false },
  "root_cause": "...", "diagnosis": [ ... ]
}
```

## Environment variables

### Gateway core

| Var | Default | Description |
| --- | --- | --- |
| `SIGNALING_SERVER_URL` | — | Socket.IO signaling URL (e.g. `wss://host/gateway`) |
| `SFU_ID` | — | Gateway/SFU identity used for registration |
| `MQTT_HOST` / `MQTT_PORT` | `emqx` / `1883` | CloudEvent command broker |
| `VIEWER_LIMIT_MODE` | `camera` | `camera` or `gateway` viewer cap policy |
| `MAX_VIEWERS_PER_CAMERA` / `MAX_VIEWERS_PER_GATEWAY` | `10` / `50` | Viewer caps |
| `DEFAULT_VIDEO_CODEC` | `H264` | `H264` or `H265` |
| `STATE_FILE_PATH` | `sfu_state.json` | Persistent camera state |
| `RUST_LOG` | `info` | Log level |

### ICE / TURN / NAT

| Var | Default | Description |
| --- | --- | --- |
| `WEBRTC_STUN_SERVER` | `stun://stun.l.google.com:19302` | Primary STUN server |
| `WEBRTC_STUN_FALLBACK` | `stun.cloudflare.com:3478` | Second STUN server (needed for NAT classification) |
| `WEBRTC_TURN_SERVERS` | — | Comma-separated TURN URLs |
| `WEBRTC_TURN_USERNAME` / `WEBRTC_TURN_PASSWORD` | — | Long-term creds; enables authenticated TURN Allocate validation |

### Recovery / resilience

| Var | Default | Description |
| --- | --- | --- |
| `WEBRTC_LOSS_RESILIENCE` | `true` | Advertise NACK/RTX/PLI/FIR/transport-cc feedback |
| `ICE_RESTART_MAX_ATTEMPTS` | `3` | Max ICE restarts per viewer before deferring to the watchdog |
| `ICE_RESTART_COOLDOWN_SECS` | `5` | Min seconds between ICE restart attempts |
| `RECOVERY_RESTART_COOLDOWN_SECS` | `20` | Cooldown shared by error + DTLS-warning pipeline restarts |
| `RECOVERY_WATCHDOG_INTERVAL_SECS` | `3` | Output-stall watchdog tick |

### Reporting

| Var | Default | Description |
| --- | --- | --- |
| `WEBRTC_TEST_REPORT_DIR` | — | If set, write per-connection JSON reports here |

### Viewer agent (`webrtc-viewer-agent`)

| Var | Default | Description |
| --- | --- | --- |
| `VIEWER_STREAM_ID` | — (required) | Stream/room id to view |
| `VIEWER_ID` | random | Viewer identity |
| `VIEWER_TEST_DURATION_SECS` | `30` | Run duration before the verdict (`0` = run forever) |
| `VIEWER_TELEMETRY_INTERVAL_SECS` | `2` | How often decode telemetry is reported |
| `VIEWER_JWT` / `GATEWAY_JWT` | — | Optional bearer token for the signaling handshake |

### Secrets

Secrets are **not** committed. Put `JWT_SECRET` (and optional TURN credentials)
in a git-ignored `.env.local` (see `.env.local.example`); `docker-compose`
loads it automatically.

## Signaling protocol (viewer side)

The viewer agent mirrors the gateway handlers:

- viewer → server: `viewer_request` `{viewer_id, stream_id, session_id}`
- server → viewer: `webrtc_offer` / `webrtc_message_to_viewer` (SDP offer + ICE)
- viewer → server: `webrtc_answer` `{viewer_id, data:{type, sdp}}`
- viewer → server: `webrtc_ice_candidate` `{viewer_id, data:{candidate, sdpMLineIndex}}`
- viewer → server: `viewer_telemetry` `{viewer_id, data:{frames_decoded, decode_fps, first_decoded_ms}}`

## Build

```bash
cargo build --release
# or per binary
cargo run --bin webrtc-connection-tester
cargo run --bin webrtc-viewer-agent
```

GStreamer (with the `webrtc`, `rtp`, `nice`, `dtls`, `srtp` plugins) is required
at build and run time; see `docker/Dockerfile` and `docker-compose.yml`.
