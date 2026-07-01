use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

use crate::tester::report::{
    NatType, NetworkProbeReport, SignalingProbeResult, StunProbeResult, TestStatus, TurnProbeResult,
};

// ─── STUN wire helpers ───────────────────────────────────────────────────────

const STUN_MAGIC_COOKIE: u32 = 0x2112A442;
const STUN_TIMEOUT_SECS: u64 = 3;

/// Build a minimal RFC 5389 Binding Request (20-byte header, no attributes).
fn build_stun_binding_request() -> ([u8; 20], [u8; 12]) {
    let mut msg = [0u8; 20];
    // Type = 0x0001 (Binding Request)
    msg[0] = 0x00;
    msg[1] = 0x01;
    // Message length = 0
    msg[2] = 0x00;
    msg[3] = 0x00;
    // Magic cookie
    msg[4..8].copy_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    // Transaction ID (12 random bytes)
    let tid: [u8; 12] = rand::random();
    msg[8..20].copy_from_slice(&tid);
    (msg, tid)
}

/// Parse a STUN Binding Response and extract the mapped address.
/// Returns `(ip_string, port)` or None on parse failure.
fn parse_stun_response(buf: &[u8], tid: &[u8; 12]) -> Option<(String, u16)> {
    if buf.len() < 20 {
        return None;
    }
    // Verify magic cookie and transaction ID
    let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if cookie != STUN_MAGIC_COOKIE {
        return None;
    }
    if &buf[8..20] != tid {
        return None;
    }
    // Message type must be Binding Response (0x0101) or Binding Success (0x0101)
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    if msg_type != 0x0101 {
        return None;
    }
    // Parse attributes
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    let mut offset = 20usize;
    let end = (20 + msg_len).min(buf.len());
    while offset + 4 <= end {
        let attr_type = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
        let attr_len  = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
        offset += 4;
        if offset + attr_len > end {
            break;
        }
        match attr_type {
            // MAPPED-ADDRESS (0x0001)
            0x0001 if attr_len >= 8 => {
                let family = buf[offset + 1];
                let port   = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]);
                if family == 0x01 {
                    // IPv4
                    let ip = format!("{}.{}.{}.{}",
                        buf[offset + 4], buf[offset + 5],
                        buf[offset + 6], buf[offset + 7]);
                    return Some((ip, port));
                }
            }
            // XOR-MAPPED-ADDRESS (0x0020)
            0x0020 if attr_len >= 8 => {
                let family = buf[offset + 1];
                let xport  = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]);
                let port   = xport ^ ((STUN_MAGIC_COOKIE >> 16) as u16);
                if family == 0x01 {
                    // IPv4 XOR
                    let xip = u32::from_be_bytes([
                        buf[offset + 4], buf[offset + 5],
                        buf[offset + 6], buf[offset + 7],
                    ]);
                    let ip_raw = xip ^ STUN_MAGIC_COOKIE;
                    let ip = format!("{}.{}.{}.{}",
                        (ip_raw >> 24) & 0xFF,
                        (ip_raw >> 16) & 0xFF,
                        (ip_raw >>  8) & 0xFF,
                         ip_raw        & 0xFF,
                    );
                    return Some((ip, port));
                }
            }
            _ => {}
        }
        // Attributes are padded to 4-byte boundary
        offset += (attr_len + 3) & !3;
    }
    None
}

// ─── STUN probe ──────────────────────────────────────────────────────────────

pub async fn probe_stun(server: &str) -> StunProbeResult {
    // Strip scheme prefix: stun:// or stuns://
    let host_port = server
        .trim_start_matches("stuns://")
        .trim_start_matches("stun://");

    let addr: SocketAddr = match tokio::net::lookup_host(host_port).await
        .ok()
        .and_then(|mut it| it.next())
    {
        Some(a) => a,
        None => return StunProbeResult {
            server: server.to_string(),
            status: TestStatus::Fail,
            reflexive_address: None,
            reflexive_port: None,
            rtt_ms: None,
            error: Some(format!("DNS resolution failed for {}", host_port)),
        },
    };

    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => return StunProbeResult {
            server: server.to_string(),
            status: TestStatus::Fail,
            reflexive_address: None,
            reflexive_port: None,
            rtt_ms: None,
            error: Some(format!("UDP bind failed: {}", e)),
        },
    };

    let (request, tid) = build_stun_binding_request();
    let start = Instant::now();
    if let Err(e) = sock.send_to(&request, addr).await {
        return StunProbeResult {
            server: server.to_string(),
            status: TestStatus::Fail,
            reflexive_address: None,
            reflexive_port: None,
            rtt_ms: None,
            error: Some(format!("UDP send failed: {}", e)),
        };
    }

    let mut buf = [0u8; 512];
    match tokio::time::timeout(
        Duration::from_secs(STUN_TIMEOUT_SECS),
        sock.recv_from(&mut buf),
    ).await {
        Ok(Ok((n, _from))) => {
            let rtt_ms = start.elapsed().as_millis() as u64;
            if let Some((ip, port)) = parse_stun_response(&buf[..n], &tid) {
                StunProbeResult {
                    server: server.to_string(),
                    status: TestStatus::Pass,
                    reflexive_address: Some(ip),
                    reflexive_port: Some(port),
                    rtt_ms: Some(rtt_ms),
                    error: None,
                }
            } else {
                StunProbeResult {
                    server: server.to_string(),
                    status: TestStatus::Warn,
                    reflexive_address: None,
                    reflexive_port: None,
                    rtt_ms: Some(rtt_ms),
                    error: Some("Received response but could not parse mapped address".to_string()),
                }
            }
        }
        Ok(Err(e)) => StunProbeResult {
            server: server.to_string(),
            status: TestStatus::Fail,
            reflexive_address: None,
            reflexive_port: None,
            rtt_ms: None,
            error: Some(format!("UDP recv error: {}", e)),
        },
        Err(_) => StunProbeResult {
            server: server.to_string(),
            status: TestStatus::Fail,
            reflexive_address: None,
            reflexive_port: None,
            rtt_ms: None,
            error: Some(format!("Timeout after {}s — STUN server unreachable or UDP blocked", STUN_TIMEOUT_SECS)),
        },
    }
}

// ─── NAT type detection ──────────────────────────────────────────────────────

/// Receive a STUN response whose 12-byte transaction ID matches `expected_tid`.
/// Discards packets with wrong TIDs (stale responses from a previous probe on the
/// same socket) and retries until the deadline expires.
async fn recv_stun_with_tid(
    sock: &UdpSocket,
    expected_tid: &[u8; 12],
    timeout_secs: u64,
) -> Option<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let rem = deadline.saturating_duration_since(tokio::time::Instant::now());
        if rem.is_zero() {
            return None;
        }
        let mut buf = [0u8; 512];
        match tokio::time::timeout(rem, sock.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if n >= 20 && &buf[8..20] == expected_tid {
                    return Some(buf[..n].to_vec());
                }
                // Wrong TID — stale packet from a previous probe; keep waiting
            }
            _ => return None,
        }
    }
}

/// Probe two STUN servers from the **same** local UDP socket so the reflexive
/// (ip, port) pair is directly comparable across destinations.
///
/// Classification:
///   - reflexive port == local socket port        → `OpenInternet` (no NAT or port-preserving)
///   - same (ip, port) to both servers            → `FullCone`     (endpoint-independent mapping)
///   - same ip, different port per destination    → `SymmetricNat` (port-dependent mapping)
///   - different ip per destination               → `SymmetricNat` (address-dependent mapping)
///   - both probes failed                         → `UdpBlocked`
///   - one probe failed                           → `Unknown`
///
/// This avoids the flaw of comparing ports across two *different* sockets
/// (different source ports → different mappings regardless of NAT type).
async fn classify_nat_from_same_socket(
    server1: &str,
    server2: &str,
) -> (StunProbeResult, StunProbeResult, NatType) {
    let host1 = server1.trim_start_matches("stuns://").trim_start_matches("stun://");
    let host2 = server2.trim_start_matches("stuns://").trim_start_matches("stun://");

    // DNS in parallel
    let (addr1_res, addr2_res) = tokio::join!(
        tokio::net::lookup_host(host1),
        tokio::net::lookup_host(host2),
    );

    let addr1: SocketAddr = match addr1_res.ok().and_then(|mut it| it.next()) {
        Some(a) => a,
        None => {
            return (
                StunProbeResult {
                    server: server1.to_string(), status: TestStatus::Fail,
                    reflexive_address: None, reflexive_port: None, rtt_ms: None,
                    error: Some(format!("DNS resolution failed for {}", host1)),
                },
                probe_stun(server2).await,
                NatType::Unknown,
            );
        }
    };
    let addr2: SocketAddr = match addr2_res.ok().and_then(|mut it| it.next()) {
        Some(a) => a,
        None => {
            return (
                probe_stun(server1).await,
                StunProbeResult {
                    server: server2.to_string(), status: TestStatus::Fail,
                    reflexive_address: None, reflexive_port: None, rtt_ms: None,
                    error: Some(format!("DNS resolution failed for {}", host2)),
                },
                NatType::Unknown,
            );
        }
    };

    // Bind ONE socket for both probes
    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => {
            let err = format!("UDP bind failed: {}", e);
            return (
                StunProbeResult { server: server1.to_string(), status: TestStatus::Fail,
                    reflexive_address: None, reflexive_port: None, rtt_ms: None, error: Some(err.clone()) },
                StunProbeResult { server: server2.to_string(), status: TestStatus::Fail,
                    reflexive_address: None, reflexive_port: None, rtt_ms: None, error: Some(err) },
                NatType::UdpBlocked,
            );
        }
    };

    // Record local port to detect "open internet" (reflexive port == local port)
    let local_port = sock.local_addr().ok().map(|a| a.port());

    // ── Probe server 1 ───────────────────────────────────────────────────────
    let (req1, tid1) = build_stun_binding_request();
    let t1 = Instant::now();
    let r1_data = if sock.send_to(&req1, addr1).await.is_ok() {
        recv_stun_with_tid(&sock, &tid1, STUN_TIMEOUT_SECS).await
    } else {
        None
    };
    let rtt1 = t1.elapsed().as_millis() as u64;
    let r1 = match r1_data.and_then(|buf| parse_stun_response(&buf, &tid1)) {
        Some((ip, port)) => StunProbeResult {
            server: server1.to_string(), status: TestStatus::Pass,
            reflexive_address: Some(ip), reflexive_port: Some(port),
            rtt_ms: Some(rtt1), error: None,
        },
        None => StunProbeResult {
            server: server1.to_string(), status: TestStatus::Fail,
            reflexive_address: None, reflexive_port: None,
            rtt_ms: Some(rtt1),
            error: Some(format!("Timeout after {}s — STUN server unreachable or UDP blocked", STUN_TIMEOUT_SECS)),
        },
    };

    // ── Probe server 2 (same socket) ─────────────────────────────────────────
    let (req2, tid2) = build_stun_binding_request();
    let t2 = Instant::now();
    let r2_data = if sock.send_to(&req2, addr2).await.is_ok() {
        recv_stun_with_tid(&sock, &tid2, STUN_TIMEOUT_SECS).await
    } else {
        None
    };
    let rtt2 = t2.elapsed().as_millis() as u64;
    let r2 = match r2_data.and_then(|buf| parse_stun_response(&buf, &tid2)) {
        Some((ip, port)) => StunProbeResult {
            server: server2.to_string(), status: TestStatus::Pass,
            reflexive_address: Some(ip), reflexive_port: Some(port),
            rtt_ms: Some(rtt2), error: None,
        },
        None => StunProbeResult {
            server: server2.to_string(), status: TestStatus::Fail,
            reflexive_address: None, reflexive_port: None,
            rtt_ms: Some(rtt2),
            error: Some(format!("Timeout after {}s — STUN server unreachable or UDP blocked", STUN_TIMEOUT_SECS)),
        },
    };

    // ── Classify NAT from the two reflexive (ip, port) mappings ──────────────
    let nat_type = match (
        r1.reflexive_address.as_deref(),
        r1.reflexive_port,
        r2.reflexive_address.as_deref(),
        r2.reflexive_port,
    ) {
        (Some(ip1), Some(p1), Some(ip2), Some(p2)) => {
            if ip1 != ip2 {
                // Different public IPs per destination → address-dependent (Symmetric)
                NatType::SymmetricNat
            } else if p1 != p2 {
                // Same IP, different port per destination → port-dependent (Symmetric)
                NatType::SymmetricNat
            } else if local_port == Some(p1) {
                // Reflexive port equals the local socket port → no NAT (or port-preserving NAT)
                NatType::OpenInternet
            } else {
                // Same (ip, port) to different destinations → endpoint-independent (Cone family)
                NatType::FullCone
            }
        }
        (None, _, None, _) => NatType::UdpBlocked,
        _ => NatType::Unknown, // one probe succeeded, one failed
    };

    (r1, r2, nat_type)
}

/// Infer NAT type from pre-collected STUN results (fallback / legacy path).
/// Prefer `classify_nat_from_same_socket` for new callers — this function can
/// only compare IPs because results from different sockets have different source
/// ports and their reflexive ports are not comparable.
pub fn detect_nat_type(results: &[StunProbeResult]) -> NatType {
    let passing: Vec<&StunProbeResult> = results.iter()
        .filter(|r| r.status == TestStatus::Pass)
        .collect();

    if passing.is_empty() {
        return NatType::UdpBlocked;
    }
    if passing.len() < 2 {
        return NatType::Unknown;
    }

    match (&passing[0].reflexive_address, &passing[1].reflexive_address) {
        (Some(ip0), Some(ip1)) => {
            if ip0 != ip1 { NatType::SymmetricNat } else { NatType::FullCone }
        }
        _ => NatType::Unknown,
    }
}

// ─── TURN probe (RFC 5766 long-term credential Allocate) ──────────────────────

type HmacSha1 = hmac::Hmac<sha1::Sha1>;

/// Append a STUN/TURN attribute (Type-Length-Value, padded to 4 bytes).
fn put_attr(buf: &mut Vec<u8>, attr_type: u16, value: &[u8]) {
    buf.extend_from_slice(&attr_type.to_be_bytes());
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
    while buf.len() % 4 != 0 {
        buf.push(0);
    }
}

/// Iterate STUN attributes, returning (type, value) pairs.
fn iter_attrs(buf: &[u8]) -> Vec<(u16, Vec<u8>)> {
    let mut out = Vec::new();
    if buf.len() < 20 {
        return out;
    }
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    let end = (20 + msg_len).min(buf.len());
    let mut offset = 20usize;
    while offset + 4 <= end {
        let attr_type = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
        let attr_len = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
        offset += 4;
        if offset + attr_len > end {
            break;
        }
        out.push((attr_type, buf[offset..offset + attr_len].to_vec()));
        offset += (attr_len + 3) & !3;
    }
    out
}

/// Build an Allocate request. When `auth` is Some((username, realm, nonce, password))
/// the message carries USERNAME/REALM/NONCE + a long-term-credential
/// MESSAGE-INTEGRITY computed per RFC 5389 §15.4.
fn build_allocate(tid: &[u8; 12], auth: Option<(&str, &str, &str, &str)>) -> Vec<u8> {
    let mut attrs = Vec::new();
    // REQUESTED-TRANSPORT (0x0019): UDP = 17.
    put_attr(&mut attrs, 0x0019, &[17, 0, 0, 0]);

    if let Some((username, realm, nonce, password)) = auth {
        put_attr(&mut attrs, 0x0006, username.as_bytes()); // USERNAME
        put_attr(&mut attrs, 0x0014, realm.as_bytes()); // REALM
        put_attr(&mut attrs, 0x0015, nonce.as_bytes()); // NONCE

        // key = MD5(username ":" realm ":" password)
        use md5::Digest;
        let key = md5::Md5::digest(format!("{}:{}:{}", username, realm, password).as_bytes());

        // Message used for HMAC has its length field include the upcoming
        // 24-byte MESSAGE-INTEGRITY attribute but does NOT contain it yet.
        let mut msg = Vec::with_capacity(20 + attrs.len() + 24);
        msg.extend_from_slice(&0x0003u16.to_be_bytes());
        msg.extend_from_slice(&(((attrs.len() + 24) as u16)).to_be_bytes());
        msg.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
        msg.extend_from_slice(tid);
        msg.extend_from_slice(&attrs);

        use hmac::Mac;
        let mut mac = HmacSha1::new_from_slice(&key).expect("HMAC accepts any key length");
        mac.update(&msg);
        let integrity = mac.finalize().into_bytes();

        put_attr(&mut msg, 0x0008, &integrity); // MESSAGE-INTEGRITY
        return msg;
    }

    // Unauthenticated probe: expect 401 with REALM/NONCE from a real TURN server.
    let mut msg = Vec::with_capacity(20 + attrs.len());
    msg.extend_from_slice(&0x0003u16.to_be_bytes());
    msg.extend_from_slice(&((attrs.len() as u16)).to_be_bytes());
    msg.extend_from_slice(&STUN_MAGIC_COOKIE.to_be_bytes());
    msg.extend_from_slice(tid);
    msg.extend_from_slice(&attrs);
    msg
}

/// Read TURN credentials for `turn_url` from the environment.
/// Looks at WEBRTC_TURN_USERNAME / WEBRTC_TURN_PASSWORD (or _CREDENTIAL).
fn turn_credentials() -> Option<(String, String)> {
    let user = std::env::var("WEBRTC_TURN_USERNAME").ok().filter(|v| !v.is_empty())?;
    let pass = std::env::var("WEBRTC_TURN_PASSWORD")
        .or_else(|_| std::env::var("WEBRTC_TURN_CREDENTIAL"))
        .ok()
        .filter(|v| !v.is_empty())?;
    Some((user, pass))
}

/// Validate a TURN server. When credentials are configured, performs a full
/// authenticated Allocate and only returns `Pass` if the server hands back a
/// relayed address (XOR-RELAYED-ADDRESS). Without credentials it can only
/// confirm reachability (`Warn`).
pub async fn probe_turn(turn_url: &str) -> TurnProbeResult {
    let stripped = turn_url
        .trim_start_matches("turns:")
        .trim_start_matches("turn:");
    let host_port = stripped.split('?').next().unwrap_or(stripped);

    let addr: SocketAddr = match tokio::net::lookup_host(host_port).await
        .ok()
        .and_then(|mut it| it.next())
    {
        Some(a) => a,
        None => return TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some(format!("DNS resolution failed for {}", host_port)),
        },
    };

    let sock = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(e) => return TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some(format!("UDP bind failed: {}", e)),
        },
    };

    // Step 1: unauthenticated Allocate -> expect 401 with REALM + NONCE.
    let tid: [u8; 12] = rand::random();
    let req = build_allocate(&tid, None);
    let start = Instant::now();
    if let Err(e) = sock.send_to(&req, addr).await {
        return TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some(format!("UDP send failed: {}", e)),
        };
    }

    let mut buf = [0u8; 1024];
    let n = match tokio::time::timeout(Duration::from_secs(STUN_TIMEOUT_SECS), sock.recv_from(&mut buf)).await {
        Ok(Ok((n, _))) => n,
        Ok(Err(e)) => return TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some(format!("UDP recv error: {}", e)),
        },
        Err(_) => return TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some("Timeout — TURN server unreachable or UDP blocked".to_string()),
        },
    };

    let attrs = iter_attrs(&buf[..n]);
    let realm = attrs.iter().find(|(t, _)| *t == 0x0014).map(|(_, v)| String::from_utf8_lossy(v).to_string());
    let nonce = attrs.iter().find(|(t, _)| *t == 0x0015).map(|(_, v)| String::from_utf8_lossy(v).to_string());

    // No credentials configured: we can only confirm the server is a live TURN
    // server (it challenged us with REALM/NONCE). Be honest: this is Warn, not Pass.
    let creds = turn_credentials();
    if creds.is_none() {
        let rtt_ms = start.elapsed().as_millis() as u64;
        return if realm.is_some() && nonce.is_some() {
            TurnProbeResult {
                server: turn_url.to_string(),
                status: TestStatus::Warn,
                rtt_ms: Some(rtt_ms),
                error: Some("TURN server reachable and requires auth (set WEBRTC_TURN_USERNAME/PASSWORD to verify relay allocation)".to_string()),
            }
        } else {
            TurnProbeResult {
                server: turn_url.to_string(),
                status: TestStatus::Warn,
                rtt_ms: Some(rtt_ms),
                error: Some("Got a response but no REALM/NONCE challenge — may not be a TURN server".to_string()),
            }
        };
    }

    let (username, password) = creds.unwrap();
    let (realm, nonce) = match (realm, nonce) {
        (Some(r), Some(no)) => (r, no),
        _ => {
            let rtt_ms = start.elapsed().as_millis() as u64;
            return TurnProbeResult {
                server: turn_url.to_string(),
                status: TestStatus::Fail,
                rtt_ms: Some(rtt_ms),
                error: Some("TURN server did not return REALM/NONCE; cannot authenticate".to_string()),
            };
        }
    };

    // Step 2: authenticated Allocate -> expect 0x0103 success + XOR-RELAYED-ADDRESS.
    let tid2: [u8; 12] = rand::random();
    let auth_req = build_allocate(&tid2, Some((&username, &realm, &nonce, &password)));
    if let Err(e) = sock.send_to(&auth_req, addr).await {
        return TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some(format!("UDP send (auth) failed: {}", e)),
        };
    }

    let mut buf2 = [0u8; 1024];
    match tokio::time::timeout(Duration::from_secs(STUN_TIMEOUT_SECS), sock.recv_from(&mut buf2)).await {
        Ok(Ok((n2, _))) => {
            let rtt_ms = start.elapsed().as_millis() as u64;
            let msg_type = u16::from_be_bytes([buf2[0], buf2[1]]);
            if msg_type == 0x0103 {
                // Allocate success: confirm a relayed address was granted.
                let has_relay = iter_attrs(&buf2[..n2]).iter().any(|(t, _)| *t == 0x0016);
                if has_relay {
                    TurnProbeResult {
                        server: turn_url.to_string(),
                        status: TestStatus::Pass,
                        rtt_ms: Some(rtt_ms),
                        error: None,
                    }
                } else {
                    TurnProbeResult {
                        server: turn_url.to_string(),
                        status: TestStatus::Warn,
                        rtt_ms: Some(rtt_ms),
                        error: Some("Allocate succeeded but no relayed address returned".to_string()),
                    }
                }
            } else {
                // Error response — surface the ERROR-CODE if present.
                let err_code = iter_attrs(&buf2[..n2])
                    .iter()
                    .find(|(t, _)| *t == 0x0009)
                    .and_then(|(_, v)| {
                        if v.len() >= 4 {
                            Some(v[2] as u16 * 100 + v[3] as u16)
                        } else {
                            None
                        }
                    });
                TurnProbeResult {
                    server: turn_url.to_string(),
                    status: TestStatus::Fail,
                    rtt_ms: Some(rtt_ms),
                    error: Some(format!(
                        "Authenticated Allocate rejected (error code {:?}); check TURN credentials",
                        err_code
                    )),
                }
            }
        }
        Ok(Err(e)) => TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some(format!("UDP recv (auth) error: {}", e)),
        },
        Err(_) => TurnProbeResult {
            server: turn_url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some("Timeout waiting for authenticated Allocate response".to_string()),
        },
    }
}

// ─── Signaling probe ─────────────────────────────────────────────────────────

/// Probe the signaling server by attempting a TCP connection.
pub async fn probe_signaling(url: &str) -> SignalingProbeResult {
    // Extract host:port from URL like http(s)://host:port/path or ws(s)://host:port/path
    let without_scheme = url
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("wss://")
        .trim_start_matches("ws://");
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    // Append default port if missing
    let host_port_with_port = if host_port.contains(':') {
        host_port.to_string()
    } else if url.starts_with("https://") || url.starts_with("wss://") {
        format!("{}:443", host_port)
    } else {
        format!("{}:80", host_port)
    };

    let start = Instant::now();
    match tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(&host_port_with_port),
    ).await {
        Ok(Ok(_)) => SignalingProbeResult {
            url: url.to_string(),
            status: TestStatus::Pass,
            rtt_ms: Some(start.elapsed().as_millis() as u64),
            error: None,
        },
        Ok(Err(e)) => SignalingProbeResult {
            url: url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some(format!("TCP connect failed: {}", e)),
        },
        Err(_) => SignalingProbeResult {
            url: url.to_string(),
            status: TestStatus::Fail,
            rtt_ms: None,
            error: Some("Timeout — signaling server unreachable".to_string()),
        },
    }
}

// ─── run_all_probes ───────────────────────────────────────────────────────────

/// Run all network probes in parallel and return consolidated report.
pub async fn run_all_probes(
    stun_server: &str,
    turn_servers: &[String],
    signaling_url: &str,
) -> NetworkProbeReport {
    // Use a single UDP socket for both STUN probes so the reflexive (ip, port)
    // pairs are directly comparable for NAT classification. The fallback STUN
    // server is configurable via WEBRTC_STUN_FALLBACK.
    let fallback_stun = std::env::var("WEBRTC_STUN_FALLBACK")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "stun.cloudflare.com:3478".to_string());

    // Signaling probe is independent — run it in parallel with STUN NAT probes.
    let (stun_nat, signaling_result) = tokio::join!(
        classify_nat_from_same_socket(stun_server, &fallback_stun),
        probe_signaling(signaling_url),
    );
    let (r1, r2, nat_type) = stun_nat;
    let stun_results = vec![r1, r2];

    let mut turn_results = Vec::new();
    for turn_url in turn_servers {
        turn_results.push(probe_turn(turn_url).await);
    }

    NetworkProbeReport {
        stun_results,
        nat_type,
        turn_results,
        signaling_result,
    }
}
