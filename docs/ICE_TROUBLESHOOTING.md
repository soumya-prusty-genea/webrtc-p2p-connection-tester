# ICE Connection Failure Runbook — local-live-streamer

This is a diagnostic playbook for when a viewer fails to establish a WebRTC connection
to the `local-live-streamer` gateway (P2P mode, socket.io signaling). It assumes you are
using `ui/local-live-tester.html` — the "Local — P2P Gateway" panel — to inspect the
failure.

## 1. Read the diagnostics panel first

Before changing anything, connect with the tester and check three things, in order:

1. **Local candidates table** — did the browser produce a `SRFLX` (server reflexive) row?
2. **Remote candidates table** — what candidate types did the gateway advertise, and are
   any of them private/Docker-internal IPs?
3. **Candidate pairs table → Notes column** — look for `remote Docker IP`, `private host`,
   or a pair stuck at `WAITING`/`FAILED` with 0 bytes exchanged.

These three answers determine which of the four categories below you're in — the fix is
different for each, and only one of them is something the *viewer's* machine/router can
fix.

## 2. The four failure categories

| # | Signature | Who fixes it | Fixable client-side? |
|---|-----------|--------------|----------------------|
| A | No `SRFLX` in **local** candidates | Viewer's network/firewall | Yes |
| B | Remote candidate is a Docker bridge IP (`172.17.x`, `172.18.x`, `192.168.65.x`) | Gateway operator | No |
| C | Only `HOST` candidates on both sides, on different networks | Either side / router | Partially |
| D | Local `SRFLX` present, but selected pair never succeeds (symmetric NAT) | Nobody, without TURN | No |

### A. Viewer produced no server-reflexive candidate

**What it means:** the browser's STUN request never got a response. Outbound UDP to the
STUN server (port 19302/3478) is being blocked or dropped somewhere on the viewer's
network path.

**Steps, in order:**
1. Disable any VPN client and retry — many VPNs only tunnel TCP and silently drop UDP.
2. Switch networks (e.g. a mobile hotspot) and retry. If `SRFLX` now appears, the original
   network's firewall is the cause, not the machine.
3. Disable browser privacy/ad-block extensions that block WebRTC ("WebRTC Leak Prevent",
   uBlock Origin's WebRTC option, etc.) and retry.
4. Check corporate endpoint security (Zscaler, Netskope, CrowdStrike, Defender browser
   extension) — some explicitly block WebRTC UDP. Ask IT to allowlist outbound UDP to the
   STUN server's IP/port.
5. Ask network/IT to confirm outbound UDP isn't default-denied (many corporate firewalls
   allow only TCP 80/443 outbound).
6. Independent confirmation: open `https://webrtc.github.io/samples/src/content/peerconnection/trickle-ice/`
   (an unrelated public demo) with the same STUN server. If it *also* produces no `SRFLX`,
   this conclusively proves it's the network, not this app.

### B. Gateway is advertising a Docker-internal IP

**What it means:** the gateway/SFU container is running on the default Docker bridge
network and is leaking its internal container IP (`172.17.0.x`, `172.18.0.x`, or Docker
Desktop's `192.168.65.x`) as an ICE candidate. No viewer, anywhere, can ever reach that
address — it doesn't exist outside the container's network namespace.

**Fix (gateway side only — not fixable by the viewer):**
- Run the gateway/media container with `network_mode: host` in `docker-compose.yml`
  (or `--network host` on `docker run`), so it advertises the host's real interface IP.
- If host networking isn't an option, add STUN (for real NAT traversal) and/or TURN
  relay candidates to the gateway's ICE configuration so it advertises a reachable
  address instead of the Docker-internal one.

### C. Only host (private LAN) candidates on both sides

**What it means:** neither side produced a STUN/TURN candidate — only raw private IPs
(`10.x`, `192.168.x`, `172.16–31.x`). This only works if viewer and gateway are on the
literal same LAN.

**Steps:**
1. Confirm whether viewer and gateway are actually meant to be on the same network. If
   yes, and it still fails, see the router checklist in §3.
2. If cross-network viewing is required, the gateway needs STUN configured (to produce a
   `SRFLX` candidate) and ideally TURN as a fallback — this is a gateway-side config
   change, not a viewer fix.

### D. Symmetric NAT (STUN present, pair still fails)

**What it means:** the viewer *did* get a `SRFLX` candidate, but the ICE pair never
succeeds against the gateway's address. The viewer is very likely behind a **symmetric
NAT** — common on some corporate networks and certain mobile carriers — which assigns a
different external port per destination. The STUN-discovered mapping is only valid for
talking to the STUN server, not the gateway.

**This is an architectural limit, not a settings problem.** Pure STUN/P2P cannot solve
symmetric NAT by design. No router setting or firewall change on the viewer's side fixes
this — only a TURN relay (which both sides can reach) does. If your deployment must
support these networks, add a TURN server (ideally `turns:` over TCP/443 so it also
survives networks that block raw UDP) instead of trying to fix client settings.

## 3. Router / user-system settings to try (Category A/C only)

These only apply when the failure is a *reachability* problem (Categories A or C above),
not Docker misconfiguration (B) or symmetric NAT (D):

| Setting | Change | Why |
|---|---|---|
| AP / client isolation (Wi-Fi) | Disable | Isolation prevents devices on the same Wi-Fi from reaching each other directly — breaks same-LAN host-candidate connectivity. |
| UPnP / NAT-PMP | Enable | Lets the router auto-open the UDP ports WebRTC needs; without it, a device behind NAT may not be reachable inbound at all. |
| Port forwarding | Forward the gateway's media UDP port range to the gateway host | Needed if the gateway is on a home/office network without UPnP and needs to be reachable from outside. |
| Double NAT (modem + router both NAT'ing) | Put one device in bridge mode | Two NAT layers stacked often breaks STUN mapping and UPnP; only one device should be doing NAT. |
| DMZ | Only as a last resort, understanding the security tradeoff | Exposes the gateway host directly to the internet — bypasses NAT/firewall issues but removes a layer of protection. |
| VPN / strict firewall | Temporarily disable to test | Confirms whether the VPN or firewall (not the network itself) is the actual blocker. |
| Corporate network UDP policy | Ask IT to allow UDP/WebRTC, or switch to TURN over TLS | Many corporate networks default-deny UDP outbound except DNS. |

## 4. Quick reference — confirming a fix worked

After any change above, reconnect via the tester and re-check the same three things from
§1:
- Local candidates: does `SRFLX` now appear (if it didn't before)?
- Remote candidates: is the gateway advertising a real (non-Docker, non-private-unless-LAN) address?
- Candidate pairs: does one pair reach `SUCCESS` with `SELECTED, nominated` in Notes, and
  non-zero `Received`/`Sent` bytes?

If all three are green, the connection has actually resolved — not just "ICE state:
connected" with no data flowing, which can still indicate a stalled/misleading pair.
