use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub mod provider;

fn stream_metrics_log_interval_secs() -> u64 {
    static LOG_INTERVAL: OnceLock<u64> = OnceLock::new();
    *LOG_INTERVAL.get_or_init(|| {
        std::env::var("STREAM_METRICS_LOG_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30)
    })
}

fn total_stream_egress_log_interval_secs() -> u64 {
    static LOG_INTERVAL: OnceLock<u64> = OnceLock::new();
    *LOG_INTERVAL.get_or_init(|| {
        std::env::var("TOTAL_STREAM_EGRESS_LOG_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(15)
    })
}

#[derive(Debug)]
struct AggregateTrafficLogState {
    last_log_at: Instant,
    last_total_output_bytes: u64,
}

fn aggregate_traffic_state() -> &'static Mutex<AggregateTrafficLogState> {
    static STATE: OnceLock<Mutex<AggregateTrafficLogState>> = OnceLock::new();
    STATE.get_or_init(|| {
        Mutex::new(AggregateTrafficLogState {
            last_log_at: Instant::now(),
            last_total_output_bytes: 0,
        })
    })
}

fn aggregate_next_log_check_ms() -> &'static AtomicU64 {
    static NEXT_CHECK_MS: OnceLock<AtomicU64> = OnceLock::new();
    NEXT_CHECK_MS.get_or_init(|| AtomicU64::new(0))
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

#[derive(Debug, Clone, Copy)]
struct ForcedZeroOutputConfig {
    enabled: bool,
    cycle_secs: u64,
    active_secs: u64,
}

fn forced_zero_output_config() -> &'static ForcedZeroOutputConfig {
    static CONFIG: OnceLock<ForcedZeroOutputConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let cycle_secs = std::env::var("RECOVERY_TEST_FORCE_ZERO_OUTPUT_CYCLE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(30);

        let active_secs = std::env::var("RECOVERY_TEST_FORCE_ZERO_OUTPUT_ACTIVE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(10)
            .min(cycle_secs);

        ForcedZeroOutputConfig {
            enabled: env_bool("RECOVERY_TEST_FORCE_ZERO_OUTPUT_ENABLED", false),
            cycle_secs,
            active_secs,
        }
    })
}

#[derive(Debug)]
struct ForcedZeroOutputState {
    started_at: Instant,
    last_logged_cycle: Option<u64>,
}

fn forced_zero_output_state() -> &'static Mutex<ForcedZeroOutputState> {
    static STATE: OnceLock<Mutex<ForcedZeroOutputState>> = OnceLock::new();
    STATE.get_or_init(|| {
        Mutex::new(ForcedZeroOutputState {
            started_at: Instant::now(),
            last_logged_cycle: None,
        })
    })
}

fn forced_zero_output_active() -> bool {
    let cfg = forced_zero_output_config();
    if !cfg.enabled {
        return false;
    }

    let mut state = match forced_zero_output_state().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    let elapsed_secs = state.started_at.elapsed().as_secs();
    let cycle_index = elapsed_secs / cfg.cycle_secs;
    let cycle_offset = elapsed_secs % cfg.cycle_secs;
    let active = cycle_offset < cfg.active_secs;

    if active && state.last_logged_cycle != Some(cycle_index) {
        state.last_logged_cycle = Some(cycle_index);
        log::warn!(
            " [recovery_test] forcing output metrics/counters to zero for {}s every {}s (cycle={})",
            cfg.active_secs,
            cfg.cycle_secs,
            cycle_index
        );
    }

    active
}

#[derive(Default)]
pub struct FrameMetrics {
    pushed_frames: AtomicU64,
}

impl FrameMetrics {
    pub fn inc_pushed(&self) {
        self.pushed_frames.fetch_add(1, Ordering::Relaxed);
    }

    pub fn pushed(&self) -> u64 {
        self.pushed_frames.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamMetricsSnapshot {
    pub stream_id: String,
    pub room_name: String,
    pub camera_uuid: String,
    pub camera_name: String,
    pub started_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    pub last_logged_at: Option<DateTime<Utc>>,
    pub uptime_seconds: u64,
    pub input_frames_total: u64,
    pub output_frames_total: u64,
    pub output_packets_total: u64,
    pub input_bytes_total: u64,
    pub output_frames_bytes_total: u64,
    pub output_packets_bytes_total: u64,
    pub input_fps: f64,
    pub output_real_fps: f64,
    pub output_packet_rate: f64,
    pub input_bitrate_kbps: f64,
    pub output_frame_bitrate_kbps: f64,
    pub output_packet_bitrate_kbps: f64,
}

#[derive(Debug, Clone)]
struct StreamMetricsState {
    stream_id: String,
    room_name: String,
    camera_uuid: String,
    camera_name: String,
    started_at: DateTime<Utc>,
    last_updated_at: DateTime<Utc>,
    last_logged_at: Option<DateTime<Utc>>,
    input_frames_total: u64,
    output_frames_total: u64,
    output_packets_total: u64,
    input_bytes_total: u64,
    output_frames_bytes_total: u64,
    output_packets_bytes_total: u64,
    window_started_at: Instant,
    window_input_frames: u64,
    window_output_frames: u64,
    window_output_packets: u64,
    window_input_bytes: u64,
    window_output_frames_bytes: u64,
    window_output_packets_bytes: u64,
    input_fps: f64,
    output_real_fps: f64,
    output_packet_rate: f64,
    input_bitrate_kbps: f64,
    output_frame_bitrate_kbps: f64,
    output_packet_bitrate_kbps: f64,
}

impl StreamMetricsState {
    fn new(room_name: String, camera_uuid: String, camera_name: String) -> Self {
        let now = Utc::now();
        Self {
            stream_id: room_name.clone(),
            room_name,
            camera_uuid,
            camera_name,
            started_at: now,
            last_updated_at: now,
            last_logged_at: None,
            input_frames_total: 0,
            output_frames_total: 0,
            output_packets_total: 0,
            input_bytes_total: 0,
            output_frames_bytes_total: 0,
            output_packets_bytes_total: 0,
            window_started_at: Instant::now(),
            window_input_frames: 0,
            window_output_frames: 0,
            window_output_packets: 0,
            window_input_bytes: 0,
            window_output_frames_bytes: 0,
            window_output_packets_bytes: 0,
            input_fps: 0.0,
            output_real_fps: 0.0,
            output_packet_rate: 0.0,
            input_bitrate_kbps: 0.0,
            output_frame_bitrate_kbps: 0.0,
            output_packet_bitrate_kbps: 0.0,
        }
    }

    fn snapshot(&self) -> StreamMetricsSnapshot {
        StreamMetricsSnapshot {
            stream_id: self.stream_id.clone(),
            room_name: self.room_name.clone(),
            camera_uuid: self.camera_uuid.clone(),
            camera_name: self.camera_name.clone(),
            started_at: self.started_at,
            last_updated_at: self.last_updated_at,
            last_logged_at: self.last_logged_at,
            uptime_seconds: (Utc::now() - self.started_at).num_seconds().max(0) as u64,
            input_frames_total: self.input_frames_total,
            output_frames_total: self.output_frames_total,
            output_packets_total: self.output_packets_total,
            input_bytes_total: self.input_bytes_total,
            output_frames_bytes_total: self.output_frames_bytes_total,
            output_packets_bytes_total: self.output_packets_bytes_total,
            input_fps: self.input_fps,
            output_real_fps: self.output_real_fps,
            output_packet_rate: self.output_packet_rate,
            input_bitrate_kbps: self.input_bitrate_kbps,
            output_frame_bitrate_kbps: self.output_frame_bitrate_kbps,
            output_packet_bitrate_kbps: self.output_packet_bitrate_kbps,
        }
    }
}

fn stream_registry() -> &'static DashMap<String, StreamMetricsState> {
    static STREAM_METRICS: OnceLock<DashMap<String, StreamMetricsState>> = OnceLock::new();
    STREAM_METRICS.get_or_init(DashMap::new)
}

pub type StreamMetricsCounter = Arc<AtomicU64>;

pub fn new_stream_counter() -> StreamMetricsCounter {
    Arc::new(AtomicU64::new(0))
}

pub fn read_stream_counter(counter: &StreamMetricsCounter) -> u64 {
    counter.load(Ordering::Relaxed)
}

pub fn register_stream(room_name: &str, camera_uuid: &str, camera_name: &str) {
    let state = StreamMetricsState::new(
        room_name.to_string(),
        camera_uuid.to_string(),
        camera_name.to_string(),
    );

    stream_registry().insert(room_name.to_string(), state);
    log::info!(
        " [metrics] registered stream {} camera={} ({})",
        room_name,
        camera_name,
        camera_uuid
    );
}

pub fn unregister_stream(room_name: &str) {
    stream_registry().remove(room_name);
    log::info!(" [metrics] unregistered stream {}", room_name);
}

pub fn record_stream_input_frame(room_name: &str, frame_bytes: usize) {
    if let Some(mut entry) = stream_registry().get_mut(room_name) {
        entry.input_frames_total = entry.input_frames_total.saturating_add(1);
        entry.input_bytes_total = entry.input_bytes_total.saturating_add(frame_bytes as u64);
        entry.window_input_frames = entry.window_input_frames.saturating_add(1);
        entry.window_input_bytes = entry.window_input_bytes.saturating_add(frame_bytes as u64);
        entry.last_updated_at = Utc::now();
        update_rates_and_log_if_due(&mut entry);
    }

    // Important: run aggregate logging outside DashMap entry lock scope.
    // Doing this while holding `get_mut()` can deadlock when iterating the map.
    maybe_log_total_stream_egress();
}

pub fn record_stream_output_frame(room_name: &str, frame_bytes: usize) {
    if forced_zero_output_active() {
        // Intentional test fault-injection mode: suppress output counters/bytes.
        maybe_log_total_stream_egress();
        return;
    }

    if let Some(mut entry) = stream_registry().get_mut(room_name) {
        entry.output_frames_total = entry.output_frames_total.saturating_add(1);
        entry.output_frames_bytes_total = entry.output_frames_bytes_total.saturating_add(frame_bytes as u64);
        entry.window_output_frames = entry.window_output_frames.saturating_add(1);
        entry.window_output_frames_bytes = entry.window_output_frames_bytes.saturating_add(frame_bytes as u64);
        entry.last_updated_at = Utc::now();
        update_rates_and_log_if_due(&mut entry);
    }

    // Important: run aggregate logging outside DashMap entry lock scope.
    maybe_log_total_stream_egress();
}

pub fn record_stream_output_packet(room_name: &str, packet_bytes: usize) {
    if forced_zero_output_active() {
        // Intentional test fault-injection mode: suppress output counters/bytes.
        maybe_log_total_stream_egress();
        return;
    }

    if let Some(mut entry) = stream_registry().get_mut(room_name) {
        entry.output_packets_total = entry.output_packets_total.saturating_add(1);
        entry.output_packets_bytes_total = entry.output_packets_bytes_total.saturating_add(packet_bytes as u64);
        entry.window_output_packets = entry.window_output_packets.saturating_add(1);
        entry.window_output_packets_bytes = entry.window_output_packets_bytes.saturating_add(packet_bytes as u64);
        entry.last_updated_at = Utc::now();
        update_rates_and_log_if_due(&mut entry);
    }

    // Important: run aggregate logging outside DashMap entry lock scope.
    maybe_log_total_stream_egress();
}

pub fn get_stream_metrics(room_name: &str) -> Option<StreamMetricsSnapshot> {
    stream_registry().get(room_name).map(|entry| entry.snapshot())
}

pub fn list_stream_metrics() -> Vec<StreamMetricsSnapshot> {
    stream_registry().iter().map(|entry| entry.snapshot()).collect()
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;

    let value = bytes as f64;
    if value >= GB {
        format!("{:.2} GB", value / GB)
    } else if value >= MB {
        format!("{:.2} MB", value / MB)
    } else if value >= KB {
        format!("{:.2} KB", value / KB)
    } else {
        format!("{} B", bytes)
    }
}

fn maybe_log_total_stream_egress() {
    let now = Instant::now();
    let now_ms = now_epoch_ms();
    let interval = Duration::from_secs(total_stream_egress_log_interval_secs());

    // Fast path for hot call sites: avoid lock attempts until the next due window.
    let next_due_ms = aggregate_next_log_check_ms().load(Ordering::Relaxed);
    if now_ms < next_due_ms {
        return;
    }

    let mut state = match aggregate_traffic_state().try_lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };

    let elapsed = now.duration_since(state.last_log_at);
    if elapsed < interval {
        let wait_ms = (interval - elapsed).as_millis() as u64;
        aggregate_next_log_check_ms().store(now_ms.saturating_add(wait_ms), Ordering::Relaxed);
        return;
    }

    let mut stream_count = 0usize;
    let mut total_output_bytes = 0u64;
    for entry in stream_registry().iter() {
        stream_count += 1;
        total_output_bytes = total_output_bytes
            .saturating_add(entry.output_packets_bytes_total);
    }

    let delta_bytes = total_output_bytes.saturating_sub(state.last_total_output_bytes);
    let elapsed_s = elapsed.as_secs_f64();
    let egress_mbps = if elapsed_s > 0.0 {
        ((delta_bytes as f64) * 8.0) / (elapsed_s * 1_000_000.0)
    } else {
        0.0
    };

    log::info!(
        " [stream_egress_total] egress_mbps={:.3} streams={} interval_s={:.2} delta={} total={} ",
        egress_mbps,
        stream_count,
        elapsed_s,
        format_bytes(delta_bytes),
        format_bytes(total_output_bytes)
    );

    state.last_log_at = now;
    state.last_total_output_bytes = total_output_bytes;
    aggregate_next_log_check_ms().store(
        now_ms.saturating_add(interval.as_millis() as u64),
        Ordering::Relaxed,
    );
}

fn log_stream_metrics(state: &StreamMetricsState) {
    log::info!(
        concat!(
            " [stream_metrics] stream={} camera={} input_fps={:.2} output_real_fps={:.2} ",
            "output_packet_rate={:.2} input_kbps={:.2} output_frame_kbps={:.2} ",
            "output_packet_kbps={:.2} input_frames_total={} output_frames_total={} ",
            "output_packets_total={} input_bytes_total={} output_frames_bytes_total={} ",
            "output_packets_bytes_total={}"
        ),
        state.stream_id,
        state.camera_uuid,
        state.input_fps,
        state.output_real_fps,
        state.output_packet_rate,
        state.input_bitrate_kbps,
        state.output_frame_bitrate_kbps,
        state.output_packet_bitrate_kbps,
        state.input_frames_total,
        state.output_frames_total,
        state.output_packets_total,
        state.input_bytes_total,
        state.output_frames_bytes_total,
        state.output_packets_bytes_total,
    );
}

fn update_rates_and_log_if_due(state: &mut StreamMetricsState) {
    let elapsed_s = state.window_started_at.elapsed().as_secs_f64();
    if elapsed_s < stream_metrics_log_interval_secs() as f64 {
        return;
    }

    state.input_fps = state.window_input_frames as f64 / elapsed_s;
    state.output_real_fps = state.window_output_frames as f64 / elapsed_s;
    state.output_packet_rate = state.window_output_packets as f64 / elapsed_s;

    state.input_bitrate_kbps = ((state.window_input_bytes as f64) * 8.0 / 1000.0) / elapsed_s;
    state.output_frame_bitrate_kbps =
        ((state.window_output_frames_bytes as f64) * 8.0 / 1000.0) / elapsed_s;
    state.output_packet_bitrate_kbps =
        ((state.window_output_packets_bytes as f64) * 8.0 / 1000.0) / elapsed_s;

    state.last_logged_at = Some(Utc::now());

    log_stream_metrics(state);

    state.window_started_at = Instant::now();
    state.window_input_frames = 0;
    state.window_output_frames = 0;
    state.window_output_packets = 0;
    state.window_input_bytes = 0;
    state.window_output_frames_bytes = 0;
    state.window_output_packets_bytes = 0;
}
