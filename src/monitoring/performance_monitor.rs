use log::{debug, info};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Simple performance monitoring for the SFU
pub struct PerformanceMonitor {
    pub frame_count: Arc<AtomicU64>,
    pub viewer_count: Arc<AtomicU64>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            frame_count: Arc::new(AtomicU64::new(0)),
            viewer_count: Arc::new(AtomicU64::new(0)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn start(&self) {
        if self.running.load(Ordering::SeqCst) {
            return; // Already running
        }

        self.running.store(true, Ordering::SeqCst);
        
        let frame_count = self.frame_count.clone();
        let viewer_count = self.viewer_count.clone();
        let running = self.running.clone();

        thread::spawn(move || {
            info!(" Performance monitoring started");
            let mut last_frame_count = 0u64;
            let mut last_time = Instant::now();

            while running.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_secs(10));

                let current_frames = frame_count.load(Ordering::SeqCst);
                let current_viewers = viewer_count.load(Ordering::SeqCst);
                let now = Instant::now();
                
                let elapsed = now.duration_since(last_time).as_secs_f64();
                let frame_rate = (current_frames - last_frame_count) as f64 / elapsed;

                if current_viewers > 0 || frame_rate > 0.0 {
                    info!(
                            "[PERF] Frames: {} (+{:.1}/s), Active Viewers: {}",
                        current_frames,
                        frame_rate,
                        current_viewers
                    );
                } else {
                    debug!(
                            "[PERF idle] Frames: {} (+{:.1}/s), Active Viewers: {}",
                        current_frames,
                        frame_rate,
                        current_viewers
                    );
                }

                last_frame_count = current_frames;
                last_time = now;
            }
            
            info!(" Performance monitoring stopped");
        });
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn increment_frame_count(&self) {
        self.frame_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn set_viewer_count(&self, count: usize) {
        self.viewer_count.store(count as u64, Ordering::SeqCst);
    }
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new()
    }
}