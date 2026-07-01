use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use chrono::{DateTime, Utc};

use crate::events::{CameraInfo, VideoCodec};

/// Persistent state for a camera stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedCameraState {
    pub camera_uuid: String,
    pub camera_name: String,
    pub room_name: String,
    pub zmq_endpoint: String,
    pub state: String, // "running", "stopped", "error"
    pub created_at: DateTime<Utc>,
    pub last_activity: DateTime<Utc>,
    pub viewer_count: usize,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl PersistedCameraState {
    pub fn from_camera_info(camera_info: &CameraInfo) -> Self {
        let now = Utc::now();
        let mut metadata = HashMap::new();
        metadata.insert("resolution".to_string(), serde_json::Value::String("1080p".to_string()));
        metadata.insert("fps".to_string(), serde_json::Value::Number(serde_json::Number::from(30)));
        metadata.insert("codec".to_string(), serde_json::Value::String("H264".to_string()));

        Self {
            camera_uuid: camera_info.camera_uuid.clone(),
            camera_name: camera_info.camera_name.clone(),
            room_name: camera_info.room_name.clone(),
            zmq_endpoint: camera_info.zmq_endpoint.clone(),
            state: "running".to_string(),
            created_at: now,
            last_activity: now,
            viewer_count: 0,
            metadata,
        }
    }

    pub fn to_camera_info(&self) -> CameraInfo {
        CameraInfo {
            camera_uuid: self.camera_uuid.clone(),
            camera_name: self.camera_name.clone(),
            room_name: self.room_name.clone(),
            zmq_endpoint: self.zmq_endpoint.clone(),
            codec: VideoCodec::detect_from_camera_name(&self.camera_name),
        }
    }

    pub fn update_activity(&mut self) {
        self.last_activity = Utc::now();
    }

    pub fn set_state(&mut self, state: &str) {
        self.state = state.to_string();
        self.update_activity();
    }

    pub fn set_viewer_count(&mut self, count: usize) {
        self.viewer_count = count;
        self.update_activity();
    }
}

/// Application statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppStatistics {
    pub total_cameras_created: u64,
    pub total_restarts: u64,
    pub uptime_start: DateTime<Utc>,
    pub last_restart: Option<DateTime<Utc>>,
}

impl Default for AppStatistics {
    fn default() -> Self {
        Self {
            total_cameras_created: 0,
            total_restarts: 0,
            uptime_start: Utc::now(),
            last_restart: None,
        }
    }
}

/// Main persistent state structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub version: String,
    pub last_updated: DateTime<Utc>,
    pub sfu_id: String,
    pub cameras: HashMap<String, PersistedCameraState>, // room_name -> camera_state
    pub statistics: AppStatistics,
}

impl PersistentState {
    pub fn new(sfu_id: String) -> Self {
        Self {
            version: "1.0".to_string(),
            last_updated: Utc::now(),
            sfu_id,
            cameras: HashMap::new(),
            statistics: AppStatistics::default(),
        }
    }

    pub fn update_timestamp(&mut self) {
        self.last_updated = Utc::now();
    }

    pub fn add_camera(&mut self, camera_info: &CameraInfo) {
        let camera_state = PersistedCameraState::from_camera_info(camera_info);
        self.cameras.insert(camera_info.room_name.clone(), camera_state);
        self.statistics.total_cameras_created += 1;
        self.update_timestamp();
        
        info!(" Added camera to persistent state: {} (total: {})", 
             camera_info.room_name, self.cameras.len());
    }

    pub fn remove_camera(&mut self, room_name: &str) -> bool {
        if self.cameras.remove(room_name).is_some() {
            self.update_timestamp();
            info!(" Removed camera from persistent state: {} (remaining: {})", 
                     room_name, self.cameras.len());
            true
        } else {
            warn!(" Attempted to remove non-existent camera from state: {}", room_name);
            false
        }
    }

    pub fn update_camera_state(&mut self, room_name: &str, state: &str) -> bool {
        if let Some(camera) = self.cameras.get_mut(room_name) {
            camera.set_state(state);
            self.update_timestamp();
            debug!(" Updated camera state: {} -> {}", room_name, state);
            true
        } else {
            warn!(" Attempted to update state for non-existent camera: {}", room_name);
            false
        }
    }

    pub fn update_camera_viewer_count(&mut self, room_name: &str, count: usize) -> bool {
        if let Some(camera) = self.cameras.get_mut(room_name) {
            camera.set_viewer_count(count);
            self.update_timestamp();
            debug!(" Updated camera viewer count: {} -> {}", room_name, count);
            true
        } else {
            false
        }
    }

    pub fn get_running_cameras(&self) -> Vec<CameraInfo> {
        self.cameras
            .values()
            .filter(|camera| camera.state == "running")
            .map(|camera| camera.to_camera_info())
            .collect()
    }

    pub fn increment_restart_count(&mut self) {
        self.statistics.total_restarts += 1;
        self.statistics.last_restart = Some(Utc::now());
        self.update_timestamp();
    }
}

/// State manager for handling persistent storage
pub struct StateManager {
    state_file_path: String,
    backup_file_path: String,
    state: PersistentState,
}

impl StateManager {
    pub fn new(sfu_id: String, state_file_path: Option<String>) -> Self {
        let state_file = state_file_path.unwrap_or_else(|| "sfu_state.json".to_string());
        let backup_file = format!("{}.backup", state_file);
        
        Self {
            state_file_path: state_file,
            backup_file_path: backup_file,
            state: PersistentState::new(sfu_id),
        }
    }

    /// Load state from file, create new if doesn't exist
    pub fn load_or_create(&mut self) -> Result<()> {
        info!(" Loading persistent state from: {}", self.state_file_path);

        if Path::new(&self.state_file_path).exists() {
            let state_file_path = self.state_file_path.clone(); // Clone to avoid borrow issues
            match self.load_from_file(&state_file_path) {
                Ok(()) => {
                    info!(" Loaded persistent state successfully");
                    info!(" State summary: {} cameras, {} total created, {} restarts", 
                          self.state.cameras.len(),
                          self.state.statistics.total_cameras_created,
                          self.state.statistics.total_restarts);
                    
                    // Increment restart count
                    self.state.increment_restart_count();
                    self.save()?;
                    
                    return Ok(());
                }
                Err(e) => {
                    error!(" Failed to load state file: {}", e);
                    
                    // Try backup file
                    let backup_file_path = self.backup_file_path.clone(); // Clone to avoid borrow issues
                    if Path::new(&backup_file_path).exists() {
                        warn!(" Attempting to load from backup file");
                        match self.load_from_file(&backup_file_path) {
                            Ok(()) => {
                                info!(" Loaded from backup file successfully");
                                self.state.increment_restart_count();
                                self.save()?; // Save to main file
                                return Ok(());
                            }
                            Err(backup_err) => {
                                error!(" Backup file also failed: {}", backup_err);
                            }
                        }
                    }
                }
            }
        }

        // Create new state file
        warn!(" Creating new persistent state file");
        self.save()?;
        info!(" New persistent state created");
        
        Ok(())
    }

    fn load_from_file(&mut self, file_path: &str) -> Result<()> {
        let content = fs::read_to_string(file_path)
            .map_err(|e| anyhow!("Failed to read state file {}: {}", file_path, e))?;
        
        self.state = serde_json::from_str(&content)
            .map_err(|e| anyhow!("Failed to parse state file {}: {}", file_path, e))?;
        
        // Validate state version
        if self.state.version != "1.0" {
            warn!(" State file version mismatch: {} (expected 1.0)", self.state.version);
        }
        
        Ok(())
    }

    /// Save current state to file
    pub fn save(&self) -> Result<()> {
        debug!(" Saving persistent state to: {}", self.state_file_path);

        // Create backup of existing file
        if Path::new(&self.state_file_path).exists() {
            if let Err(e) = fs::copy(&self.state_file_path, &self.backup_file_path) {
                warn!(" Failed to create backup: {}", e);
            }
        }

        // Serialize state
        let json_content = serde_json::to_string_pretty(&self.state)
            .map_err(|e| anyhow!("Failed to serialize state: {}", e))?;

        // Write to file
        fs::write(&self.state_file_path, json_content)
            .map_err(|e| anyhow!("Failed to write state file {}: {}", self.state_file_path, e))?;

        debug!(" State saved successfully");
        Ok(())
    }

    /// Add camera to persistent state
    pub fn add_camera(&mut self, camera_info: &CameraInfo) -> Result<()> {
        self.state.add_camera(camera_info);
        self.save()
    }

    /// Remove camera from persistent state
    pub fn remove_camera(&mut self, room_name: &str) -> Result<()> {
        self.state.remove_camera(room_name);
        self.save()
    }

    /// Update camera state
    pub fn update_camera_state(&mut self, room_name: &str, state: &str) -> Result<()> {
        if self.state.update_camera_state(room_name, state) {
            self.save()
        } else {
            Ok(()) // Camera not found, no need to save
        }
    }

    /// Update camera viewer count
    pub fn update_camera_viewer_count(&mut self, room_name: &str, count: usize) -> Result<()> {
        if self.state.update_camera_viewer_count(room_name, count) {
            self.save()
        } else {
            Ok(()) // Camera not found, no need to save
        }
    }

    /// Get all cameras that should be running
    pub fn get_cameras_to_restore(&self) -> Vec<CameraInfo> {
        let cameras = self.state.get_running_cameras();
        if !cameras.is_empty() {
            info!(" Found {} cameras to restore from persistent state", cameras.len());
            for camera in &cameras {
                info!("   - {} ({})", camera.room_name, camera.camera_name);
            }
        }
        cameras
    }

    /// Get current state statistics
    pub fn get_statistics(&self) -> &AppStatistics {
        &self.state.statistics
    }

    /// Get current state summary
    pub fn get_state_summary(&self) -> serde_json::Value {
        serde_json::json!({
            "version": self.state.version,
            "sfu_id": self.state.sfu_id,
            "last_updated": self.state.last_updated,
            "total_cameras": self.state.cameras.len(),
            "running_cameras": self.state.get_running_cameras().len(),
            "statistics": self.state.statistics,
            "cameras": self.state.cameras.values().collect::<Vec<_>>()
        })
    }

    /// Clean up stale entries (cameras inactive for more than specified duration)
    pub fn cleanup_stale_entries(&mut self, max_inactive_hours: i64) -> Result<usize> {
        let cutoff_time = Utc::now() - chrono::Duration::hours(max_inactive_hours);
        let mut removed_count = 0;
        
        let stale_cameras: Vec<String> = self.state.cameras
            .iter()
            .filter(|(_, camera)| camera.last_activity < cutoff_time)
            .map(|(room_name, _)| room_name.clone())
            .collect();
        
        for room_name in stale_cameras {
            info!(" Removing stale camera from persistent state: {}", room_name);
            self.state.remove_camera(&room_name);
            removed_count += 1;
        }
        
        if removed_count > 0 {
            self.save()?;
            info!(" Cleaned up {} stale camera entries", removed_count);
        }
        
        Ok(removed_count)
    }
}