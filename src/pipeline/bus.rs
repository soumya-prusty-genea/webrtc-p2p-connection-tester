use log::{debug, error, info, warn};

pub fn log_bus_message(message: &str) {
    debug!("GStreamer bus message: {}", message);
}

pub fn log_bus_warning(message: &str) {
    warn!("GStreamer bus warning: {}", message);
}

pub fn log_bus_error(message: &str) {
    error!("GStreamer bus error: {}", message);
}

pub fn log_bus_eos(room_name: &str) {
    info!("EOS reached for room: {}", room_name);
}
