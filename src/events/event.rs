use anyhow::Result;

/// Video codec enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum VideoCodec {
    H264,
    H265,
}

impl VideoCodec {
    pub fn from_str(codec: &str) -> Self {
        match codec.to_lowercase().as_str() {
            "h265" | "hevc" | "h.265" => VideoCodec::H265,
            _ => VideoCodec::H264,
        }
    }

    pub fn detect_from_camera_name(camera_name: &str) -> Self {
        let name_lower = camera_name.to_lowercase();
        if name_lower.contains("h265") || name_lower.contains("hevc") {
            VideoCodec::H265
        } else if name_lower.contains("h264") || name_lower.contains("avc") {
            VideoCodec::H264
        } else if let Ok(codec_env) = std::env::var("DEFAULT_VIDEO_CODEC") {
            Self::from_str(&codec_env)
        } else {
            VideoCodec::H264
        }
    }

    pub fn from_frame_details(frame_details_json: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let details: serde_json::Value = serde_json::from_str(frame_details_json)?;

        if let Some(codec) = details
            .get("codec")
            .or_else(|| details.get("video_codec"))
            .or_else(|| details.get("encoding"))
            .and_then(|c| c.as_str())
        {
            let codec_lower = codec.to_lowercase();
            if codec_lower.contains("h265") || codec_lower.contains("hevc") {
                return Ok(VideoCodec::H265);
            } else if codec_lower.contains("h264") || codec_lower.contains("avc") {
                return Ok(VideoCodec::H264);
            }
        }

        if let Some(codec_name) = details.get("codec_name").and_then(|c| c.as_str()) {
            let codec_lower = codec_name.to_lowercase();
            if codec_lower.contains("h265") || codec_lower.contains("hevc") {
                return Ok(VideoCodec::H265);
            } else if codec_lower.contains("h264") || codec_lower.contains("avc") {
                return Ok(VideoCodec::H264);
            }
        }

        if let Some(format) = details.get("format").and_then(|f| f.as_str()) {
            let format_lower = format.to_lowercase();
            if format_lower.contains("h265") || format_lower.contains("hevc") {
                return Ok(VideoCodec::H265);
            }
        }

        Ok(VideoCodec::H264)
    }

    pub fn from_video_data(data: &[u8]) -> Self {
        for i in 0..data.len().saturating_sub(4) {
            if (data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x00 && data[i + 3] == 0x01)
                || (data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01)
            {
                let nal_start = if data[i + 2] == 0x01 { i + 3 } else { i + 4 };

                if nal_start < data.len() {
                    let nal_header = data[nal_start];
                    let nal_type = (nal_header >> 1) & 0x3F;
                    if nal_type >= 32 || nal_type == 32 {
                        return VideoCodec::H265;
                    }
                }
            }
        }
        VideoCodec::H264
    }

    pub fn detect_from_zmq_data(frame_details_json: &str, video_data: &[u8]) -> Self {
        if let Ok(codec) = Self::from_frame_details(frame_details_json) {
            return codec;
        }
        Self::from_video_data(video_data)
    }

    pub fn detect_and_update_from_zmq(frame_details_json: &str, video_data: &[u8]) -> Self {
        Self::detect_from_zmq_data(frame_details_json, video_data)
    }

    pub fn to_string(&self) -> &'static str {
        match self {
            VideoCodec::H264 => "H264",
            VideoCodec::H265 => "H265",
        }
    }

    pub fn get_gstreamer_caps(&self) -> &'static str {
        match self {
            VideoCodec::H264 => "video/x-h264,stream-format=byte-stream,alignment=au",
            VideoCodec::H265 => "video/x-h265,stream-format=byte-stream,alignment=au",
        }
    }

    pub fn get_parser_element(&self) -> &'static str {
        match self {
            VideoCodec::H264 => "h264parse",
            VideoCodec::H265 => "h265parse",
        }
    }
}

/// Processed camera information for SFU operations
#[derive(Debug, Clone)]
pub struct CameraInfo {
    pub camera_uuid: String,
    pub camera_name: String,
    pub room_name: String,
    pub zmq_endpoint: String,
    pub codec: VideoCodec,
}

impl CameraInfo {
    pub fn validate(&self) -> Result<()> {
        if self.camera_uuid.trim().is_empty() {
            return Err(anyhow::anyhow!("camera_uuid cannot be empty"));
        }
        if self.camera_name.trim().is_empty() {
            return Err(anyhow::anyhow!("camera_name cannot be empty"));
        }
        if self.room_name.trim().is_empty() {
            return Err(anyhow::anyhow!("room_name cannot be empty"));
        }
        if self.zmq_endpoint.trim().is_empty() {
            return Err(anyhow::anyhow!("zmq_endpoint cannot be empty"));
        }

        if self.camera_uuid.len() > 256 {
            return Err(anyhow::anyhow!("camera_uuid too long (max 256 characters)"));
        }
        if self.camera_name.len() > 256 {
            return Err(anyhow::anyhow!("camera_name too long (max 256 characters)"));
        }
        if self.room_name.len() > 256 {
            return Err(anyhow::anyhow!("room_name too long (max 256 characters)"));
        }

        if !self.zmq_endpoint.starts_with("ipc://")
            && !self.zmq_endpoint.starts_with("tcp://")
            && !self.zmq_endpoint.starts_with("inproc://")
            && !self.zmq_endpoint.starts_with("test://")
        {
            return Err(anyhow::anyhow!(
                "zmq_endpoint must start with ipc://, tcp://, inproc://, or test://"
            ));
        }

        for field in &[&self.camera_uuid, &self.camera_name, &self.room_name] {
            if field.chars().any(|c| c.is_control() && c != '\t' && c != '\n' && c != '\r') {
                return Err(anyhow::anyhow!("fields cannot contain control characters"));
            }
        }

        Ok(())
    }

    pub fn new(
        camera_uuid: String,
        camera_name: String,
        room_name: String,
        zmq_endpoint: String,
        codec: VideoCodec,
    ) -> Result<Self> {
        let info = CameraInfo {
            camera_uuid,
            camera_name,
            room_name,
            zmq_endpoint,
            codec,
        };
        info.validate()?;
        Ok(info)
    }
}
