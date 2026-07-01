#[derive(Clone, serde::Deserialize)]
pub struct RuntimeConfig {
    pub signaling_server_url: String,
    pub sfu_id: String,
    pub webrtc_stun_server: String,
    pub webrtc_turn_servers: Vec<String>,
    pub jwt_auth_enabled: bool,
    pub gateway_uuid: Option<String>,
    pub customer_uuid: Option<String>,
    pub jwt_secret: Option<String>,
    pub jwt_expiry_secs: u64,
}

impl RuntimeConfig {
    pub fn from_env() -> Self {
        let signaling_server_url = std::env::var("SIGNALING_SERVER_URL")
            .unwrap_or_else(|_| "http://localhost:8443".to_string());
        let sfu_id = std::env::var("SFU_ID")
            .unwrap_or_else(|_| format!("sfu_jetson_{}", rand::random::<u16>()));

        let webrtc_stun_server = std::env::var("WEBRTC_STUN_SERVER")
            .unwrap_or_else(|_| "stun://stun.l.google.com:19302".to_string());

        let webrtc_turn_servers = std::env::var("WEBRTC_TURN_SERVERS")
            .ok()
            .map(|s| s.split(',').map(|url| url.trim().to_string()).collect())
            .unwrap_or_default();

        let gateway_uuid = std::env::var("GATEWAY_UUID").ok();
        let customer_uuid = std::env::var("CUSTOMER_UUID").ok();
        let jwt_secret = std::env::var("JWT_SECRET").ok();
        let jwt_expiry_secs = std::env::var("JWT_EXPIRY_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3600);
        let jwt_auth_enabled = std::env::var("JWT_AUTH_ENABLED")
            .map(|v| !matches!(v.to_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(true);

        Self {
            signaling_server_url,
            sfu_id,
            webrtc_stun_server,
            webrtc_turn_servers,
            jwt_auth_enabled,
            gateway_uuid,
            customer_uuid,
            jwt_secret,
            jwt_expiry_secs,
        }
    }

    pub fn jwt_config(&self) -> Option<crate::auth::JwtConfig> {
        if !self.jwt_auth_enabled {
            return None;
        }

        if let Ok(raw) = std::env::var("GATEWAY_JWT") {
            if !raw.trim().is_empty() {
                let bearer = if raw.trim_start().starts_with("Bearer ") {
                    raw.trim().to_string()
                } else {
                    format!("Bearer {}", raw.trim())
                };
                return Some(crate::auth::JwtConfig::PreBuilt { bearer });
            }
        }

        let gateway_uuid = self.gateway_uuid.clone()?;
        let customer_uuid = self.customer_uuid.clone()?;
        let secret = self.jwt_secret.clone()?;

        Some(crate::auth::JwtConfig::SelfSigned {
            gateway_uuid,
            customer_uuid,
            secret,
            expiry_secs: self.jwt_expiry_secs,
        })
    }
}
