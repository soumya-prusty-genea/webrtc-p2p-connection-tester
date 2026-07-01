use anyhow::{Context, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Claims embedded in the JWT sent to the signaling server.
///
/// ```json
/// {
///   "gateway_uuid": "GW-12345",
///   "customer_uuid": "CUST-98765",
///   "iat": 1773036000,
///   "exp": 1773039600
/// }
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub gateway_uuid: String,
    pub customer_uuid: String,
    /// Issued-at (Unix seconds)
    pub iat: u64,
    /// Expiry (Unix seconds)
    pub exp: u64,
}

/// How the SFU obtains its Bearer token.
///
/// Resolution order (checked by `RuntimeConfig::jwt_config()`):
/// 1. `GATEWAY_JWT` env var present → `PreBuilt`
/// 2. `JWT_SECRET` + `GATEWAY_UUID` + `CUSTOMER_UUID` all present → `SelfSigned`
/// 3. Any missing / `JWT_AUTH_ENABLED=false` → auth disabled (`None`)
///
/// - `SelfSigned` — SFU generates the JWT locally using HMAC-SHA256.  
///   Requires the SFU to share the same `JWT_SECRET` as the signaling server.
/// - `PreBuilt` — A token is supplied externally via `GATEWAY_JWT`.  
///   Use this when the signaling server issues tokens itself (e.g. via a
///   separate auth API) and you paste/inject the token at deploy time.
#[derive(Debug, Clone)]
pub enum JwtConfig {
    SelfSigned {
        gateway_uuid: String,
        customer_uuid: String,
        /// Must match the secret the signaling server uses to verify tokens.
        secret: String,
        /// Token lifetime in seconds (default: 3600)
        expiry_secs: u64,
    },
    PreBuilt {
        /// Full `Bearer <token>` string, ready to use as an Authorization header.
        bearer: String,
    },
}

/// Return a `Bearer <token>` string for the given config.
///
/// - `PreBuilt`   → returns the stored token as-is (no signing).
/// - `SelfSigned` → mints a fresh HS256 JWT each call so reconnects after
///    expiry automatically get a new valid token.
pub fn generate_token(config: &JwtConfig) -> Result<String> {
    match config {
        JwtConfig::PreBuilt { bearer } => Ok(bearer.clone()),
        JwtConfig::SelfSigned {
            gateway_uuid,
            customer_uuid,
            secret,
            expiry_secs,
        } => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("System time before UNIX epoch")?
                .as_secs();

            let claims = Claims {
                gateway_uuid: gateway_uuid.clone(),
                customer_uuid: customer_uuid.clone(),
                iat: now,
                exp: now + expiry_secs,
            };

            let header = Header::new(Algorithm::HS256);
            let key = EncodingKey::from_secret(secret.as_bytes());
            let token = encode(&header, &claims, &key)
                .context("Failed to encode JWT")?;

            Ok(format!("Bearer {}", token))
        }
    }
}
