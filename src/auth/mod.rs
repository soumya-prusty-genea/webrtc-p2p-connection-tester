pub mod jwt;
pub use jwt::{JwtConfig, generate_token};

/// Tracks whether the signaling server accepted or rejected our JWT.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthStatus {
    /// Auth is configured but the server has not yet confirmed it.
    Pending,
    /// Server accepted the token (received `sfu_register_ack`).
    Verified,
    /// Server explicitly rejected the token (`auth_error` event or connect error).
    Failed(String),
    /// No JWT config provided — running without authentication.
    Disabled,
}
