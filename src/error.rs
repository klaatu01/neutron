#[derive(Debug, Clone)]
pub enum NeutronError {
    Disconnected,
    OperationTimeout,
    UnsupportedCommand,
    DecodeFailed,
    EncodeFailed,
    ChannelTerminated,
    DuplicateRequest,
    Unresolvable,
    Io,
    DeserializationFailed,
    SerializationFailed,
    AuthenticationFailed(String),
    ConnectionFailed,
    InvalidUrl,
    PulsarError(String),
}

/// Whether an operation that failed this way may be retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryability {
    /// The failure is tied to a connection's lifetime; the same request
    /// can be reissued once a connection is live again.
    Transient,
    /// Retrying would fail the same way (protocol, serialization, or
    /// broker-rejected errors).
    Permanent,
}

impl NeutronError {
    pub fn is_disconnect(&self) -> bool {
        matches!(
            self,
            NeutronError::Disconnected | NeutronError::ChannelTerminated
        )
    }

    /// The retry decision belongs on the error type, not at each call
    /// site: one policy serves every path.
    ///
    /// `OperationTimeout` is deliberately Permanent for automatic
    /// retries: the request may still be executing broker-side, so only
    /// the application can decide whether reissuing is safe.
    pub fn retryability(&self) -> Retryability {
        match self {
            NeutronError::Disconnected
            | NeutronError::ChannelTerminated
            | NeutronError::ConnectionFailed
            | NeutronError::Io => Retryability::Transient,
            _ => Retryability::Permanent,
        }
    }
}

impl std::fmt::Display for NeutronError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NeutronError::Disconnected => write!(f, "Disconnected"),
            NeutronError::OperationTimeout => write!(f, "Operation timeout"),
            NeutronError::UnsupportedCommand => write!(f, "Unsupported command"),
            NeutronError::DecodeFailed => write!(f, "Decode failed"),
            NeutronError::EncodeFailed => write!(f, "Encode failed"),
            NeutronError::ChannelTerminated => write!(f, "Channel terminated"),
            NeutronError::DuplicateRequest => write!(f, "Duplicate in-flight request"),
            NeutronError::Unresolvable => write!(f, "Unresolvable"),
            NeutronError::Io => write!(f, "IO error"),
            NeutronError::DeserializationFailed => write!(f, "Deserialization failed"),
            NeutronError::SerializationFailed => write!(f, "Serialization failed"),
            NeutronError::AuthenticationFailed(e) => write!(f, "Authentication failed: {}", e),
            NeutronError::ConnectionFailed => write!(f, "Connection failed"),
            NeutronError::InvalidUrl => write!(f, "Invalid URL"),
            NeutronError::PulsarError(e) => write!(f, "Pulsar error: {}", e),
        }
    }
}

impl std::error::Error for NeutronError {}
