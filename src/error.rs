#[derive(Debug, Clone)]
pub enum NeutronError {
    Disconnected,
    OperationTimeout,
    UnsupportedCommand,
    DecodeFailed,
    EncodeFailed,
    ChannelTerminated,
}

impl NeutronError {
    pub fn is_disconnect(&self) -> bool {
        match self {
            NeutronError::Disconnected => true,
            NeutronError::ChannelTerminated => true,
            _ => false,
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
        }
    }
}

impl std::error::Error for NeutronError {}
