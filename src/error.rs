use std::{error, fmt, io};

#[derive(Debug)]
pub enum BrokerError {
    TopicExists,
    TopicNotFound,
    InvalidCommand,
    InvalidData,
    SendFailed,
    IoError(String),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerError::TopicExists => write!(f, "Topic already exists"),
            BrokerError::TopicNotFound => write!(f, "Topic not found"),
            BrokerError::InvalidCommand => write!(f, "Invalid command"),
            BrokerError::InvalidData => write!(f, "Invalid data"),
            BrokerError::SendFailed => write!(f, "Failed to send message"),
            BrokerError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl error::Error for BrokerError {}

impl From<io::Error> for BrokerError {
    fn from(value: io::Error) -> Self {
        BrokerError::IoError(value.to_string())
    }
}
