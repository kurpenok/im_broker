use base64::{prelude::BASE64_STANDARD, Engine};

use crate::{error::BrokerError, message::Message};

#[derive(Debug)]
pub enum BrokerCommand {
    CreateTopic(String),
    Subscribe(String),
    Unsubscribe(String),
    Publish(Message),
}

pub fn parse_command(line: &str) -> Result<BrokerCommand, BrokerError> {
    let parts: Vec<&str> = line.trim().split_whitespace().collect();
    if parts.is_empty() {
        return Err(BrokerError::InvalidCommand);
    }

    match parts[0] {
        "CREATE_TOPIC" => {
            if parts.len() != 2 {
                return Err(BrokerError::InvalidCommand);
            }
            Ok(BrokerCommand::CreateTopic(parts[1].to_string()))
        }
        "SUBSCRIBE" => {
            if parts.len() != 2 {
                return Err(BrokerError::InvalidCommand);
            }
            Ok(BrokerCommand::Subscribe(parts[1].to_string()))
        }
        "UNSUBSCRIBE" => {
            if parts.len() != 2 {
                return Err(BrokerError::InvalidCommand);
            }
            Ok(BrokerCommand::Unsubscribe(parts[1].to_string()))
        }
        "PUBLISH" => {
            if parts.len() != 3 {
                return Err(BrokerError::InvalidCommand);
            }

            let topic = parts[1].to_string();
            let key = if parts[2] != "-" {
                Some(parts[2].to_string())
            } else {
                None
            };
            let data = BASE64_STANDARD
                .decode(parts[3..].join(""))
                .map_err(|_| BrokerError::InvalidData)?;

            Ok(BrokerCommand::Publish(Message { topic, key, data }))
        }
        _ => Err(BrokerError::InvalidCommand),
    }
}
