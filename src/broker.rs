use std::{collections::HashMap, sync::Arc};

use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock,
};

use crate::{error::BrokerError, message::Message};

#[derive(Debug)]
pub struct Broker {
    topics: Arc<RwLock<HashMap<String, Vec<UnboundedSender<Message>>>>>,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_topic(&self, name: &str) -> Result<(), BrokerError> {
        let mut topics = self.topics.write().await;

        if topics.contains_key(name) {
            return Err(BrokerError::TopicExists);
        }

        let (tx, _) = unbounded_channel();
        topics.insert(name.to_string(), vec![tx]);

        Ok(())
    }

    pub async fn subscribe(&self, topic: &str) -> Result<UnboundedReceiver<Message>, BrokerError> {
        let mut topics = self.topics.write().await;

        let (tx, rx) = unbounded_channel();
        topics.entry(topic.to_string()).or_default().push(tx);

        Ok(rx)
    }

    pub async fn unsubscribe(
        &self,
        topic: &str,
        sender: UnboundedSender<Message>,
    ) -> Result<(), BrokerError> {
        let mut topics = self.topics.write().await;

        if let Some(senders) = topics.get_mut(topic) {
            senders.retain(|s| !s.same_channel(&sender));
            Ok(())
        } else {
            Err(BrokerError::TopicNotFound)
        }
    }

    pub async fn publish(&self, msg: Message) -> Result<(), BrokerError> {
        let topics = self.topics.read().await;

        if let Some(subscribers) = topics.get(&msg.topic) {
            for tx in subscribers {
                tx.send(msg.clone()).map_err(|_| BrokerError::SendFailed)?;
            }
        }

        Ok(())
    }
}
