mod broker;
mod client;
mod command;
mod error;
mod message;

use std::sync::Arc;

use base64::{prelude::BASE64_STANDARD, Engine};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::Mutex,
};

use broker::Broker;
use command::{parse_command, BrokerCommand};
use error::BrokerError;

#[tokio::main]
async fn main() -> Result<(), BrokerError> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let broker = Broker::new();

    loop {
        let (socket, _) = listener.accept().await?;
        let broker_clone = broker.clone();
        tokio::spawn(async move {
            handle_client(socket, broker_clone).await;
        });
    }
}

async fn handle_client(socket: TcpStream, broker: Broker) {
    let (read, write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let writer = Arc::new(Mutex::new(BufWriter::new(write)));
    let mut line = String::new();

    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => match parse_command(&line) {
                Ok(cmd) => process_command(cmd, &broker, &writer).await,
                Err(e) => eprintln!("Error: {}", e),
            },
            Err(e) => {
                eprintln!("Error reading: {}", e);
                break;
            }
        }
    }
}

async fn process_command(
    cmd: BrokerCommand,
    broker: &Broker,
    writer: &Arc<Mutex<BufWriter<OwnedWriteHalf>>>,
) {
    match cmd {
        BrokerCommand::CreateTopic(topic) => {
            if let Err(e) = broker.create_topic(&topic).await {
                let mut writer = writer.lock().await;
                let _ = writer.write_all(format!("Error: {}\n", e).as_bytes()).await;
            }
        }
        BrokerCommand::Subscribe(topic) => match broker.subscribe(&topic).await {
            Ok(mut rx) => {
                let writer = writer.clone();
                tokio::spawn(async move {
                    while let Some(msg) = rx.recv().await {
                        let data = format!(
                            "TOPIC {} DATA {}\n",
                            msg.topic,
                            BASE64_STANDARD.encode(&msg.data)
                        );
                        let mut writer = writer.lock().await;
                        let _ = writer.write_all(data.as_bytes()).await;
                    }
                });
            }
            Err(e) => {
                let mut writer = writer.lock().await;
                let _ = writer.write_all(format!("Error: {}\n", e).as_bytes()).await;
            }
        },
        BrokerCommand::Unsubscribe(topic) => {
            if let Err(e) = broker.unsubscribe(&topic).await {
                let mut writer = writer.lock().await;
                let _ = writer.write_all(format!("Error: {}\n", e).as_bytes()).await;
            }
        }
        BrokerCommand::Publish(msg) => {
            if let Err(e) = broker.publish(msg).await {
                let mut writer = writer.lock().await;
                let _ = writer.write_all(format!("Error: {}\n", e).as_bytes()).await;
            }
        }
    }

    let mut writer = writer.lock().await;
    let _ = writer.flush().await;
}
