use std::sync::Arc;

use base64::{prelude::BASE64_STANDARD, Engine};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{
    broker::Broker,
    command::{parse_command, BrokerCommand},
    error::BrokerError,
};

pub async fn start_server() -> Result<(), BrokerError> {
    println!("[BROKER] Starting broker on 127.0.0.1:8080");

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

pub async fn handle_client(socket: TcpStream, broker: Broker) {
    println!("[CLIENT] New connection established");

    let (read, write) = socket.into_split();
    let mut reader = BufReader::new(read);
    let writer = Arc::new(Mutex::new(BufWriter::new(write)));
    let mut line = String::new();

    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Ok(0) => {
                println!("[CLIENT] Connection closed");
                break;
            }
            Ok(_) => match parse_command(&line) {
                Ok(cmd) => {
                    println!("[CLIENT] Received command: {}", line.trim());
                    process_command(cmd, &broker, &writer).await
                }
                Err(e) => eprintln!("[BROKER] Error: {}", e),
            },
            Err(e) => {
                eprintln!("[BROKER] Error reading: {}", e);
                break;
            }
        }
    }
}

pub async fn process_command(
    cmd: BrokerCommand,
    broker: &Broker,
    writer: &Arc<Mutex<BufWriter<OwnedWriteHalf>>>,
) {
    match cmd {
        BrokerCommand::CreateTopic(topic) => {
            println!("[BROKER] Creating topic: {}", topic);
            if let Err(e) = broker.create_topic(&topic).await {
                println!("[BROKER] Error creating topic: {}", e);
                let mut writer = writer.lock().await;
                let _ = writer
                    .write_all(format!("[BROKER] Error: {}\n", e).as_bytes())
                    .await;
            } else {
                println!("[BROKER] Topic created: {}", topic);
            }
        }
        BrokerCommand::Subscribe(topic) => {
            println!("[BROKER] New subscription to: {}", topic);
            match broker.subscribe(&topic).await {
                Ok(mut rx) => {
                    println!("[BROKER] Subscription successful: {}", topic);
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            println!("[BROKER] Sending message to subscriber: {:?}", msg);
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
                    println!("[BROKER] Subscription error: {}", e);
                    let mut writer = writer.lock().await;
                    let _ = writer
                        .write_all(format!("[BROKER] Error: {}\n", e).as_bytes())
                        .await;
                }
            }
        }
        BrokerCommand::Unsubscribe(topic) => {
            println!("[BROKER] Unsubscribing from: {}", topic);
            if let Err(e) = broker.unsubscribe(&topic).await {
                let mut writer = writer.lock().await;
                let _ = writer
                    .write_all(format!("[BROKER] Error: {}\n", e).as_bytes())
                    .await;
            }
        }
        BrokerCommand::Publish(msg) => {
            println!("[BROKER] Publishing message to: {}", msg.topic);
            if let Err(e) = broker.publish(msg).await {
                let mut writer = writer.lock().await;
                let _ = writer
                    .write_all(format!("[BROKER] Error: {}\n", e).as_bytes())
                    .await;
            }
            println!("[BROKER] Message published successfully");
        }
    }

    let mut writer = writer.lock().await;
    let _ = writer.flush().await;
}
