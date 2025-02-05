use im_broker::{error::BrokerError, network::start_server};

#[tokio::main]
async fn main() -> Result<(), BrokerError> {
    start_server().await
}
