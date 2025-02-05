use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[derive(Debug)]
struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect(&addr).await?;
        Ok(Client { stream })
    }

    pub async fn send_command(&mut self, cmd: &str) -> Result<(), std::io::Error> {
        self.stream.write_all(cmd.as_bytes()).await?;
        self.stream.write_all(b"\n").await?;
        self.stream.flush().await
    }

    pub async fn read_response(&mut self) -> Result<String, std::io::Error> {
        let mut reader = BufReader::new(&mut self.stream);
        let mut response = String::new();
        reader.read_line(&mut response).await?;
        Ok(response)
    }
}
