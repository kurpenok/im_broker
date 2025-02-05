#[derive(Debug, Clone)]
pub struct Message {
    pub topic: String,
    pub key: Option<String>,
    pub data: Vec<u8>,
}
