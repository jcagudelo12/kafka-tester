//! Envío de mensajes a un topic.

use std::collections::BTreeMap;

use rskafka::{
    client::{
        Client,
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};

pub struct OutgoingMessage {
    pub key: Option<String>,
    pub value: String,
    pub headers: BTreeMap<String, String>,
}

impl OutgoingMessage {
    pub fn text(value: impl Into<String>) -> Self {
        Self { key: None, value: value.into(), headers: BTreeMap::new() }
    }
}

/// Envía un mensaje y devuelve el offset que le asignó Kafka.
pub async fn send(client: &Client, topic: &str, partition: i32, msg: OutgoingMessage) -> anyhow::Result<i64> {
    let pc = client
        .partition_client(topic, partition, UnknownTopicHandling::Retry)
        .await?;
    let record = Record {
        key: msg.key.filter(|k| !k.is_empty()).map(String::into_bytes),
        value: Some(msg.value.into_bytes()),
        headers: msg.headers.into_iter().map(|(k, v)| (k, v.into_bytes())).collect(),
        timestamp: chrono::Utc::now(),
    };
    let offsets = pc.produce(vec![record], Compression::NoCompression).await?;
    offsets.first().copied().ok_or_else(|| anyhow::anyhow!("Kafka no devolvió offset"))
}
