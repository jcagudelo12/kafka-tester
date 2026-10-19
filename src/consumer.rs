//! Lectura continua de mensajes de un topic.

use std::{collections::BTreeMap, sync::Arc};

use futures::{Stream, StreamExt};
use rskafka::client::{
    Client,
    consumer::{StartOffset, StreamConsumerBuilder},
    partition::UnknownTopicHandling,
};
use serde::Serialize;

pub use rskafka::client::consumer::StartOffset as From;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub offset: i64,
    pub high_watermark: i64,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub timestamp: String,
}

/// "earliest" → desde el inicio del topic; cualquier otra cosa → solo mensajes nuevos.
pub fn start_offset(from: Option<&str>) -> StartOffset {
    match from {
        Some("earliest") => StartOffset::Earliest,
        _ => StartOffset::Latest,
    }
}

/// Stream infinito con cada mensaje que llega a `topic`/`partition`.
pub async fn subscribe(
    client: &Client,
    topic: &str,
    partition: i32,
    start: StartOffset,
) -> anyhow::Result<impl Stream<Item = Result<Message, String>> + use<>> {
    let pc = client
        .partition_client(topic, partition, UnknownTopicHandling::Retry)
        .await?;
    let stream = StreamConsumerBuilder::new(Arc::new(pc), start)
        .with_max_wait_ms(500)
        .build()
        .map(|item| {
            let (r, high_watermark) = item.map_err(|e| e.to_string())?;
            let text = |b: Vec<u8>| String::from_utf8_lossy(&b).into_owned();
            Ok(Message {
                offset: r.offset,
                high_watermark,
                key: r.record.key.map(text),
                value: r.record.value.map(text),
                headers: r.record.headers.into_iter().map(|(k, v)| (k, text(v))).collect(),
                timestamp: r.record.timestamp.to_rfc3339(),
            })
        });
    Ok(stream)
}
