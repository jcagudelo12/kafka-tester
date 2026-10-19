//! Listar y crear topics.

use rskafka::client::Client;
use serde::Serialize;

#[derive(Serialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<i32>,
}

/// Topics del clúster, sin los internos (`__consumer_offsets`, ...), ordenados por nombre.
pub async fn list(client: &Client) -> anyhow::Result<Vec<TopicInfo>> {
    let mut topics: Vec<TopicInfo> = client
        .list_topics()
        .await?
        .into_iter()
        .filter(|t| !t.name.starts_with("__"))
        .map(|t| TopicInfo { name: t.name, partitions: t.partitions.into_iter().collect() })
        .collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
}

pub async fn create(client: &Client, name: &str, partitions: i32, replication: i16) -> anyhow::Result<()> {
    client
        .controller_client()?
        .create_topic(name, partitions, replication, 5_000)
        .await?;
    Ok(())
}
