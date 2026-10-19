//! Configuración y conexión al broker.

use std::time::Duration;

use anyhow::{Context, anyhow};
use rskafka::client::{Client, ClientBuilder};

pub const DEFAULT_BROKERS: &str = "192.168.78.165:9092";
pub const DEFAULT_TOPIC: &str = "pruebas";
pub const DEFAULT_BIND: &str = "127.0.0.1:3000";

/// Brokers de `KAFKA_BROKERS`, o `DEFAULT_BROKERS` si no está definida.
pub fn brokers_from_env() -> String {
    std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| DEFAULT_BROKERS.into())
}

/// Conecta a uno o varios brokers separados por coma ("host:9092,host2:9092").
pub async fn connect(brokers: &str) -> anyhow::Result<Client> {
    let list = brokers.split(',').map(|s| s.trim().to_string()).collect();
    let fut = ClientBuilder::new(list).client_id("kafka-tester").build();
    tokio::time::timeout(Duration::from_secs(10), fut)
        .await
        .map_err(|_| anyhow!("tiempo de espera agotado al conectar a {brokers}"))?
        .with_context(|| format!("no se pudo conectar a {brokers}"))
}
