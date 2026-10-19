//! Escucha un topic desde la terminal (equivalente a `consumer.py`).
//!
//! cargo run --bin consumer                      # topic por defecto, solo mensajes nuevos
//! cargo run --bin consumer -- otro-topic earliest

use futures::StreamExt;
use kafka_tester::{config, consumer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let topic = args.next().unwrap_or_else(|| config::DEFAULT_TOPIC.into());
    let from = args.next();

    let brokers = config::brokers_from_env();
    let client = config::connect(&brokers).await?;
    let mut stream = Box::pin(consumer::subscribe(&client, &topic, 0, consumer::start_offset(from.as_deref())).await?);

    println!("Esperando mensajes en {topic}...");
    while let Some(item) = stream.next().await {
        match item {
            Ok(m) => println!("Recibido (offset {}): {}", m.offset, m.value.unwrap_or_default()),
            Err(e) => eprintln!("Error: {e}"),
        }
    }
    Ok(())
}
