//! Envía un mensaje desde la terminal (equivalente a `producer.py`).
//!
//! cargo run --bin producer -- "mi mensaje"            # topic por defecto
//! cargo run --bin producer -- "mi mensaje" otro-topic

use kafka_tester::{config, producer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let value = args.next().unwrap_or_else(|| "Primer mensaje desde Rust".into());
    let topic = args.next().unwrap_or_else(|| config::DEFAULT_TOPIC.into());

    let brokers = config::brokers_from_env();
    let client = config::connect(&brokers).await?;
    let offset = producer::send(&client, &topic, 0, producer::OutgoingMessage::text(&value)).await?;
    println!("Enviado a {topic} (offset {offset}): {value}");
    Ok(())
}
