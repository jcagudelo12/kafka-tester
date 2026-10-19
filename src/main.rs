//! Vista web: `cargo run` y abre http://127.0.0.1:3000

use std::sync::Arc;

use kafka_tester::{config, web};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state: web::Shared = Arc::new(web::AppState::default());

    // Conexión automática al arrancar; KAFKA_BROKERS permite usar otro broker.
    let brokers = config::brokers_from_env();
    match config::connect(&brokers).await {
        Ok(client) => {
            println!("Conectado a Kafka en {brokers}");
            state.set_client(brokers, client).await;
        }
        Err(e) => eprintln!("{e:#}"),
    }

    let addr = std::env::var("BIND").unwrap_or_else(|_| config::DEFAULT_BIND.into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("Vista web en http://{addr}");
    axum::serve(listener, web::router(state)).await?;
    Ok(())
}
