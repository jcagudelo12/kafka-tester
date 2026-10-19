//! Servidor web: estado compartido y rutas.

mod error;
mod handlers;

use std::sync::Arc;

use axum::{
    Router,
    http::header,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use rskafka::client::Client;
use tokio::sync::RwLock;

use error::ApiError;

/// Conexión activa a Kafka (brokers + cliente), compartida entre peticiones.
#[derive(Default)]
pub struct AppState {
    client: RwLock<Option<(String, Arc<Client>)>>,
}

pub type Shared = Arc<AppState>;

impl AppState {
    pub async fn set_client(&self, brokers: String, client: Client) {
        *self.client.write().await = Some((brokers, Arc::new(client)));
    }

    async fn client(&self) -> Result<Arc<Client>, ApiError> {
        self.client
            .read()
            .await
            .as_ref()
            .map(|(_, c)| c.clone())
            .ok_or_else(ApiError::not_connected)
    }
}

pub fn router(state: Shared) -> Router {
    Router::new()
        .route("/", get(|| async { Html(include_str!("../../static/index.html")) }))
        .route("/style.css", get(|| async { static_file("text/css", include_str!("../../static/style.css")) }))
        .route("/app.js", get(|| async { static_file("text/javascript", include_str!("../../static/app.js")) }))
        .route("/api/status", get(handlers::status))
        .route("/api/connect", post(handlers::connect))
        .route("/api/topics", get(handlers::list_topics).post(handlers::create_topic))
        .route("/api/produce", post(handlers::produce))
        .route("/api/consume", get(handlers::consume))
        .with_state(state)
}

/// Archivo de `static/` incluido en el binario, con su tipo de contenido.
fn static_file(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    ([(header::CONTENT_TYPE, content_type)], body)
}
