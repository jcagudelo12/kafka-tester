use std::{collections::BTreeMap, convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{
        Html, IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::{Stream, StreamExt};
use rskafka::{
    client::{
        Client, ClientBuilder,
        consumer::{StartOffset, StreamConsumerBuilder},
        partition::{Compression, UnknownTopicHandling},
    },
    record::Record,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;

#[derive(Default)]
struct AppState {
    client: RwLock<Option<(String, Arc<Client>)>>,
}

type Shared = Arc<AppState>;

const DEFAULT_BROKERS: &str = "192.168.78.165:9092";

/// Error que se devuelve al navegador como JSON `{ "error": "..." }`.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl<E: std::fmt::Display> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError(StatusCode::BAD_GATEWAY, e.to_string())
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

async fn client(state: &Shared) -> Result<Arc<Client>, ApiError> {
    state
        .client
        .read()
        .await
        .as_ref()
        .map(|(_, c)| c.clone())
        .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, "No conectado a Kafka".into()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state: Shared = Arc::new(AppState::default());

    // Conexión automática al arrancar; KAFKA_BROKERS permite usar otro broker.
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| DEFAULT_BROKERS.into());
    {
        match connect_to(&brokers).await {
            Ok(c) => {
                println!("Conectado a Kafka en {brokers}");
                *state.client.write().await = Some((brokers, Arc::new(c)));
            }
            Err(e) => eprintln!("No se pudo conectar a {brokers}: {e}"),
        }
    }

    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("../static/index.html")) }))
        .route("/api/status", get(status))
        .route("/api/connect", post(connect))
        .route("/api/topics", get(list_topics).post(create_topic))
        .route("/api/produce", post(produce))
        .route("/api/consume", get(consume))
        .with_state(state);

    let addr = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("Vista web en http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn connect_to(brokers: &str) -> Result<Client, String> {
    let list = brokers.split(',').map(|s| s.trim().to_string()).collect();
    let fut = ClientBuilder::new(list).client_id("kafka-tester").build();
    match tokio::time::timeout(Duration::from_secs(10), fut).await {
        Ok(r) => r.map_err(|e| e.to_string()),
        Err(_) => Err(format!("tiempo de espera agotado al conectar a {brokers}")),
    }
}

async fn status(State(state): State<Shared>) -> Json<serde_json::Value> {
    let guard = state.client.read().await;
    Json(json!({
        "connected": guard.is_some(),
        "brokers": guard.as_ref().map(|(b, _)| b.clone()),
    }))
}

#[derive(Deserialize)]
struct ConnectReq {
    brokers: String,
}

async fn connect(
    State(state): State<Shared>,
    Json(req): Json<ConnectReq>,
) -> ApiResult<serde_json::Value> {
    let c = connect_to(&req.brokers).await?;
    *state.client.write().await = Some((req.brokers.clone(), Arc::new(c)));
    Ok(Json(json!({ "connected": true, "brokers": req.brokers })))
}

#[derive(Serialize)]
struct TopicInfo {
    name: String,
    partitions: Vec<i32>,
}

async fn list_topics(State(state): State<Shared>) -> ApiResult<Vec<TopicInfo>> {
    let mut topics: Vec<TopicInfo> = client(&state)
        .await?
        .list_topics()
        .await?
        .into_iter()
        .filter(|t| !t.name.starts_with("__"))
        .map(|t| TopicInfo { name: t.name, partitions: t.partitions.into_iter().collect() })
        .collect();
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(topics))
}

#[derive(Deserialize)]
struct CreateTopicReq {
    name: String,
    #[serde(default = "one")]
    partitions: i32,
    #[serde(default = "one_i16")]
    replication: i16,
}
fn one() -> i32 {
    1
}
fn one_i16() -> i16 {
    1
}

async fn create_topic(
    State(state): State<Shared>,
    Json(req): Json<CreateTopicReq>,
) -> ApiResult<serde_json::Value> {
    client(&state)
        .await?
        .controller_client()?
        .create_topic(&req.name, req.partitions, req.replication, 5_000)
        .await?;
    Ok(Json(json!({ "created": req.name })))
}

#[derive(Deserialize)]
struct ProduceReq {
    topic: String,
    #[serde(default)]
    partition: i32,
    key: Option<String>,
    value: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

async fn produce(
    State(state): State<Shared>,
    Json(req): Json<ProduceReq>,
) -> ApiResult<serde_json::Value> {
    let pc = client(&state)
        .await?
        .partition_client(&req.topic, req.partition, UnknownTopicHandling::Retry)
        .await?;
    let record = Record {
        key: req.key.filter(|k| !k.is_empty()).map(String::into_bytes),
        value: Some(req.value.into_bytes()),
        headers: req.headers.into_iter().map(|(k, v)| (k, v.into_bytes())).collect(),
        timestamp: chrono::Utc::now(),
    };
    let offsets = pc.produce(vec![record], Compression::NoCompression).await?;
    Ok(Json(json!({ "offset": offsets.first() })))
}

#[derive(Deserialize)]
struct ConsumeQuery {
    topic: String,
    #[serde(default)]
    partition: i32,
    /// "earliest" o "latest" (por defecto)
    from: Option<String>,
}

/// Transmite al navegador (Server-Sent Events) cada mensaje que llega al topic.
async fn consume(
    State(state): State<Shared>,
    Query(q): Query<ConsumeQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let pc = client(&state)
        .await?
        .partition_client(&q.topic, q.partition, UnknownTopicHandling::Retry)
        .await?;
    let start = match q.from.as_deref() {
        Some("earliest") => StartOffset::Earliest,
        _ => StartOffset::Latest,
    };
    let stream = StreamConsumerBuilder::new(Arc::new(pc), start)
        .with_max_wait_ms(500)
        .build()
        .map(|item| {
            let ev = match item {
                Ok((r, high_watermark)) => {
                    let text = |b: Option<Vec<u8>>| b.map(|b| String::from_utf8_lossy(&b).into_owned());
                    Event::default().event("message").json_data(json!({
                        "offset": r.offset,
                        "highWatermark": high_watermark,
                        "key": text(r.record.key),
                        "value": text(r.record.value),
                        "headers": r.record.headers.into_iter()
                            .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
                            .collect::<BTreeMap<_, _>>(),
                        "timestamp": r.record.timestamp.to_rfc3339(),
                    }))
                    .unwrap()
                }
                Err(e) => Event::default().event("kafka-error").data(e.to_string()),
            };
            Ok(ev)
        });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
