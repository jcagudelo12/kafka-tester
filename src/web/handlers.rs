//! Handlers HTTP: traducen JSON/query de la web a llamadas de los módulos de Kafka.

use std::{collections::BTreeMap, convert::Infallible};

use axum::{
    Json,
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    Shared,
    error::{ApiError, ApiResult},
};
use crate::{config, consumer, producer, topics};

pub async fn status(State(state): State<Shared>) -> Json<Value> {
    let guard = state.client.read().await;
    Json(json!({
        "connected": guard.is_some(),
        "brokers": guard.as_ref().map(|(b, _)| b.clone()),
    }))
}

#[derive(Deserialize)]
pub struct ConnectReq {
    brokers: String,
}

pub async fn connect(State(state): State<Shared>, Json(req): Json<ConnectReq>) -> ApiResult<Value> {
    let client = config::connect(&req.brokers).await?;
    state.set_client(req.brokers.clone(), client).await;
    Ok(Json(json!({ "connected": true, "brokers": req.brokers })))
}

pub async fn list_topics(State(state): State<Shared>) -> ApiResult<Vec<topics::TopicInfo>> {
    Ok(Json(topics::list(&*state.client().await?).await?))
}

#[derive(Deserialize)]
pub struct CreateTopicReq {
    name: String,
    partitions: Option<i32>,
    replication: Option<i16>,
}

pub async fn create_topic(State(state): State<Shared>, Json(req): Json<CreateTopicReq>) -> ApiResult<Value> {
    let client = state.client().await?;
    topics::create(&client, &req.name, req.partitions.unwrap_or(1), req.replication.unwrap_or(1)).await?;
    Ok(Json(json!({ "created": req.name })))
}

#[derive(Deserialize)]
pub struct ProduceReq {
    topic: String,
    #[serde(default)]
    partition: i32,
    key: Option<String>,
    value: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

pub async fn produce(State(state): State<Shared>, Json(req): Json<ProduceReq>) -> ApiResult<Value> {
    let client = state.client().await?;
    let msg = producer::OutgoingMessage { key: req.key, value: req.value, headers: req.headers };
    let offset = producer::send(&client, &req.topic, req.partition, msg).await?;
    Ok(Json(json!({ "offset": offset })))
}

#[derive(Deserialize)]
pub struct ConsumeQuery {
    topic: String,
    #[serde(default)]
    partition: i32,
    /// "earliest" o "latest" (por defecto)
    from: Option<String>,
}

/// Transmite al navegador (Server-Sent Events) cada mensaje que llega al topic.
pub async fn consume(
    State(state): State<Shared>,
    Query(q): Query<ConsumeQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let client = state.client().await?;
    let start = consumer::start_offset(q.from.as_deref());
    let stream = consumer::subscribe(&client, &q.topic, q.partition, start)
        .await?
        .map(|item| {
            Ok(match item {
                Ok(msg) => Event::default().event("message").json_data(msg).unwrap(),
                Err(e) => Event::default().event("kafka-error").data(e),
            })
        });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
