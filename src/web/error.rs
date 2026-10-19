use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

/// Error que se devuelve al navegador como JSON `{ "error": "..." }`.
pub struct ApiError(StatusCode, String);

impl ApiError {
    pub fn not_connected() -> Self {
        ApiError(StatusCode::BAD_REQUEST, "No conectado a Kafka".into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError(StatusCode::BAD_GATEWAY, format!("{e:#}"))
    }
}

pub type ApiResult<T> = Result<Json<T>, ApiError>;
