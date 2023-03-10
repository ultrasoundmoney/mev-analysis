use axum::{http::StatusCode, Json};

pub type ApiResponse<T> = Result<Json<T>, (StatusCode, String)>;
