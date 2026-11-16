//! Routing of the whole server.

use crate::AppState;
use crate::routes::{TAP_PATH, doc_handler, tap_router};
use axum::Router;
use axum::routing::get;

/// Endpoints of all services, by their URL paths.
pub(crate) async fn app_router() -> Router<AppState> {
    Router::new()
        .route("/doc", get(doc_handler))
        .nest(TAP_PATH, tap_router().await)
}
