//! Routing of the TAP service.

use crate::AppState;
use crate::routes::tap::{availability_handler, capabilities_handler, examples_handler};
use axum::Router;
use axum::routing::get;

/// Endpoints of the TAP service, relative to its base path.
pub(crate) async fn tap_router() -> Router<AppState> {
    Router::new()
        .route("/availability", get(availability_handler))
        .route("/capabilities", get(capabilities_handler))
        .route("/examples", get(examples_handler))
}
