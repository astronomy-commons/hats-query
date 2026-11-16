use crate::AppState;
use crate::routes::tap::availability_handler;
use axum::Router;
use axum::routing::get;

pub(crate) async fn tap_router() -> Router<AppState> {
    Router::new().route("/availability", get(availability_handler))
}
