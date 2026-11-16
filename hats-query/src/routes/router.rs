use crate::AppState;
use crate::routes::tap_router;
use axum::Router;

pub(crate) async fn app_router() -> Router<AppState> {
    Router::new().nest("/tap", tap_router().await)
}
