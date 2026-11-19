//! Documentation page.

use crate::routes::TAP_PATH;
use askama::Template;
use axum::http::StatusCode;
use axum::response::Html;

/// Documentation page template.
#[derive(Template)]
#[template(path = "doc.html")]
struct Tmpl {
    /// URL path of the TAP service.
    tap_path: &'static str,
}

/// Human-readable page describing where the services live.
pub(super) async fn doc_handler() -> Result<Html<String>, StatusCode> {
    let body = Tmpl { tap_path: TAP_PATH }
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(body))
}
