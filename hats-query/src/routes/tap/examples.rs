//! DALI examples endpoint.

use askama::Template;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use hats_query_core::configuration::{Configuration, TapExampleConfig};
use std::sync::Arc;

/// DALI examples template.
#[derive(Template)]
#[template(path = "tap/examples.html")]
struct Tmpl<'a> {
    /// Example queries from the config.
    examples: &'a [TapExampleConfig],
}

/// DALI examples page listing the configured example queries.
pub(super) async fn examples_handler(
    State(config): State<Arc<Configuration>>,
) -> Result<impl IntoResponse, StatusCode> {
    let template = Tmpl {
        examples: &config.tap.examples,
    };
    let body = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(body))
}
