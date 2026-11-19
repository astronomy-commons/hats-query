//! VOSI tables endpoint.

use crate::reposnses::xml_response;
use askama::Template;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use hats_query_core::Engine;
use hats_query_core::tables::TableSet;
use std::sync::Arc;

/// VOSI tables template.
#[derive(Template)]
#[template(path = "tap/tables.xml")]
struct Tmpl<'a> {
    /// Known and TAP_SCHEMA tables, grouped by schema.
    table_set: &'a TableSet,
}

/// VOSI tables document describing the known tables and TAP_SCHEMA.
pub(super) async fn tables_handler(
    State(engine): State<Arc<Engine>>,
) -> Result<impl IntoResponse, StatusCode> {
    let table_set = engine.table_set().await.map_err(|err| {
        tracing::error!("cannot load the table metadata: {err}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let template = Tmpl {
        table_set: &table_set,
    };
    let body = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // The tables follow the server configuration, so caches must not keep them across restarts
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        xml_response(body).await,
    ))
}
