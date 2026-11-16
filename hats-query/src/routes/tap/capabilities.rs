//! VOSI capabilities endpoint.

use crate::reposnses::xml_response;
use crate::routes::tap::TAP_PATH;
use askama::Template;
use axum::http::uri::Authority;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;

/// VOSI capabilities template.
#[derive(Template)]
#[template(path = "tap/capabilities.xml")]
struct Tmpl {
    /// Base URL of the TAP service.
    tap_url: String,
    /// Full URL of the VOSI capabilities endpoint.
    capabilities_url: String,
    /// Full URL of the VOSI availability endpoint.
    availability_url: String,
    /// Full URL of the VOSI tables endpoint.
    tables_url: String,
    /// Full URL of the DALI examples endpoint.
    examples_url: String,
}

/// VOSI capabilities of the TAP service, with absolute URLs on the requested host.
pub(super) async fn capabilities_handler(
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    // Only "host[:port]", so that a malformed Host header can't change the rest of the URLs
    let authority: Authority = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .and_then(|host| host.parse().ok())
        .filter(|authority: &Authority| !authority.as_str().contains('@'))
        .ok_or(StatusCode::BAD_REQUEST)?;
    let tap_url = format!("http://{authority}{TAP_PATH}");
    let template = Tmpl {
        capabilities_url: format!("{tap_url}/capabilities"),
        availability_url: format!("{tap_url}/availability"),
        tables_url: format!("{tap_url}/tables"),
        examples_url: format!("{tap_url}/examples"),
        tap_url,
    };
    let body = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // The URLs depend on the request headers, so a shared cache must not serve them to others
    // See https://github.com/astronomy-commons/hats-query/issues/91
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        xml_response(body).await,
    ))
}
