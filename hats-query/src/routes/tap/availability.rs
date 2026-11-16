use crate::reposnses::xml_response;
use askama::Template;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use hats_query_core::dali::DateTimeToIvoa as _;

#[derive(Template)]
#[template(path = "tap/availability.xml")]
struct Tmpl {
    up_since: String,
}

pub(super) async fn availability_handler(
    State(up_since): State<DateTime<Utc>>,
) -> Result<impl IntoResponse, StatusCode> {
    let template = Tmpl {
        up_since: up_since.to_ivoa(),
    };
    let body = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(xml_response(body).await)
}
