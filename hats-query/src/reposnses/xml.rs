use axum::http::header;
use axum::response::IntoResponse;

/// Wraps `body`, an XML document, into a `text/xml` response.
pub(crate) async fn xml_response(body: String) -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/xml; charset=utf-8")], body)
}
