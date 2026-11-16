//! HTTP endpoints, a module per service.

mod doc;
mod router;
mod tap;

use doc::doc_handler;
pub(crate) use router::app_router;
use tap::{TAP_PATH, tap_router};
