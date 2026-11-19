//! TAP service, see https://www.ivoa.net/documents/TAP/

mod availability;
mod capabilities;
mod examples;
mod router;
mod tables;

use availability::availability_handler;
use capabilities::capabilities_handler;
use examples::examples_handler;
pub(super) use router::tap_router;
use tables::tables_handler;

/// URL path of the TAP service.
pub(super) const TAP_PATH: &str = "/tap";
