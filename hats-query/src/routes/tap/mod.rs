mod availability;
mod router;

use availability::availability_handler;
pub(super) use router::tap_router;

/// URL path of the TAP service.
pub(super) const TAP_PATH: &str = "/tap";
