//! Origins of the served tables' rows.

use crate::hats::Hats;
use std::sync::Arc;

/// Origin of a table's rows.
pub enum TableSource {
    /// HATS catalog or collection.
    Hats(Arc<Hats>),
    /// TAP_SCHEMA, the TAP metadata tables describing the served tables.
    TapSchema,
}
