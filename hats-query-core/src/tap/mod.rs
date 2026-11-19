mod known_tables;
mod tap_schema;

pub(crate) use known_tables::KnownTables;
pub(crate) use tap_schema::schema_of;
pub use tap_schema::{TAP_SCHEMA, TapSchema};
