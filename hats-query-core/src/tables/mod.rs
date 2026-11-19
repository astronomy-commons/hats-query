//! Descriptions of the served tables: their columns, origins and grouping by schema.

mod named_table;
mod table_metadata;
mod table_set;
mod table_source;

pub use named_table::NamedTable;
pub use table_metadata::{ColumnMetadata, TableMetadata};
pub use table_set::TableSet;
pub use table_source::TableSource;
