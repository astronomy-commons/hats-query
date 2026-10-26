use datafusion::arrow::datatypes::SchemaRef;
use std::collections::BTreeSet;

/// Metadata of the table.
pub(crate) struct TableMetadata {
    /// Arrow schema of the table.
    pub(crate) schema: SchemaRef,
    /// Names of the indexed columns.
    pub(crate) indexed_columns: BTreeSet<String>,
}
