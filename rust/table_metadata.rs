use datafusion::arrow::datatypes::SchemaRef;

/// Metadata of the table.
pub(crate) struct TableMetadata {
    /// Arrow schema of the table.
    pub(crate) schema: SchemaRef,
}
