use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Reads arrow schema from parquet file bytes, e.g. a footer-only "_common_metadata" file.
pub(crate) fn parquet_schema_from_bytes(bytes: Bytes) -> DFResult<SchemaRef> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    Ok(builder.schema().clone())
}
