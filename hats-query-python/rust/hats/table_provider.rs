use crate::hats::Hats;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// DataFusion table of a HATS catalog or collection.
pub(crate) struct HatsTableProvider {
    hats: Arc<Hats>,
    /// The schema of the primary catalog, loaded upfront because `TableProvider::schema` is sync.
    schema: SchemaRef,
}

impl HatsTableProvider {
    pub(crate) async fn new(hats: Arc<Hats>) -> DFResult<Self> {
        let schema = hats.metadata().await?.schema();
        Ok(Self { hats, schema })
    }
}

impl Debug for HatsTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HatsTableProvider").finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for HatsTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Temporary implementation: lists and scans all parquet files of the primary catalog.
        // It will be reimplemented to get the partition paths through Hats, applying spatial
        // filtering to select the partitions.
        let catalog_location = self.hats.catalog_location().await?;
        // The trailing slash makes it a directory to list recursively. The store is registered in
        // the session, which is true for the mounts.
        let url = ListingTableUrl::parse(format!("{}/dataset/", catalog_location.url()))?;
        // The extension skips "_metadata" and "_common_metadata" files
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(self.schema.clone());
        ListingTable::try_new(config)?
            .scan(state, projection, filters, limit)
            .await
    }
}
