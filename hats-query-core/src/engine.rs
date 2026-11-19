//! The query engine over the mounted HATS catalogs.

use crate::adql::{AdqlContext as _, register_adql};
use crate::configuration::{MountConfig, TapTableConfig};
use crate::hats::HatsTableProvider;
use crate::store::{Location, Storage};
use crate::tables::{TableSet, TableSource};
use crate::tap::{KnownTables, TapSchema, tap_schema_tables};
use bytes::Bytes;
use datafusion::catalog::{MemorySchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult, TableReference};
use datafusion::datasource::MemTable;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::object_store::path::Path;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// A query engine reading from the virtual file system.
pub struct Engine {
    /// The storage system: the mount tree and external locations.
    storage: Storage,
    tables: KnownTables,
    /// DataFusion session with ADQL functions and TAP tables, lazily initialized.
    ctx: OnceCell<SessionContext>,
}

impl Engine {
    /// Builds an engine serving the `tables` stored in the `mounts`.
    pub fn new(mounts: Vec<MountConfig>, tables: Vec<TapTableConfig>) -> DFResult<Self> {
        let storage = Storage::from_mount_configs(mounts)?;
        let tables = KnownTables::from_tap_table_configs(&storage, tables)?;
        Ok(Self {
            storage,
            tables,
            ctx: OnceCell::new(),
        })
    }

    /// Queryable tables: the known tables and the TAP_SCHEMA tables, grouped by schema.
    pub async fn table_set(&self) -> DFResult<TableSet> {
        let known = self.tables.named_tables().await?;
        TableSet::new(known.into_iter().chain(tap_schema_tables()))
    }

    async fn ctx(&self) -> DFResult<&SessionContext> {
        self.ctx
            .get_or_try_init(async || {
                let ctx = SessionContext::new();
                register_adql(&ctx);
                self.storage.register_object_stores(&ctx);
                register_tables(&ctx, &self.table_set().await?).await?;
                Ok(ctx)
            })
            .await
    }

    /// Plans an ADQL query, returns the stream of its results.
    pub async fn query(&self, adql: &str) -> DFResult<SendableRecordBatchStream> {
        self.ctx().await?.adql(adql).await?.execute_stream().await
    }

    /// Reads the whole object at `path`, a server path inside the mounts.
    pub async fn read_bytes(&self, path: &Path) -> DFResult<Bytes> {
        self.storage
            .resolve(&Location::Mounted(path.clone()))?
            .read_bytes()
            .await
    }
}

/// Registers the `tables` in the session's default catalog, creating their DataFusion schemas if
/// needed.
async fn register_tables(ctx: &SessionContext, tables: &TableSet) -> DFResult<()> {
    let tap_schema = TapSchema::new(tables)?;
    for table in tables.tables() {
        let provider: Arc<dyn TableProvider> = match table.source() {
            TableSource::Hats(hats) => Arc::new(
                HatsTableProvider::new(hats.clone())
                    .await
                    .map_err(|e| e.context(format!("Cannot load table '{}'", table.name())))?,
            ),
            TableSource::TapSchema => {
                let batch = tap_schema.batch(table.name())?;
                Arc::new(MemTable::try_new(
                    batch.schema(),
                    vec![vec![batch.clone()]],
                )?)
            }
        };
        let table_ref = TableReference::from(table.name());
        if let Some(df_schema_name) = table_ref.schema() {
            register_df_schema_if_missing(ctx, df_schema_name)?;
        }
        ctx.register_table(table_ref, provider)?;
    }
    Ok(())
}

/// Registers an empty DataFusion schema (a namespace of tables, not an Arrow schema) in the
/// session's default catalog, if it has none with this name.
fn register_df_schema_if_missing(ctx: &SessionContext, df_schema_name: &str) -> DFResult<()> {
    let catalog_name = ctx.state().config_options().catalog.default_catalog.clone();
    let catalog = ctx.catalog(&catalog_name).ok_or_else(|| {
        DataFusionError::Internal(format!("Default catalog '{catalog_name}' is not found"))
    })?;
    if catalog.schema(df_schema_name).is_none() {
        catalog.register_schema(df_schema_name, Arc::new(MemorySchemaProvider::new()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::util::{WORKSPACE_ROOT, test_data_path};
    use datafusion::arrow::array::{AsArray, RecordBatch};
    use futures_util::TryStreamExt;

    /// Engine serving test data directories as tables, given as (table name, directory) pairs.
    fn engine(tables: &[(&str, &str)]) -> Engine {
        let mount =
            MountConfig::new("/", WORKSPACE_ROOT.to_owned(), std::path::Path::new("")).unwrap();
        let configs = tables
            .iter()
            .map(|(name, dir)| {
                TapTableConfig::new(name.to_string(), &format!("/{}", test_data_path(dir))).unwrap()
            })
            .collect();
        Engine::new(vec![mount], configs).unwrap()
    }

    /// Runs the ADQL query, returns all its results.
    async fn query(engine: &Engine, adql: &str) -> Vec<RecordBatch> {
        engine
            .query(adql)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    fn num_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[tokio::test]
    async fn hats_tables_queryable() {
        let engine = engine(&[("a.x", "small_sky")]);
        let batches = query(&engine, "SELECT id, ra, dec FROM a.x").await;
        // hats_nrows of small_sky
        assert_eq!(num_rows(&batches), 131);
    }

    #[tokio::test]
    async fn tap_schema_queryable() {
        let engine = engine(&[("a.x", "small_sky"), ("b.y", "small_sky_source")]);

        let batches = query(&engine, "SELECT table_name FROM TAP_SCHEMA.tables").await;
        assert_eq!(num_rows(&batches), 2 + 5);

        for table in ["schemas", "columns", "keys", "key_columns"] {
            query(&engine, &format!("SELECT * FROM TAP_SCHEMA.{table}")).await;
        }

        let batches = query(
            &engine,
            "SELECT column_name FROM TAP_SCHEMA.columns WHERE table_name = 'a.x'",
        )
        .await;
        let column_names: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.column(0).as_string::<i32>().iter().flatten())
            .collect();
        for column in ["id", "ra", "dec", r#""_healpix_29""#] {
            assert!(
                column_names.contains(&column),
                "{column} not in {column_names:?}"
            );
        }
    }

    /// Column names listed in TAP_SCHEMA.columns can be used in ADQL queries as they are.
    #[tokio::test]
    async fn listed_column_names_are_queryable() {
        let engine = engine(&[]);

        let batches = query(
            &engine,
            r#"SELECT column_name FROM TAP_SCHEMA.columns
            WHERE table_name = 'TAP_SCHEMA.columns' AND column_name = '"size"'"#,
        )
        .await;
        assert_eq!(num_rows(&batches), 1);

        query(&engine, r#"SELECT "size" FROM TAP_SCHEMA.columns"#).await;
    }
}
