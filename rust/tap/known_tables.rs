use crate::hats::Hats;
use crate::tap::tap_schema::{TAP_SCHEMA, TapSchema};
use datafusion::catalog::MemorySchemaProvider;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use pyo3::FromPyObject;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct KnownTables {
    tables: BTreeMap<String, Hats>,
}

impl KnownTables {
    pub(crate) fn from_tap_table_configs(configs: Vec<TapTableConfig>) -> Self {
        let tables = configs
            .into_iter()
            .map(|config| {
                let table = Hats::from_path(config.path.as_str());
                (config.name, table)
            })
            .collect();
        Self { tables }
    }

    /// Registers TAP_SCHEMA tables describing the known tables in the session's default catalog.
    pub(crate) fn register_tap_schema_tables(&self, ctx: &SessionContext) -> DFResult<()> {
        let tap_schema =
            TapSchema::new(self.tables.iter().map(|(name, hats)| (name.as_str(), hats)))?;

        let catalog_name = ctx.state().config_options().catalog.default_catalog.clone();
        let catalog = ctx.catalog(&catalog_name).ok_or_else(|| {
            DataFusionError::Internal(format!("Default catalog '{catalog_name}' is not found"))
        })?;
        // DataFusion lowercases unquoted identifiers, so the schema must be registered
        // lowercased to be found by `FROM TAP_SCHEMA.tables`
        let schema_name = TAP_SCHEMA.to_ascii_lowercase();
        if catalog.schema(&schema_name).is_none() {
            catalog.register_schema(&schema_name, Arc::new(MemorySchemaProvider::new()))?;
        }

        for (name, batch) in tap_schema.named_batches() {
            let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])?;
            // The string is parsed and lowercased, giving tap_schema.<table>
            ctx.register_table(name, Arc::new(table))?;
        }
        Ok(())
    }
}

#[derive(FromPyObject)]
pub(crate) struct TapTableConfig {
    name: String,
    path: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adql::AdqlContext;

    #[tokio::test]
    async fn register_tap_schema_tables() {
        let known_tables = KnownTables {
            tables: BTreeMap::from([
                ("a.x".to_string(), Hats::from_path("a/x")),
                ("b.y".to_string(), Hats::from_path("b/y")),
            ]),
        };
        let ctx = SessionContext::new();
        known_tables.register_tap_schema_tables(&ctx).unwrap();

        let batches = ctx
            .adql("SELECT table_name FROM TAP_SCHEMA.tables")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(num_rows, 2 + 5);

        for table in ["schemas", "columns", "keys", "key_columns"] {
            ctx.adql(&format!("SELECT * FROM TAP_SCHEMA.{table}"))
                .await
                .unwrap();
        }
    }
}
