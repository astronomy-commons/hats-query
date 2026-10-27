use crate::hats::Hats;
use crate::store::location::Location;
use crate::store::storage::Storage;
use crate::tap::tap_schema::{TAP_SCHEMA, TapSchema};
use datafusion::catalog::MemorySchemaProvider;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::MemTable;
use datafusion::object_store::path::Path;
use datafusion::prelude::SessionContext;
use futures_util::future::try_join_all;
use pyo3::FromPyObject;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct KnownTables {
    tables: BTreeMap<String, Hats>,
}

impl KnownTables {
    /// Resolves the configured table paths, which are server paths, through `storage`.
    pub(crate) fn from_tap_table_configs(
        storage: &Storage,
        configs: Vec<TapTableConfig>,
    ) -> DFResult<Self> {
        let tables = configs
            .into_iter()
            .map(|config| {
                let location = storage
                    .resolve(&Location::Mounted(Path::from(config.path.as_str())))
                    .map_err(|e| e.context(format!("Cannot resolve table '{}'", config.name)))?;
                Ok((config.name, Hats::new(location)))
            })
            .collect::<DFResult<_>>()?;
        Ok(Self { tables })
    }

    /// Registers TAP_SCHEMA tables describing the known tables in the session's default catalog.
    pub(crate) async fn register_tap_schema_tables(&self, ctx: &SessionContext) -> DFResult<()> {
        let tables = try_join_all(self.tables.iter().map(|(name, hats)| async move {
            let metadata = hats
                .metadata()
                .await
                .map_err(|e| e.context(format!("Cannot load table '{name}'")))?;
            Ok::<_, DataFusionError>((name.as_str(), metadata))
        }))
        .await?;
        let tap_schema = TapSchema::new(tables)?;

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
    use crate::tests::util::{storage, test_data_location, test_data_path};
    use datafusion::arrow::array::AsArray;

    #[tokio::test]
    async fn register_tap_schema_tables() {
        let known_tables = KnownTables {
            tables: BTreeMap::from([
                (
                    "a.x".to_string(),
                    Hats::new(test_data_location("small_sky")),
                ),
                (
                    "b.y".to_string(),
                    Hats::new(test_data_location("small_sky_source")),
                ),
            ]),
        };
        let ctx = SessionContext::new();
        known_tables.register_tap_schema_tables(&ctx).await.unwrap();

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

        let batches = ctx
            .adql("SELECT column_name FROM TAP_SCHEMA.columns WHERE table_name = 'a.x'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
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
        let known_tables = KnownTables {
            tables: BTreeMap::new(),
        };
        let ctx = SessionContext::new();
        known_tables.register_tap_schema_tables(&ctx).await.unwrap();

        let batches = ctx
            .adql(
                r#"SELECT column_name FROM TAP_SCHEMA.columns
                WHERE table_name = 'TAP_SCHEMA.columns' AND column_name = '"size"'"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(num_rows, 1);

        ctx.adql(r#"SELECT "size" FROM TAP_SCHEMA.columns"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn register_tap_schema_tables_missing_catalog() {
        let known_tables = KnownTables {
            tables: BTreeMap::from([(
                "a.x".to_string(),
                Hats::new(test_data_location("does_not_exist")),
            )]),
        };
        let err = known_tables
            .register_tap_schema_tables(&SessionContext::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Cannot load table 'a.x'"), "{err}");
    }

    #[test]
    fn from_tap_table_configs() {
        let configs = vec![TapTableConfig {
            name: "a.x".to_string(),
            path: format!("/{}", test_data_path("small_sky")),
        }];
        let known_tables = KnownTables::from_tap_table_configs(&storage(), configs).unwrap();
        assert_eq!(known_tables.tables.keys().collect::<Vec<_>>(), ["a.x"]);
    }
}
