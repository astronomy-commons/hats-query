use crate::configuration::TapTableConfig;
use crate::hats::Hats;
use crate::store::{Location, Storage};
use crate::tables::{NamedTable, TableSource};
use datafusion::common::{DataFusionError, Result as DFResult};
use futures_util::future::try_join_all;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) struct KnownTables {
    tables: BTreeMap<String, Arc<Hats>>,
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
                    .resolve(&Location::Mounted(config.path))
                    .map_err(|e| e.context(format!("Cannot resolve table '{}'", config.name)))?;
                Ok((config.name, Hats::new(location)))
            })
            .collect::<DFResult<_>>()?;
        Ok(Self { tables })
    }

    /// Loads the known tables' metadata, returns them with their HATS sources.
    pub(crate) async fn named_tables(&self) -> DFResult<Vec<NamedTable>> {
        try_join_all(self.tables.iter().map(|(name, hats)| async move {
            let metadata = hats
                .metadata()
                .await
                .map_err(|e| e.context(format!("Cannot load table '{name}'")))?;
            Ok::<_, DataFusionError>(NamedTable::new(
                name.clone(),
                metadata,
                TableSource::Hats(hats.clone()),
            ))
        }))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::util::{storage, test_data_path};

    /// The location of a test data directory, resolved through `storage()`.
    fn test_data_location(name: &str) -> crate::store::StorePath {
        storage()
            .resolve(&Location::Mounted(test_data_path(name)))
            .unwrap()
    }

    #[tokio::test]
    async fn named_tables_missing_catalog() {
        let known_tables = KnownTables {
            tables: BTreeMap::from([(
                "a.x".to_string(),
                Hats::new(test_data_location("does_not_exist")),
            )]),
        };
        let Err(err) = known_tables.named_tables().await else {
            panic!("expected an error");
        };
        assert!(err.to_string().contains("Cannot load table 'a.x'"), "{err}");
    }

    #[test]
    fn from_tap_table_configs() {
        let configs = vec![
            TapTableConfig::new(
                "a.x".to_string(),
                &format!("/{}", test_data_path("small_sky")),
            )
            .unwrap(),
        ];
        let known_tables = KnownTables::from_tap_table_configs(&storage(), configs).unwrap();
        assert_eq!(known_tables.tables.keys().collect::<Vec<_>>(), ["a.x"]);
    }
}
