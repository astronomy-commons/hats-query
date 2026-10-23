use crate::hats::Hats;
use pyo3::FromPyObject;
use std::collections::BTreeMap;

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
}

#[derive(FromPyObject)]
pub(crate) struct TapTableConfig {
    name: String,
    path: String,
}
