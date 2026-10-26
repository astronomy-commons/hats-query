#![allow(dead_code)] // TODO: remove this when the code is used

use crate::engine::Engine;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::object_store::path::Path;
use itertools::Itertools;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::sync::Arc;

const COLLECTION_PROPERTIES_FILE: &str = "collection.properties";
const HATS_PROPERTIES_FILE: &str = "hats.properties";
const HATS_PROPERTIES_FILE_DEPRECATED: &str = "properties";

/// A HATS properties file in java-properties format.
pub(super) trait Properties: Sized {
    /// Candidate file names, the first existing one is used.
    const FILE_NAMES: &'static [&'static str];

    fn from_map(map: HashMap<String, String>) -> DFResult<Self>;

    fn from_reader(reader: impl Read) -> DFResult<Self> {
        let map =
            java_properties::read(reader).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Self::from_map(map)
    }

    async fn from_dir(engine: Arc<Engine>, path: &Path) -> DFResult<Self> {
        for file_name in Self::FILE_NAMES {
            if let Ok(bytes) = engine.read_bytes(&path.clone().join(*file_name)).await {
                return Self::from_reader(bytes.as_ref());
            }
        }
        Err(DataFusionError::Configuration(format!(
            "Missing {} in path '{}'",
            Self::FILE_NAMES
                .iter()
                .map(|f| format!("'{f}'"))
                .join(" or "),
            path
        )))
    }
}

/// Representation of the properties of a HATS collection, as read from "collection.properties" file.
/// New fields can be added later.
pub(super) struct CollectionProperties {
    pub hats_primary_table_url: String,
    pub all_indexes: BTreeMap<String, String>,
}

impl Properties for CollectionProperties {
    const FILE_NAMES: &'static [&'static str] = &[COLLECTION_PROPERTIES_FILE];

    fn from_map(map: HashMap<String, String>) -> DFResult<Self> {
        let hats_primary_table_url =
            map.get("hats_primary_table_url").cloned().ok_or_else(|| {
                DataFusionError::Configuration(
                    "Missing 'hats_primary_table_url' in collection.properties".to_string(),
                )
            })?;
        let all_indexes = map
            .get("all_indexes")
            .map(|s| space_separated_list_to_map(s))
            .unwrap_or_default();
        Ok(Self {
            hats_primary_table_url,
            all_indexes,
        })
    }
}

/// "hats.properties" with fallback to the deprecated "properties" file.
const CATALOG_PROPERTIES_FILES: &[&str] = &[HATS_PROPERTIES_FILE, HATS_PROPERTIES_FILE_DEPRECATED];

const DEFAULT_COL_HEALPIX: &str = "_healpix_29";
const DEFAULT_COL_HEALPIX_ORDER: u8 = 29;
const DEFAULT_NPIX_SUFFIX: &str = ".parquet";

pub(super) struct HatsProperties {
    pub nrows: Option<u64>,
    pub col_ra: String,
    pub col_dec: String,
    pub cols_default: Option<Vec<String>>,
    pub col_healpix: String,
    pub col_healpix_order: u8,
    pub npix_suffix: String,
}

impl Properties for HatsProperties {
    const FILE_NAMES: &'static [&'static str] = CATALOG_PROPERTIES_FILES;

    fn from_map(map: HashMap<String, String>) -> DFResult<Self> {
        let nrows = map
            .get("hats_nrows")
            .map(|s| {
                s.parse::<u64>().map_err(|e| {
                DataFusionError::Configuration(format!(
                    "Invalid 'hats_nrows' value in hats.properties (unsigned integer expected): {}",
                    e
                ))
            })
            })
            .transpose()?;
        let col_ra = map.get("hats_col_ra").cloned().ok_or_else(|| {
            DataFusionError::Configuration("Missing 'hats_col_ra' in hats.properties".to_string())
        })?;
        let col_dec = map.get("hats_col_dec").cloned().ok_or_else(|| {
            DataFusionError::Configuration("Missing 'hats_col_dec' in hats.properties".to_string())
        })?;
        let cols_default = map
            .get("hats_cols_default")
            .map(|s| space_separated_list_to_vec(s));
        let col_healpix = map
            .get("hats_col_healpix")
            .cloned()
            .unwrap_or_else(|| DEFAULT_COL_HEALPIX.to_string());
        let col_healpix_order = map
            .get("hats_col_healpix_order")
            .map(|s| s.parse::<u8>())
            .transpose()
            .map_err(|e| {
                DataFusionError::Configuration(format!(
                    "Invalid 'hats_col_healpix_order' value in hats.properties (unsigned integer expected): {}",
                    e
                ))
            })?.unwrap_or(DEFAULT_COL_HEALPIX_ORDER);
        let npix_suffix = map
            .get("hats_npix_suffix")
            .cloned()
            .unwrap_or_else(|| DEFAULT_NPIX_SUFFIX.to_string());
        Ok(Self {
            nrows,
            col_ra,
            col_dec,
            cols_default,
            col_healpix,
            col_healpix_order,
            npix_suffix,
        })
    }
}

pub(super) struct IndexProperties {
    pub hats_primary_table_url: String,
    pub hats_index_column: String,
    pub hats_npix_suffix: String,
}

impl Properties for IndexProperties {
    const FILE_NAMES: &'static [&'static str] = CATALOG_PROPERTIES_FILES;

    fn from_map(map: HashMap<String, String>) -> DFResult<Self> {
        let hats_primary_table_url =
            map.get("hats_primary_table_url").cloned().ok_or_else(|| {
                DataFusionError::Configuration(
                    "Missing 'hats_primary_table_url' in index.properties".to_string(),
                )
            })?;
        let hats_index_column = map.get("hats_index_column").cloned().ok_or_else(|| {
            DataFusionError::Configuration(
                "Missing 'hats_index_column' in index.properties".to_string(),
            )
        })?;
        let hats_npix_suffix = map
            .get("hats_npix_suffix")
            .cloned()
            .unwrap_or_else(|| DEFAULT_NPIX_SUFFIX.to_string());
        Ok(Self {
            hats_primary_table_url,
            hats_index_column,
            hats_npix_suffix,
        })
    }
}

fn space_separated_list_to_vec(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// HATS standard uses a space-separated "key1 value1 key2 value2" list for some fields.
fn space_separated_list_to_map(input: &str) -> BTreeMap<String, String> {
    input
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .tuples()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::util::{engine, test_data_path};

    #[tokio::test]
    async fn collection_properties_from_dir() {
        let path = test_data_path("small_sky_o1_collection");
        let properties = CollectionProperties::from_dir(engine(), &path)
            .await
            .unwrap();
        assert_eq!(properties.hats_primary_table_url, "small_sky_order1");
        assert_eq!(
            properties.all_indexes,
            BTreeMap::from([("id".to_string(), "small_sky_order1_id_index".to_string())])
        );
    }

    #[tokio::test]
    async fn collection_properties_missing_file() {
        let path = test_data_path("small_sky");
        assert!(
            CollectionProperties::from_dir(engine(), &path)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn hats_properties_from_dir() {
        let path = test_data_path("small_sky");
        let properties = HatsProperties::from_dir(engine(), &path).await.unwrap();
        assert_eq!(properties.nrows, Some(131));
        assert_eq!(properties.col_ra, "ra");
        assert_eq!(properties.col_dec, "dec");
        assert_eq!(properties.cols_default, None);
        assert_eq!(properties.col_healpix, "_healpix_29");
        assert_eq!(properties.col_healpix_order, 29);
        assert_eq!(properties.npix_suffix, ".parquet");
    }

    #[tokio::test]
    async fn index_properties_from_dir() {
        let path = test_data_path("small_sky_o1_collection/small_sky_order1_id_index");
        let properties = IndexProperties::from_dir(engine(), &path).await.unwrap();
        assert_eq!(
            properties.hats_primary_table_url,
            "small_sky_o1_collection/small_sky_order1"
        );
        assert_eq!(properties.hats_index_column, "id");
        assert_eq!(properties.hats_npix_suffix, ".parquet");
    }

    #[tokio::test]
    async fn hats_properties_missing_file() {
        let path = test_data_path("small_sky_o1_collection");
        let result = HatsProperties::from_dir(engine(), &path).await;
        assert!(matches!(result, Err(DataFusionError::Configuration(_))));
    }
}
