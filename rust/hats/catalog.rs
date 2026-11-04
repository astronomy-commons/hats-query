#![allow(dead_code)] // TODO: remove this when the code is used

use crate::common::{SingleCache, parquet_schema_from_bytes};
use crate::engine::Engine;
use crate::hats::properties::{CollectionProperties, HatsProperties, IndexProperties, Properties};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::object_store::path::Path;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::OnceCell;

const DATASET_DIR: &str = "dataset";
const SCHEMA_FILE: &str = "_common_metadata";

/// Representation of a HATS collection or catalog.
pub(crate) struct Hats {
    /// Path specified by the user/config, it may point to a collection or a catalog.
    discovery_path: Path,
    /// Layzily initialized inner representation of the HATS collection or catalog.
    inner: SingleCache<Arc<HatsInner>>,
}

impl Hats {
    /// Creates a lazily initialized HATS catalog from a path, which should point to
    /// either a HATS collection (with "collection.properties" file) or a HATS catalog
    /// (with "hats.properties" file).
    /// We assume that collection.properties' hats_primary_table_url is a relative path,
    /// available in the same directory as collection.properties. Remote paths are not supported
    /// yet.
    /// In the future we may support discovery of catalog/collection by moving up the
    /// directory tree, but for now we require the user to specify the path explicitly.
    pub(crate) fn from_path(discovery_path: impl Into<Path>) -> Self {
        Self {
            discovery_path: discovery_path.into(),
            inner: SingleCache::new(),
        }
    }

    async fn get_inner(&self, engine: Arc<Engine>) -> DFResult<Arc<HatsInner>> {
        self.inner
            .try_get_with(async || {
                HatsInner::try_from_discovery_path(engine, &self.discovery_path)
                    .await
                    .map(Arc::new)
            })
            .await
    }

    async fn schema(&self, engine: Arc<Engine>) -> DFResult<SchemaRef> {
        self.get_inner(engine.clone()).await?.schema(engine).await
    }
}

struct HatsInner {
    /// Primary HATS catalog, lazily initialized.
    catalog: HatsCatalog,
    /// HATS collection.
    collection: Option<HatsCollection>,
}

impl HatsInner {
    async fn try_from_discovery_path(engine: Arc<Engine>, discovery_path: &Path) -> DFResult<Self> {
        let collection = HatsCollection::try_from_dir(engine.clone(), discovery_path)
            .await
            .ok();
        match collection {
            Some(collection) => {
                let catalog = collection.find_catalog(engine.clone()).await?;
                Ok(Self {
                    catalog,
                    collection: Some(collection),
                })
            }
            None => match HatsCatalog::try_from_dir(engine.clone(), discovery_path).await {
                Ok(catalog) => Ok(Self {
                    catalog,
                    collection: None,
                }),
                Err(err) => Err(DataFusionError::Configuration(format!(
                    "Path '{}' is neither a HATS collection nor a catalog: {err}",
                    discovery_path
                ))),
            },
        }
    }

    async fn schema(&self, engine: Arc<Engine>) -> DFResult<SchemaRef> {
        self.catalog.schema(engine).await
    }
}

/// Representation of a HATS collection.
struct HatsCollection {
    /// Collection path, one with "collection.properties" file.
    path: Path,
    /// Lazily initialized inner representation of the collection, which requires propertis file
    /// parsing.
    lazy_inner: OnceCell<LazilyInitializedHatsCollection>,
}

impl HatsCollection {
    fn new(path: impl Into<Path>) -> Self {
        Self {
            path: path.into(),
            lazy_inner: OnceCell::new(),
        }
    }

    async fn new_with_properties(path: impl Into<Path>, properties: CollectionProperties) -> Self {
        Self {
            path: path.into(),
            lazy_inner: OnceCell::new_with(Some(LazilyInitializedHatsCollection::new(properties))),
        }
    }

    async fn lazy_inner(&self, engine: Arc<Engine>) -> DFResult<&LazilyInitializedHatsCollection> {
        let engine = Arc::clone(&engine);
        let path = self.path.clone();
        self.lazy_inner
            .get_or_try_init(async move || {
                let collection_properties = CollectionProperties::from_dir(engine, &path).await?;
                Ok(LazilyInitializedHatsCollection::new(collection_properties))
            })
            .await
    }

    async fn try_from_dir(engine: Arc<Engine>, path: &Path) -> DFResult<Self> {
        let collection_properties = CollectionProperties::from_dir(engine, path).await?;
        Ok(Self::new_with_properties(path.clone(), collection_properties).await)
    }

    async fn properties(&self, engine: Arc<Engine>) -> DFResult<&CollectionProperties> {
        Ok(&self.lazy_inner(engine).await?.properties)
    }

    async fn indexes(&self, engine: Arc<Engine>) -> DFResult<impl Iterator<Item = &str>> {
        Ok(self
            .lazy_inner(engine)
            .await?
            .index_catalogs
            .keys()
            .map(String::as_str))
    }

    async fn find_catalog(&self, engine: Arc<Engine>) -> DFResult<HatsCatalog> {
        let properties = self.properties(engine.clone()).await?;
        let catalog_path = self
            .path
            .clone()
            .join(properties.hats_primary_table_url.as_str());
        HatsCatalog::try_from_dir(engine, &catalog_path).await
    }
}

/// Lazily initialized inner representation of a HATS collection, which requires properties file
/// parsing.
struct LazilyInitializedHatsCollection {
    /// Properties of the collection, lazily initialized.
    properties: CollectionProperties,
    /// Index catalogs, mapping from index column to catalog.
    index_catalogs: BTreeMap<String, OnceCell<IndexCatalog>>,
}

impl LazilyInitializedHatsCollection {
    fn new(properties: CollectionProperties) -> Self {
        let index_catalogs = properties
            .all_indexes
            .keys()
            .map(|column| (column.clone(), OnceCell::new()))
            .collect();
        Self {
            properties,
            index_catalogs,
        }
    }
}

/// Representation of a HATS catalog.
struct HatsCatalog {
    /// Catalog path, one with "hats.properties" file.
    path: Path,
    /// Properties of the catalog, lazily initialized.
    properties: OnceCell<HatsProperties>,
    /// The schema of the catalog, lazily initialized.
    schema: OnceCell<SchemaRef>,
}

impl CatalogDir for HatsCatalog {
    type Properties = HatsProperties;

    fn new(path: impl Into<Path>, properties: Option<HatsProperties>) -> Self {
        Self {
            path: path.into(),
            properties: OnceCell::new_with(properties),
            schema: OnceCell::new(),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn properties_cell(&self) -> &OnceCell<HatsProperties> {
        &self.properties
    }

    fn schema_cell(&self) -> &OnceCell<SchemaRef> {
        &self.schema
    }
}

/// Representation of a HATS index catalog.
struct IndexCatalog {
    /// Index catalog path, one with "hats.properties" file.
    path: Path,
    /// Properties of the index catalog, lazily initialized.
    properties: OnceCell<IndexProperties>,
    /// The schema of the index catalog, lazily initialized.
    schema: OnceCell<SchemaRef>,
}

impl CatalogDir for IndexCatalog {
    type Properties = IndexProperties;

    fn new(path: impl Into<Path>, properties: Option<IndexProperties>) -> Self {
        Self {
            path: path.into(),
            properties: OnceCell::new_with(properties),
            schema: OnceCell::new(),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn properties_cell(&self) -> &OnceCell<IndexProperties> {
        &self.properties
    }

    fn schema_cell(&self) -> &OnceCell<SchemaRef> {
        &self.schema
    }
}

/// A HATS catalog directory, one with "hats.properties" file and "dataset/" subdirectory.
///
/// Implementors provide `new` and access to their lazily initialized fields,
/// discovery, properties and schema loading are shared.
trait CatalogDir: Sized {
    type Properties: Properties;

    /// Creates a catalog, properties are loaded lazily if not provided.
    fn new(path: impl Into<Path>, properties: Option<Self::Properties>) -> Self;

    fn path(&self) -> &Path;

    fn properties_cell(&self) -> &OnceCell<Self::Properties>;

    fn schema_cell(&self) -> &OnceCell<SchemaRef>;

    async fn try_from_dir(engine: Arc<Engine>, path: &Path) -> DFResult<Self> {
        let properties = Self::Properties::from_dir(engine, path).await?;
        Ok(Self::new(path.clone(), Some(properties)))
    }

    async fn properties(&self, engine: Arc<Engine>) -> DFResult<&Self::Properties> {
        self.properties_cell()
            .get_or_try_init(async || Self::Properties::from_dir(engine, self.path()).await)
            .await
    }

    async fn schema(&self, engine: Arc<Engine>) -> DFResult<SchemaRef> {
        self.schema_cell()
            .get_or_try_init(async || {
                let schema_path = self.path().clone().join(DATASET_DIR).join(SCHEMA_FILE);
                let bytes = engine.read_bytes(&schema_path).await.map_err(|e| {
                    e.context(format!(
                        "Cannot read HATS catalog schema file '{schema_path}'"
                    ))
                })?;
                // Footer-only file, so an async reader wouldn't save any I/O
                parquet_schema_from_bytes(bytes).map_err(|e| {
                    e.context(format!(
                        "HATS catalog schema file '{schema_path}' is not a valid parquet file"
                    ))
                })
            })
            .await
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hats::test_utils::{engine, test_data_path};

    fn field_names(schema: &SchemaRef) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    #[tokio::test]
    async fn discover_catalog() {
        let engine = engine();
        let hats = Hats::from_path(test_data_path("small_sky"));
        let inner = hats.get_inner(engine.clone()).await.unwrap();
        assert!(inner.collection.is_none());
        assert_eq!(inner.catalog.path, test_data_path("small_sky"));

        let schema = hats.schema(engine).await.unwrap();
        let names = field_names(&schema);
        for column in ["id", "ra", "dec", "_healpix_29"] {
            assert!(names.contains(&column), "{column} not in {names:?}");
        }
    }

    #[tokio::test]
    async fn discover_collection() {
        let engine = engine();
        let hats = Hats::from_path(test_data_path("small_sky_o1_collection"));
        let inner = hats.get_inner(engine.clone()).await.unwrap();
        let Some(collection) = &inner.collection else {
            panic!("small_sky_o1_collection should be discovered as a collection");
        };
        assert_eq!(
            collection
                .indexes(engine.clone())
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            ["id"]
        );
        assert_eq!(
            inner.catalog.path,
            test_data_path("small_sky_o1_collection/small_sky_order1")
        );

        let schema = hats.schema(engine.clone()).await.unwrap();
        assert_eq!(schema, inner.catalog.schema(engine).await.unwrap());
    }

    #[tokio::test]
    async fn discover_neither() {
        let hats = Hats::from_path(test_data_path("does_not_exist"));
        let Err(DataFusionError::Shared(err)) = hats.get_inner(engine()).await else {
            panic!("expected an error");
        };
        assert!(matches!(err.as_ref(), DataFusionError::Configuration(_)));
    }

    #[tokio::test]
    async fn index_catalog_schema() {
        let engine = engine();
        let path = test_data_path("small_sky_o1_collection/small_sky_order1_id_index");
        let catalog = IndexCatalog::try_from_dir(engine.clone(), &path)
            .await
            .unwrap();
        let properties = catalog.properties(engine.clone()).await.unwrap();
        assert_eq!(properties.hats_index_column, "id");

        let schema = catalog.schema(engine).await.unwrap();
        let names = field_names(&schema);
        assert!(names.contains(&"id"), "id not in {names:?}");
    }

    #[tokio::test]
    async fn index_catalog_schema_missing_file() {
        let catalog = IndexCatalog::new(test_data_path("does_not_exist"), None);
        let err = catalog.schema(engine()).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot read HATS catalog schema file"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn schema_missing_file() {
        let catalog = HatsCatalog::new(test_data_path("does_not_exist"), None);
        let err = catalog.schema(engine()).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot read HATS catalog schema file"),
            "{err}"
        );
    }
}
