use crate::common::{SingleCache, parquet_schema_from_bytes};
use crate::hats::properties::{CollectionProperties, HatsProperties, IndexProperties, Properties};
use crate::store::store_path::StorePath;
use crate::table_metadata::TableMetadata;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::OnceCell;

const DATASET_DIR: &str = "dataset";
const SCHEMA_FILE: &str = "_common_metadata";

/// Representation of a HATS collection or catalog.
pub(crate) struct Hats {
    /// Location specified by the user/config, it may point to a collection or a catalog.
    discovery_location: StorePath,
    /// Lazily initialized inner representation of the HATS collection or catalog.
    inner: SingleCache<Arc<HatsInner>>,
}

impl Hats {
    /// Creates a lazily initialized HATS catalog from a location, which should point to
    /// either a HATS collection (with "collection.properties" file) or a HATS catalog
    /// (with "hats.properties" file).
    /// We assume that collection.properties' hats_primary_table_url is a relative path,
    /// available in the same directory as collection.properties, so it is in the same store.
    /// In the future we may support discovery of catalog/collection by moving up the
    /// directory tree, but for now we require the user to specify the location explicitly.
    ///
    /// It is shared, so all its users benefit from the lazily loaded data.
    pub(crate) fn new(discovery_location: StorePath) -> Arc<Self> {
        Arc::new(Self {
            discovery_location,
            inner: SingleCache::new(),
        })
    }

    async fn get_inner(&self) -> DFResult<Arc<HatsInner>> {
        self.inner
            .try_get_with(async || {
                HatsInner::try_from_discovery_location(&self.discovery_location)
                    .await
                    .map(Arc::new)
            })
            .await
    }

    pub(crate) async fn metadata(&self) -> DFResult<Arc<TableMetadata>> {
        self.get_inner().await?.metadata().await
    }

    /// Location of the primary catalog directory.
    pub(crate) async fn catalog_location(&self) -> DFResult<StorePath> {
        Ok(self.get_inner().await?.catalog.location.clone())
    }

    #[allow(dead_code)] // Remove when used
    /// Location of the directory of the index catalog for `column`.
    ///
    /// Returns `None` if this is a catalog rather than a collection, or if the collection has no
    /// index catalog for `column`. Fails if the index catalog exists but cannot be loaded.
    pub(crate) async fn index_location(&self, column: &str) -> DFResult<Option<StorePath>> {
        let inner = self.get_inner().await?;
        let Some(collection) = &inner.collection else {
            return Ok(None);
        };
        Ok(collection
            .index_catalog(column)
            .await?
            .map(|index| index.location.clone()))
    }
}

struct HatsInner {
    /// Primary HATS catalog, lazily initialized.
    catalog: HatsCatalog,
    /// HATS collection.
    collection: Option<HatsCollection>,
    /// Metadata of the table, lazily initialized.
    metadata: OnceCell<Arc<TableMetadata>>,
}

impl HatsInner {
    async fn try_from_discovery_location(discovery_location: &StorePath) -> DFResult<Self> {
        let collection = HatsCollection::try_from_dir(discovery_location).await.ok();
        match collection {
            Some(collection) => {
                let catalog = collection.find_catalog().await?;
                Ok(Self {
                    catalog,
                    collection: Some(collection),
                    metadata: OnceCell::new(),
                })
            }
            None => match HatsCatalog::try_from_dir(discovery_location).await {
                Ok(catalog) => Ok(Self {
                    catalog,
                    collection: None,
                    metadata: OnceCell::new(),
                }),
                Err(err) => Err(DataFusionError::Configuration(format!(
                    "Path '{}' is neither a HATS collection nor a catalog: {err}",
                    discovery_location.url()
                ))),
            },
        }
    }

    /// Metadata of the primary catalog.
    ///
    /// Indexed columns are the HEALPix column and the columns of the collection's index catalogs.
    async fn metadata(&self) -> DFResult<Arc<TableMetadata>> {
        self.metadata
            .get_or_try_init(async || {
                let schema = self.catalog.schema().await?;
                let properties = self.catalog.properties().await?;
                let index_columns: BTreeSet<&str> = match &self.collection {
                    Some(collection) => collection.indexes().await?.collect(),
                    None => BTreeSet::new(),
                };
                let principal_columns: BTreeSet<&str> = match &properties.cols_default {
                    Some(columns) => columns.iter().map(String::as_str).collect(),
                    None => {
                        BTreeSet::from([properties.col_ra.as_str(), properties.col_dec.as_str()])
                    }
                };

                let mut metadata = TableMetadata::from_schema(&schema)?;
                for column in &mut metadata.columns {
                    let name = column.arrow.name().as_str();
                    column.indexed = name == properties.col_healpix || index_columns.contains(name);
                    column.principal = principal_columns.contains(name);
                    if name == properties.col_ra {
                        column.vo.unit = Some("deg".to_string());
                        column.vo.ucd = Some("pos.eq.ra;meta.main".to_string());
                    } else if name == properties.col_dec {
                        column.vo.unit = Some("deg".to_string());
                        column.vo.ucd = Some("pos.eq.dec;meta.main".to_string());
                    } else if name == properties.col_healpix {
                        column.vo.ucd = Some("pos.healpix".to_string());
                        column.vo.description = Some(
                            format!(
                                "Nested HEALPix index at order {}",
                                properties.col_healpix_order
                            )
                            .into(),
                        );
                    }
                }
                Ok(Arc::new(metadata))
            })
            .await
            .cloned()
    }
}

/// Representation of a HATS collection.
struct HatsCollection {
    /// Collection location, one with "collection.properties" file.
    location: StorePath,
    /// Lazily initialized inner representation of the collection, which requires properties file
    /// parsing.
    lazy_inner: OnceCell<LazilyInitializedHatsCollection>,
}

impl HatsCollection {
    fn new(location: StorePath, properties: Option<CollectionProperties>) -> Self {
        Self {
            location,
            lazy_inner: OnceCell::new_with(properties.map(LazilyInitializedHatsCollection::new)),
        }
    }

    async fn lazy_inner(&self) -> DFResult<&LazilyInitializedHatsCollection> {
        self.lazy_inner
            .get_or_try_init(async || {
                let collection_properties = CollectionProperties::from_dir(&self.location).await?;
                Ok(LazilyInitializedHatsCollection::new(collection_properties))
            })
            .await
    }

    async fn try_from_dir(location: &StorePath) -> DFResult<Self> {
        let collection_properties = CollectionProperties::from_dir(location).await?;
        Ok(Self::new(location.clone(), Some(collection_properties)))
    }

    async fn properties(&self) -> DFResult<&CollectionProperties> {
        Ok(&self.lazy_inner().await?.properties)
    }

    async fn indexes(&self) -> DFResult<impl Iterator<Item = &str>> {
        Ok(self
            .lazy_inner()
            .await?
            .index_catalogs
            .keys()
            .map(String::as_str))
    }

    #[allow(dead_code)] // Remove when used
    /// The index catalog for `column`, initialized on the first call.
    ///
    /// Returns `None` if the collection has no index catalog for `column`.
    async fn index_catalog(&self, column: &str) -> DFResult<Option<&IndexCatalog>> {
        let inner = self.lazy_inner().await?;
        let (Some(cell), Some(index_dir)) = (
            inner.index_catalogs.get(column),
            inner.properties.all_indexes.get(column),
        ) else {
            return Ok(None);
        };
        let index_location = self.location.join(index_dir.as_str());
        cell.get_or_try_init(async || IndexCatalog::try_from_dir(&index_location).await)
            .await
            .map(Some)
    }

    async fn find_catalog(&self) -> DFResult<HatsCatalog> {
        let properties = self.properties().await?;
        let catalog_location = self
            .location
            .join(properties.hats_primary_table_url.as_str());
        HatsCatalog::try_from_dir(&catalog_location).await
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
    /// Catalog location, one with "hats.properties" file.
    location: StorePath,
    /// Properties of the catalog, lazily initialized.
    properties: OnceCell<HatsProperties>,
    /// The schema of the catalog, lazily initialized.
    schema: OnceCell<SchemaRef>,
}

impl CatalogDir for HatsCatalog {
    type Properties = HatsProperties;

    fn new(location: StorePath, properties: Option<HatsProperties>) -> Self {
        Self {
            location,
            properties: OnceCell::new_with(properties),
            schema: OnceCell::new(),
        }
    }

    fn location(&self) -> &StorePath {
        &self.location
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
    /// Index catalog location, one with "hats.properties" file.
    location: StorePath,
    /// Properties of the index catalog, lazily initialized.
    properties: OnceCell<IndexProperties>,
    /// The schema of the index catalog, lazily initialized.
    schema: OnceCell<SchemaRef>,
}

impl CatalogDir for IndexCatalog {
    type Properties = IndexProperties;

    fn new(location: StorePath, properties: Option<IndexProperties>) -> Self {
        Self {
            location,
            properties: OnceCell::new_with(properties),
            schema: OnceCell::new(),
        }
    }

    fn location(&self) -> &StorePath {
        &self.location
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
    fn new(location: StorePath, properties: Option<Self::Properties>) -> Self;

    /// Location of the catalog directory.
    fn location(&self) -> &StorePath;

    fn properties_cell(&self) -> &OnceCell<Self::Properties>;

    fn schema_cell(&self) -> &OnceCell<SchemaRef>;

    async fn try_from_dir(location: &StorePath) -> DFResult<Self> {
        let properties = Self::Properties::from_dir(location).await?;
        Ok(Self::new(location.clone(), Some(properties)))
    }

    async fn properties(&self) -> DFResult<&Self::Properties> {
        self.properties_cell()
            .get_or_try_init(async || Self::Properties::from_dir(self.location()).await)
            .await
    }

    async fn schema(&self) -> DFResult<SchemaRef> {
        self.schema_cell()
            .get_or_try_init(async || {
                let schema_location = self.location().join(DATASET_DIR).join(SCHEMA_FILE);
                let schema_url = schema_location.url();
                let bytes = schema_location.read_bytes().await.map_err(|e| {
                    e.context(format!(
                        "Cannot read HATS catalog schema file '{schema_url}'"
                    ))
                })?;
                // Footer-only file, so an async reader wouldn't save any I/O
                parquet_schema_from_bytes(bytes).map_err(|e| {
                    e.context(format!(
                        "HATS catalog schema file '{schema_url}' is not a valid parquet file"
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
    use crate::engine::Engine;
    use crate::table_metadata::ColumnMetadata;
    use crate::tests::util::{engine, test_data_location, test_data_path};
    use datafusion::prelude::{ParquetReadOptions, SessionContext};

    fn field_names(schema: &SchemaRef) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    /// Names of the columns matching the predicate.
    fn column_names(
        metadata: &TableMetadata,
        predicate: impl Fn(&ColumnMetadata) -> bool,
    ) -> Vec<&str> {
        metadata
            .columns
            .iter()
            .filter(|column| predicate(column))
            .map(|column| column.arrow.name().as_str())
            .collect()
    }

    fn column<'a>(metadata: &'a TableMetadata, name: &str) -> &'a ColumnMetadata {
        metadata
            .columns
            .iter()
            .find(|column| column.arrow.name() == name)
            .unwrap()
    }

    #[tokio::test]
    async fn discover_catalog() {
        let hats = Hats::new(test_data_location("small_sky"));
        let inner = hats.get_inner().await.unwrap();
        assert!(inner.collection.is_none());
        assert_eq!(inner.catalog.location.path(), &test_data_path("small_sky"));

        let metadata = hats.metadata().await.unwrap();
        let names = column_names(&metadata, |_| true);
        for column in ["id", "ra", "dec", "_healpix_29"] {
            assert!(names.contains(&column), "{column} not in {names:?}");
        }
        assert_eq!(column_names(&metadata, |c| c.indexed), ["_healpix_29"]);
        // No hats_cols_default in the test catalog, so the coordinates are principal
        assert_eq!(column_names(&metadata, |c| c.principal), ["ra", "dec"]);

        let ra = &column(&metadata, "ra").vo;
        assert_eq!(
            (ra.unit.as_deref(), ra.ucd.as_deref()),
            (Some("deg"), Some("pos.eq.ra;meta.main"))
        );
        let dec = &column(&metadata, "dec").vo;
        assert_eq!(
            (dec.unit.as_deref(), dec.ucd.as_deref()),
            (Some("deg"), Some("pos.eq.dec;meta.main"))
        );
        let healpix = &column(&metadata, "_healpix_29").vo;
        assert_eq!(healpix.ucd.as_deref(), Some("pos.healpix"));
        assert_eq!(
            healpix.description.as_ref().map(ToString::to_string),
            Some("Nested HEALPix index at order 29".to_string())
        );
        let id = &column(&metadata, "id").vo;
        assert_eq!((id.unit.as_deref(), id.ucd.as_deref()), (None, None));
    }

    #[tokio::test]
    async fn discover_collection() {
        let hats = Hats::new(test_data_location("small_sky_o1_collection"));
        let inner = hats.get_inner().await.unwrap();
        let Some(collection) = &inner.collection else {
            panic!("small_sky_o1_collection should be discovered as a collection");
        };
        assert_eq!(
            collection.indexes().await.unwrap().collect::<Vec<_>>(),
            ["id"]
        );
        assert_eq!(
            inner.catalog.location.path(),
            &test_data_path("small_sky_o1_collection/small_sky_order1")
        );

        let metadata = hats.metadata().await.unwrap();
        let schema = inner.catalog.schema().await.unwrap();
        assert_eq!(column_names(&metadata, |_| true), field_names(&schema));
        assert_eq!(
            column_names(&metadata, |c| c.indexed),
            ["_healpix_29", "id"]
        );
    }

    #[tokio::test]
    async fn discover_neither() {
        let hats = Hats::new(test_data_location("does_not_exist"));
        let Err(DataFusionError::Shared(err)) = hats.get_inner().await else {
            panic!("expected an error");
        };
        assert!(matches!(err.as_ref(), DataFusionError::Configuration(_)));
    }

    #[tokio::test]
    async fn index_catalog_schema() {
        let location = test_data_location("small_sky_o1_collection/small_sky_order1_id_index");
        let catalog = IndexCatalog::try_from_dir(&location).await.unwrap();
        let properties = catalog.properties().await.unwrap();
        assert_eq!(properties.hats_index_column, "id");

        let schema = catalog.schema().await.unwrap();
        let names = field_names(&schema);
        assert!(names.contains(&"id"), "id not in {names:?}");
    }

    #[tokio::test]
    async fn index_catalog_schema_missing_file() {
        let catalog = IndexCatalog::new(test_data_location("does_not_exist"), None);
        let err = catalog.schema().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot read HATS catalog schema file"),
            "{err}"
        );
    }

    /// Number of rows in `file` inside `location`, read by URL through the engine's session.
    async fn count_rows_by_url(engine: &Arc<Engine>, location: &StorePath, file: &str) -> usize {
        let url = format!("{}/{file}", location.url());
        engine
            .ctx()
            .await
            .unwrap()
            .read_parquet(url, ParquetReadOptions::default())
            .await
            .unwrap()
            .count()
            .await
            .unwrap()
    }

    /// Number of rows in the test data file at `path`, read directly from the local filesystem.
    async fn count_rows_locally(path: &str) -> usize {
        let path = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), test_data_path(path));
        SessionContext::new()
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .unwrap()
            .count()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn catalog_location() {
        let engine = engine();
        let hats = Hats::new(test_data_location("small_sky_o1_collection"));
        let location = hats.catalog_location().await.unwrap();
        assert_eq!(
            location.path(),
            &test_data_path("small_sky_o1_collection/small_sky_order1")
        );

        let file = "dataset/Norder=1/Dir=0/Npix=44.parquet";
        assert_eq!(
            count_rows_by_url(&engine, &location, file).await,
            count_rows_locally(&format!("small_sky_o1_collection/small_sky_order1/{file}")).await
        );
    }

    #[tokio::test]
    async fn index_location() {
        let engine = engine();
        let hats = Hats::new(test_data_location("small_sky_o1_collection"));
        let location = hats.index_location("id").await.unwrap().unwrap();
        assert_eq!(
            location.path(),
            &test_data_path("small_sky_o1_collection/small_sky_order1_id_index")
        );

        let file = "dataset/index/part.0.parquet";
        assert_eq!(
            count_rows_by_url(&engine, &location, file).await,
            count_rows_locally(&format!(
                "small_sky_o1_collection/small_sky_order1_id_index/{file}"
            ))
            .await
        );
    }

    #[tokio::test]
    async fn index_location_unknown_column() {
        let hats = Hats::new(test_data_location("small_sky_o1_collection"));
        assert!(hats.index_location("ra").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn index_location_of_catalog() {
        let hats = Hats::new(test_data_location("small_sky"));
        assert!(hats.index_location("id").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn schema_missing_file() {
        let catalog = HatsCatalog::new(test_data_location("does_not_exist"), None);
        let err = catalog.schema().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot read HATS catalog schema file"),
            "{err}"
        );
    }
}
