use bytes::Bytes;
use datafusion::common::Result as DFResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectStore, ObjectStoreExt};
use object_store_opendal::OpendalStore;
use std::sync::Arc;
use url::Url;

/// A location inside an object store: the store's OpenDAL operator, the URL the store is
/// registered under in a session, and the path inside the store.
#[derive(Debug)]
pub(crate) struct StorePath {
    opendal_operator: opendal::Operator,
    store_url: ObjectStoreUrl,
    path: Path,
}

impl StorePath {
    pub(crate) fn new(
        opendal_operator: opendal::Operator,
        store_url: ObjectStoreUrl,
        path: Path,
    ) -> Self {
        Self {
            opendal_operator,
            store_url,
            path,
        }
    }

    /// The URL the store is registered under in a session.
    pub(crate) fn store_url(&self) -> &ObjectStoreUrl {
        &self.store_url
    }

    #[allow(dead_code)] // Remove when used
    /// The path inside the store.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The store as an object store.
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::new(OpendalStore::new(self.opendal_operator.clone()))
    }

    #[allow(dead_code)] // Remove when used
    /// The full URL of the location, readable by a session with the store registered.
    pub(crate) fn url(&self) -> Url {
        let mut url = AsRef::<Url>::as_ref(&self.store_url).clone();
        // The path is already percent-encoded, so it is kept as is
        url.set_path(&format!("/{}", self.path));
        url
    }

    /// Reads the whole object at the location.
    pub(crate) async fn read_bytes(&self) -> DFResult<Bytes> {
        Ok(self.object_store().get(&self.path).await?.bytes().await?)
    }
}
