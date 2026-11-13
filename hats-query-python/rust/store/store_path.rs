use bytes::Bytes;
use datafusion::common::Result as DFResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::path::{Path, PathPart};
use datafusion::object_store::{ObjectStore, ObjectStoreExt};
use object_store_opendal::OpendalStore;
use std::sync::Arc;
use url::Url;

/// A location inside an object store: the store's OpenDAL operator, the URL the store is
/// registered under in a session, and the path inside the store.
#[derive(Clone, Debug)]
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

    /// The location of `child` inside this location, in the same store.
    ///
    /// `child` is a single path segment, see `Path::join`.
    pub(crate) fn join<'a>(&self, child: impl Into<PathPart<'a>>) -> Self {
        Self {
            opendal_operator: self.opendal_operator.clone(),
            store_url: self.store_url.clone(),
            path: self.path.clone().join(child),
        }
    }

    /// The store as an object store.
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::new(OpendalStore::new(self.opendal_operator.clone()))
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::services;

    #[test]
    fn join() {
        let operator =
            opendal::Operator::new(services::Fs::default().root(env!("CARGO_MANIFEST_DIR")))
                .unwrap();
        let store_url = ObjectStoreUrl::parse("mount://test").unwrap();
        let parent = StorePath::new(operator, store_url.clone(), Path::from("a/b"));
        let child = parent.join("c");
        assert_eq!(child.store_url(), &store_url);
        assert_eq!(child.url().as_str(), "mount://test/a/b/c");
    }
}
