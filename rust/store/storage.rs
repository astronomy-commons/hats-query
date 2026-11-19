use crate::store::location::Location;
use crate::store::mount_tree::MountTree;
use crate::store::store_path::StorePath;
use datafusion::common::Result as DFResult;
use datafusion::prelude::SessionContext;

/// The storage system, which resolves locations and registers stores with DataFusion sessions.
pub(crate) struct Storage {
    mounts: MountTree,
    // User-provided stores, e.g.
    // external: Cache<something>,
}

impl Storage {
    pub(crate) fn new(mounts: MountTree) -> Self {
        Self { mounts }
    }

    /// Registers the stores known upfront, i.e. the mounts, in the session.
    pub(crate) fn register_object_stores(&self, ctx: &SessionContext) {
        self.mounts.register_object_stores(ctx);
    }

    /// Finds the store containing `location`, returns the store and the path inside it.
    pub(crate) fn resolve(&self, location: &Location) -> DFResult<StorePath> {
        match location {
            Location::Mounted(path) => self.mounts.resolve(path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::mount_tree::MountConfig;
    use datafusion::object_store::path::Path;

    /// A storage with the crate directory mounted at "/data".
    fn storage() -> Storage {
        let mount = MountConfig::new("/data", env!("CARGO_MANIFEST_DIR"));
        Storage::new(MountTree::from_mount_configs(vec![mount]).unwrap())
    }

    #[test]
    fn resolve_mounted() {
        let store_path = storage()
            .resolve(&Location::Mounted(Path::from("data/x/y")))
            .unwrap();
        assert_eq!(store_path.url().as_str(), "mount://0.data/x/y");
    }

    #[test]
    fn resolve_mounted_outside_of_mounts() {
        assert!(
            storage()
                .resolve(&Location::Mounted(Path::from("other")))
                .is_err()
        );
    }
}
