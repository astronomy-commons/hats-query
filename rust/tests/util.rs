use crate::engine::Engine;
use crate::store::location::Location;
use crate::store::mount_tree::{MountConfig, MountTree};
use crate::store::storage::Storage;
use crate::store::store_path::StorePath;
use datafusion::object_store::path::Path;
use std::sync::Arc;

const HATS_TEST_DATA: &str = "tests/data/git-submodules/hats/tests/data";

/// The mount of the crate directory at "/".
fn crate_mount() -> MountConfig {
    MountConfig::new("/", env!("CARGO_MANIFEST_DIR"))
}

/// An engine with the crate directory mounted at "/".
pub(crate) fn engine() -> Arc<Engine> {
    Arc::new(Engine::new(vec![crate_mount()], vec![]).unwrap())
}

/// A storage with the crate directory mounted at "/".
pub(crate) fn storage() -> Storage {
    Storage::new(MountTree::from_mount_configs(vec![crate_mount()]).unwrap())
}

pub(crate) fn test_data_path(name: &str) -> Path {
    Path::from(format!("{HATS_TEST_DATA}/{name}"))
}

/// The location of a test data directory, resolved through `storage()`.
pub(crate) fn test_data_location(name: &str) -> StorePath {
    storage()
        .resolve(&Location::Mounted(test_data_path(name)))
        .unwrap()
}
