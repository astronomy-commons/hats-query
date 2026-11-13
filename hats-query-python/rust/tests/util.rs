use crate::store::mount_tree::{MountConfig, MountTree};
use crate::store::storage::Storage;
use datafusion::object_store::path::Path;
use hats_query_core::store::{Location, StorePath};

/// The repository root, which holds the "test-data" directory.
const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

const HATS_TEST_DATA: &str = "test-data/git-submodules/hats/tests/data";

/// The mount of the repository root at "/".
fn workspace_mount() -> MountConfig {
    MountConfig::new("/", WORKSPACE_ROOT)
}

/// A storage with the repository root mounted at "/".
pub(crate) fn storage() -> Storage {
    Storage::new(MountTree::from_mount_configs(vec![workspace_mount()]).unwrap())
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
