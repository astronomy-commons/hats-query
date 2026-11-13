use crate::store::StorePath;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::path::Path;
use opendal::services;

/// The repository root, which holds the "test-data" directory.
pub(crate) const WORKSPACE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/..");

const HATS_TEST_DATA: &str = "test-data/git-submodules/hats/tests/data";

pub(crate) fn test_data_path(name: &str) -> Path {
    Path::from(format!("{HATS_TEST_DATA}/{name}"))
}

/// The location of a test data directory, in a store rooted at the repository root.
pub(crate) fn test_data_location(name: &str) -> StorePath {
    let operator = opendal::Operator::new(services::Fs::default().root(WORKSPACE_ROOT)).unwrap();
    let store_url = ObjectStoreUrl::parse("mount://workspace").unwrap();
    StorePath::new(operator, store_url, test_data_path(name))
}
