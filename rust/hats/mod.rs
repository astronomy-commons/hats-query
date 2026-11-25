mod catalog;
mod properties;

pub(crate) use catalog::Hats;

#[cfg(test)]
mod test_utils {
    use crate::engine::Engine;
    use crate::store::mount_tree::MountConfig;
    use datafusion::object_store::path::Path;
    use std::sync::Arc;

    const HATS_TEST_DATA: &str = "tests/data/git-submodules/hats/tests/data";

    /// An engine with the crate directory mounted at "/".
    pub(super) fn engine() -> Arc<Engine> {
        let mount = MountConfig::new("/", env!("CARGO_MANIFEST_DIR"));
        Arc::new(Engine::new(vec![mount], vec![]).unwrap())
    }

    pub(super) fn test_data_path(name: &str) -> Path {
        Path::from(format!("{HATS_TEST_DATA}/{name}"))
    }
}
