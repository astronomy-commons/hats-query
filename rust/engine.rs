use crate::exceptions::datafusion_error_to_py_exception;
use crate::store::mount_tree::{MountConfig, MountTree};
use datafusion::object_store::ObjectStoreExt;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

/// A query engine reading from the virtual file system.
#[pyclass]
pub(crate) struct Engine {
    /// The virtual file system.
    mounts: MountTree,
}

/// Builds an engine over the mount points.
///
/// Returns an error if two mount points share a path or a source is unsupported.
#[pyfunction]
pub(crate) fn build_engine(mount_configs: Vec<MountConfig>) -> PyResult<Engine> {
    let mounts =
        MountTree::from_mount_configs(mount_configs).map_err(datafusion_error_to_py_exception)?;
    Ok(Engine { mounts })
}

#[pymethods]
impl Engine {
    /// Reads the whole file at a virtual `path`, awaitable.
    fn read_bytes<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
        let (store, path) = self
            .mounts
            .resolve(path)
            .map_err(datafusion_error_to_py_exception)?;
        future_into_py(py, async move {
            let get = async { store.get(&path).await?.bytes().await };
            get.await
                .map_err(|err| datafusion_error_to_py_exception(err.into()))
        })
    }
}
