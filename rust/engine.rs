use crate::exceptions::datafusion_error_to_py_exception;
use crate::store::mount_tree::{MountConfig, MountTree};
use crate::tap::{KnownTables, TapTableConfig};
use bytes::Bytes;
use datafusion::common::Result as DFResult;
use datafusion::object_store::ObjectStoreExt;
use datafusion::object_store::path::Path;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(name = "Engine")]
pub(crate) struct PyEngine {
    inner: Arc<Engine>,
}

pub(crate) struct Engine {
    mounts: MountTree,
    tables: KnownTables,
}

/// Builds an engine with the given mount configurations.
///
/// Parameters
/// ----------
/// mount_configs : list of MountConfig
///     The mount configurations to use
/// tap_table_configs : list of TapTableConfig
///     The TAP table configurations to use
///
/// Returns
/// -------
/// Engine
#[pyfunction]
pub(crate) fn build_engine(
    mount_configs: Vec<MountConfig>,
    tap_table_configs: Vec<TapTableConfig>,
) -> PyResult<PyEngine> {
    let engine =
        Engine::new(mount_configs, tap_table_configs).map_err(datafusion_error_to_py_exception)?;
    Ok(PyEngine {
        inner: Arc::new(engine),
    })
}

impl Engine {
    pub(crate) fn new(
        mount_configs: Vec<MountConfig>,
        tap_table_configs: Vec<TapTableConfig>,
    ) -> DFResult<Self> {
        let mounts = MountTree::from_mount_configs(mount_configs)?;
        let tables = KnownTables::from_tap_table_configs(tap_table_configs);
        Ok(Self { mounts, tables })
    }

    pub(crate) async fn read_bytes(&self, path: &Path) -> DFResult<Bytes> {
        let (store, path) = self.mounts.resolve(path)?;
        Ok(store.get(&path).await?.bytes().await?)
    }
}

#[pymethods]
impl PyEngine {
    /// Reads bytes from the given path inside the mounted paths.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     The path to read bytes from
    ///
    /// Returns
    /// -------
    /// bytes
    ///     The bytes read from the path
    fn read_bytes<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.inner.clone();
        future_into_py(py, async move {
            engine
                .read_bytes(&path.into())
                .await
                .map_err(datafusion_error_to_py_exception)
        })
    }

    // fn query<'py>(&self, py: Python<'py>, adql: String) -> PyResult<Bound<'py, PyAny>> {
    //     let engine = self.inner.clone();
    //     future_into_py(py, async move {
    //         engine.query(adql).await.map_err(datafusion_error_to_py_exception)
    //     })
    // }
}
