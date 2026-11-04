use crate::adql::{AdqlContext as _, register_adql};
use crate::exceptions::datafusion_error_to_py_exception;
use crate::output::{PyAsyncRecordBatchIterator, RecordBatchIterator, SyncableRecordBatchStream};
use crate::store::mount_tree::{MountConfig, MountTree};
use crate::tap::{KnownTables, TapTableConfig};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::Result as DFResult;
use datafusion::object_store::ObjectStoreExt;
use datafusion::object_store::path::Path;
use datafusion::prelude::SessionContext;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

#[pyclass(name = "Engine")]
pub(crate) struct PyEngine {
    inner: Arc<Engine>,
}

pub(crate) struct Engine {
    mounts: MountTree,
    tables: KnownTables,
    /// DataFusion session with ADQL functions and TAP tables, lazily initialized.
    ctx: OnceCell<SessionContext>,
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
        Ok(Self {
            mounts,
            tables,
            ctx: OnceCell::new(),
        })
    }

    async fn ctx(self: &Arc<Self>) -> DFResult<&SessionContext> {
        self.ctx
            .get_or_try_init(async || {
                let ctx = SessionContext::new();
                register_adql(&ctx);
                self.tables
                    .register_tap_schema_tables(self.clone(), &ctx)
                    .await?;
                Ok(ctx)
            })
            .await
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

    /// Runs an ADQL query.
    ///
    /// The query is planned and executed lazily, on the first iteration.
    ///
    /// Parameters
    /// ----------
    /// adql : str
    ///     The ADQL query to run
    ///
    /// Returns
    /// -------
    /// AsyncRecordBatchIterator
    ///     Async iterator over the resulting arrow record batches
    fn query(&self, adql: String) -> PyAsyncRecordBatchIterator {
        PyAsyncRecordBatchIterator {
            inner: Arc::new(QueryRecordBatchIterator {
                engine: self.inner.clone(),
                adql,
                stream: OnceCell::new(),
            }),
        }
    }
}

/// State of a single ADQL query execution.
struct QueryRecordBatchIterator {
    engine: Arc<Engine>,
    adql: String,
    stream: OnceCell<SyncableRecordBatchStream>,
}

#[async_trait]
impl RecordBatchIterator for QueryRecordBatchIterator {
    async fn get_stream(&self) -> DFResult<&SyncableRecordBatchStream> {
        self.stream
            .get_or_try_init(async || {
                let ctx = self.engine.ctx().await?;
                let stream = ctx.adql(&self.adql).await?.execute_stream().await?;
                Ok(Mutex::new(stream))
            })
            .await
    }
}
