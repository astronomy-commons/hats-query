use crate::adql::{AdqlContext as _, register_adql};
use crate::exceptions::datafusion_error_to_py_exception;
use crate::output::{PyAsyncRecordBatchIterator, RecordBatchIterator, SyncableRecordBatchStream};
use crate::store::location::Location;
use crate::store::mount_tree::{MountConfig, MountTree};
use crate::store::storage::Storage;
use crate::tap::{KnownTables, TapTableConfig};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::Result as DFResult;
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
    storage: Storage,
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
        let storage = Storage::new(MountTree::from_mount_configs(mount_configs)?);
        let tables = KnownTables::from_tap_table_configs(&storage, tap_table_configs)?;
        Ok(Self {
            storage,
            tables,
            ctx: OnceCell::new(),
        })
    }

    pub(crate) async fn ctx(self: &Arc<Self>) -> DFResult<&SessionContext> {
        self.ctx
            .get_or_try_init(async || {
                let ctx = SessionContext::new();
                register_adql(&ctx);
                self.storage.register_object_stores(&ctx);
                self.tables.register_tap_schema_tables(&ctx).await?;
                Ok(ctx)
            })
            .await
    }

    /// Reads the whole object at `path`, a server path inside the mounts.
    pub(crate) async fn read_bytes(&self, path: &Path) -> DFResult<Bytes> {
        self.storage
            .resolve(&Location::Mounted(path.clone()))?
            .read_bytes()
            .await
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
