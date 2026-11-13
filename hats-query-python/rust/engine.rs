use crate::exceptions::datafusion_error_to_py_exception;
use crate::output::{PyAsyncRecordBatchIterator, RecordBatchIterator, SyncableRecordBatchStream};
use async_trait::async_trait;
use datafusion::common::Result as DFResult;
use hats_query_core::Engine;
use hats_query_core::configuration::{MountConfig, TapTableConfig};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

#[pyclass(name = "Engine")]
pub(crate) struct PyEngine {
    inner: Arc<Engine>,
}

#[pymethods]
impl PyEngine {
    /// Query engine serving HATS catalogs from mounted directories.
    ///
    /// Parameters
    /// ----------
    /// mounts : dict[str, str]
    ///     Server paths, e.g. "/hats", mapped to the directories served under them. A relative
    ///     local directory is resolved against the current working directory.
    /// tables : dict[str, str]
    ///     Table names, e.g. "schema.table", mapped to the server paths of their HATS catalogs,
    ///     e.g. "/hats/catalog".
    #[new]
    fn new(mounts: &Bound<PyDict>, tables: &Bound<PyDict>) -> PyResult<Self> {
        let base_dir = std::env::current_dir()?;
        let mounts = mounts
            .iter()
            .map(|(path, source)| {
                let path: String = path.extract()?;
                MountConfig::new(&path, source.extract()?, &base_dir)
                    .map_err(|err| PyValueError::new_err(format!("mount {path}: {err}")))
            })
            .collect::<PyResult<_>>()?;
        let tables = tables
            .iter()
            .map(|(name, path)| {
                let name: String = name.extract()?;
                TapTableConfig::new(name.clone(), &path.extract::<String>()?)
                    .map_err(|err| PyValueError::new_err(format!("table {name}: {err}")))
            })
            .collect::<PyResult<_>>()?;
        let engine = Engine::new(mounts, tables).map_err(datafusion_error_to_py_exception)?;
        Ok(Self {
            inner: Arc::new(engine),
        })
    }

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
            .get_or_try_init(async || Ok(Mutex::new(self.engine.query(&self.adql).await?)))
            .await
    }
}
