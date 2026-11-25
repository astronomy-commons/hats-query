use crate::exceptions::datafusion_error_to_py_exception;
use crate::output::{PyAsyncRecordBatchIterator, RecordBatchIterator, SyncableRecordBatchStream};
use arrow_pyarrow::{PyArrowType, Table};
use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::physical_plan::common::collect;
use hats_query_core::Engine;
use hats_query_core::arrow_votable::write_votable;
use hats_query_core::configuration::{MountConfig, TapTableConfig};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};
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

    /// Runs an ADQL query, returns all its results at once.
    ///
    /// The GIL is released while the query runs.
    ///
    /// Parameters
    /// ----------
    /// adql : str
    ///     The ADQL query to run
    /// format : str, default "arrow"
    ///     Format of the results, "arrow" or "votable"
    ///
    /// Returns
    /// -------
    /// pyarrow.Table or bytes
    ///     The results as a pyarrow table for "arrow", or as a VOTable XML document for
    ///     "votable", with a single BINARY2 table and strings stored as UTF-8 ``char`` arrays
    #[pyo3(signature = (adql, format = "arrow"))]
    fn query<'py>(&self, py: Python<'py>, adql: &str, format: &str) -> PyResult<Bound<'py, PyAny>> {
        let engine = &self.inner;
        // Schema and record batches of the results
        let results = || {
            get_runtime().block_on(async {
                let stream = engine.query(adql).await?;
                let schema = stream.schema();
                Ok::<_, DataFusionError>((schema, collect(stream).await?))
            })
        };
        match format {
            "arrow" => {
                let table = py
                    .detach(|| -> DFResult<_> {
                        let (schema, batches) = results()?;
                        Ok(Table::try_new(batches, schema)?)
                    })
                    .map_err(datafusion_error_to_py_exception)?;
                PyArrowType(table).into_pyobject(py)
            }
            "votable" => {
                let document = py
                    .detach(|| -> DFResult<_> {
                        let (schema, batches) = results()?;
                        let mut document = Vec::new();
                        write_votable(&schema, &batches, &mut document)?;
                        Ok(document)
                    })
                    .map_err(datafusion_error_to_py_exception)?;
                Ok(PyBytes::new(py, &document).into_any())
            }
            _ => Err(PyValueError::new_err(format!(
                "Unknown format '{format}', expected 'arrow' or 'votable'"
            ))),
        }
    }

    /// Runs an ADQL query, returns its results as an async iterator.
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
    fn async_query(&self, adql: String) -> PyAsyncRecordBatchIterator {
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
