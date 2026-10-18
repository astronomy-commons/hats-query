use crate::adql::register_adql;
use crate::exceptions::datafusion_error_to_py_exception;
use arrow_pyarrow::PyArrowType;
use datafusion::common::arrow::array::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::common::spawn_buffered;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use futures_util::StreamExt as _;
use futures_util::future;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

/// Reads parquet and returns arrow table.
#[pyfunction]
pub(crate) fn read_parquet(py: Python, path_in: String) -> PyResult<Bound<PyAny>> {
    future_into_py(py, async {
        // let path = Path::new(path_in);

        let ctx = SessionContext::new();

        // create the dataframe
        let df = ctx
            .read_parquet(path_in, ParquetReadOptions::new())
            .await
            .map_err(datafusion_error_to_py_exception)?;

        // execute the plan
        let results: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(datafusion_error_to_py_exception)?;

        Ok(PyArrowType(results))
    })
}

type SyncableRecordBatchStream = Mutex<SendableRecordBatchStream>;

/// Number of partitions executed concurrently ahead of the consumer.
const PARTITION_WINDOW: usize = 8;

/// Number of record batches each running partition may buffer ahead of the consumer.
const PARTITION_BUFFER: usize = 2;

/// Async iterator over a parquet file, yielding arrow record batches.
///
/// Parameters
/// ----------
/// path : pathlib.Path | str
///     The path to the parquet file.
/// sql : str | None, optional
///     The SQL query to execute, must be provided if table_name is provided.
/// table_name : str | None, optional
///     The name of the table to query, must be provided if sql is provided.
#[pyclass]
pub(crate) struct ParquetReader(Arc<InnerParquetReader>);

struct InnerParquetReader {
    args: ParquetReaderArgs,
    stream: OnceCell<SyncableRecordBatchStream>,
}

struct ParquetReaderArgs {
    path: PathBuf,
    query: Option<QueryArgs>,
}

struct QueryArgs {
    sql: String,
    table_name: String,
}

impl InnerParquetReader {
    fn new(path: PathBuf, query: Option<QueryArgs>) -> Self {
        Self {
            args: ParquetReaderArgs { path, query },
            stream: OnceCell::new(),
        }
    }

    async fn get_or_try_init_stream(&self) -> PyResult<&SyncableRecordBatchStream> {
        self.stream
            .get_or_try_init(|| async {
                let stream = Self::initialize_stream(&self.args).await?;
                Ok::<_, PyErr>(Mutex::new(stream))
            })
            .await
    }

    async fn initialize_stream(args: &ParquetReaderArgs) -> PyResult<SendableRecordBatchStream> {
        // Disable work stealing, so each partition reads only its own contiguous file ranges
        // and concatenating partitions in order preserves the file order.
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .execution
            .enable_file_stream_work_stealing = false;
        let ctx = SessionContext::new_with_config(config);
        register_adql(&ctx);

        let string_path = args.path.to_str().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("Invalid path: cannot convert to string")
        })?;
        let sql = match &args.query {
            Some(query) => query.sql.as_str(),
            None => "SELECT * FROM t",
        };
        let table_name = match &args.query {
            Some(query) => query.table_name.as_str(),
            None => "t",
        };

        ctx.register_parquet(table_name, string_path, ParquetReadOptions::new())
            .await
            .map_err(datafusion_error_to_py_exception)?;
        let df = ctx
            .sql(sql)
            .await
            .map_err(datafusion_error_to_py_exception)?;

        let schema = df.schema().inner().clone();

        // execute the plan and get the partitioned streams
        let partition_streams = df
            .execute_stream_partitioned()
            .await
            .map_err(datafusion_error_to_py_exception)?;

        // Process PARTITION_BUFFER partitions in parallel and pre-fetch up to PARTITION_WINDOW
        // partitions ahead of the consumer.
        let flatten_ordered_stream = futures_util::stream::iter(partition_streams)
            .map(|s| future::ready(spawn_buffered(s, PARTITION_BUFFER)))
            .buffered(PARTITION_WINDOW)
            .flatten();
        let ordered_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            flatten_ordered_stream,
        ));
        Ok(ordered_stream)
    }
}

#[pymethods]
impl ParquetReader {
    #[new]
    #[pyo3(signature = (path: "pathlib.Path | str", *, sql: "str | None" = None, table_name: "str | None" = None))]
    fn new(path: PathBuf, sql: Option<String>, table_name: Option<String>) -> PyResult<Self> {
        let query = match (sql, table_name) {
            (Some(sql), Some(table_name)) => Some(QueryArgs { sql, table_name }),
            (None, None) => None,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Both sql and table_name must be provided together or neither.",
                ));
            }
        };
        let inner = InnerParquetReader::new(path, query);
        Ok(Self(Arc::new(inner)))
    }

    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.0);

        future_into_py(py, async move {
            let stream = inner.get_or_try_init_stream().await?;
            match stream.lock().await.next().await {
                Some(batch) => Ok(PyArrowType(
                    batch.map_err(datafusion_error_to_py_exception)?,
                )),
                None => Err(PyErr::new::<pyo3::exceptions::PyStopAsyncIteration, _>(
                    "End of iteration",
                )),
            }
        })
    }
}
