use crate::exceptions::datafusion_error_to_py_exception;
use arrow_pyarrow::PyArrowType;
use datafusion::common::arrow::array::RecordBatch;
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures_util::StreamExt as _;
use futures_util::stream;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py, get_runtime};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

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

type SyncableRecordBatchStream = Arc<Mutex<SendableRecordBatchStream>>;

#[pyclass]
pub(crate) struct ParquetReader {
    unordered_stream: SyncableRecordBatchStream,
}

impl ParquetReader {
    async fn execute_stream_partitioned(path: PathBuf) -> PyResult<Self> {
        let ctx = SessionContext::new();

        let string_path = path.to_str().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err("Invalid path: cannot convert to string")
        })?;

        // create the dataframe
        let df = ctx
            .read_parquet(string_path, ParquetReadOptions::new())
            .await
            .map_err(datafusion_error_to_py_exception)?;

        let schema = df.schema().inner().clone();

        // execute the plan and get the partitioned streams
        let partition_streams = df
            .execute_stream_partitioned()
            .await
            .map_err(datafusion_error_to_py_exception)?;

        // select_all doesn't preserve the order but may work faster
        let flatten_unordered_stream = stream::select_all(partition_streams);
        let unordered_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            flatten_unordered_stream,
        ));
        let unordered_stream = Arc::new(Mutex::new(unordered_stream));

        Ok(Self { unordered_stream })
    }
}

#[pymethods]
impl ParquetReader {
    // TODO: Consider to initialize lazily, when the first iteration is being called
    #[new]
    fn new(path: PathBuf) -> PyResult<Self> {
        let runtime = get_runtime();
        runtime.block_on(async { Self::execute_stream_partitioned(path).await })
    }

    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = Arc::clone(&self.unordered_stream);

        future_into_py(py, async move {
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
