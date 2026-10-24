use crate::exceptions::datafusion_error_to_py_exception;
use arrow_pyarrow::PyArrowType;
use async_trait::async_trait;
use datafusion::common::Result as DFResult;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::StreamExt;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) type SyncableRecordBatchStream = Mutex<SendableRecordBatchStream>;

#[async_trait]
pub(crate) trait RecordBatchIterator: Send + Sync {
    /// Returns a reference to the underlying stream. Should initialize the stream if it doesn't
    /// exist yet.
    async fn get_stream(&self) -> DFResult<&SyncableRecordBatchStream>;
}

#[pyclass(name = "AsyncRecordBatchIterator")]
pub(crate) struct PyAsyncRecordBatchIterator {
    pub(crate) inner: Arc<dyn RecordBatchIterator>,
}

#[pymethods]
impl PyAsyncRecordBatchIterator {
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        future_into_py(py, async move {
            let stream = inner
                .get_stream()
                .await
                .map_err(datafusion_error_to_py_exception)?;
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
