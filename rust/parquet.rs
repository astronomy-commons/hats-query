use crate::exceptions::datafusion_error_to_py_exception;

use arrow_pyarrow::PyArrowType;
use datafusion::{
    common::arrow::array::RecordBatch, datasource::file_format::options::ParquetReadOptions,
    execution::context::SessionContext,
};
use pyo3::prelude::*;

/// Reads parquet and returns arrow table.
#[pyfunction]
pub(crate) fn read_parquet(py: Python, path_in: String) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async {
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
