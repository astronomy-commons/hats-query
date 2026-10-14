use pyo3::prelude::*;

use arrow_pyarrow::PyArrowType;
use datafusion::{
    common::arrow::array::RecordBatch, datasource::file_format::options::ParquetReadOptions,
    execution::context::SessionContext,
};
use std::{fs::File, path::Path};

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
            .unwrap();

        // execute the plan
        let results: Vec<RecordBatch> = df.collect().await.unwrap();

        Ok(PyArrowType(results))
    })
}
