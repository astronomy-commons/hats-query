mod exceptions;
mod parquet;
mod sums;

use crate::parquet::ParquetReader;
use parquet::read_parquet;
use pyo3::prelude::*;
use sums::sum_as_string;

#[pymodule(gil_used = false)]
fn _extension(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<ParquetReader>()?;
    Ok(())
}
