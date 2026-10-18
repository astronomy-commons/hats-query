mod adql;
mod exceptions;
mod parquet;
mod sums;

use crate::parquet::ParquetReader;
use crate::parquet::read_parquet;
use crate::sums::sum_as_string;
use pyo3::prelude::*;

#[pymodule(gil_used = false)]
fn _extension(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<ParquetReader>()?;
    Ok(())
}
