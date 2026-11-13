mod adql;
mod arrow_votable;
mod common;
mod engine;
mod exceptions;
mod hats;
mod output;
mod parquet;
mod store;
mod sums;
mod table_metadata;
mod tap;
#[cfg(test)]
mod tests;

use crate::engine::{PyEngine, build_engine};
use crate::parquet::ParquetReader;
use crate::parquet::read_parquet;
use crate::sums::sum_as_string;
use pyo3::prelude::*;

#[pymodule(gil_used = false)]
fn _extension(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(build_engine, m)?)?;
    m.add_class::<PyEngine>()?;
    m.add_function(wrap_pyfunction!(read_parquet, m)?)?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<ParquetReader>()?;
    Ok(())
}
