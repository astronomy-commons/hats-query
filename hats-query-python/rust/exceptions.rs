use datafusion::error::DataFusionError;
use pyo3::PyErr;
use pyo3::exceptions::{
    PyFileNotFoundError, PyMemoryError, PyNotImplementedError, PyOSError, PyRuntimeError,
    PyValueError,
};

/// Converts DataFusion error to the closest Python exception, keeping the full message.
pub(crate) fn datafusion_error_to_py_exception(error: DataFusionError) -> PyErr {
    let message = error.to_string();
    exception_for(&error, message)
}

fn exception_for(error: &DataFusionError, message: String) -> PyErr {
    match error {
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            exception_for(inner, message)
        }
        DataFusionError::Shared(inner) => exception_for(inner, message),
        DataFusionError::IoError(e) if e.kind() == std::io::ErrorKind::NotFound => {
            PyFileNotFoundError::new_err(message)
        }
        DataFusionError::IoError(_) => PyOSError::new_err(message),
        DataFusionError::ObjectStore(e) => match e.as_ref() {
            datafusion::object_store::Error::NotFound { .. } => {
                PyFileNotFoundError::new_err(message)
            }
            _ => PyOSError::new_err(message),
        },
        DataFusionError::ParquetError(_)
        | DataFusionError::ArrowError(_, _)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::Configuration(_) => PyValueError::new_err(message),
        DataFusionError::NotImplemented(_) => PyNotImplementedError::new_err(message),
        DataFusionError::ResourcesExhausted(_) => PyMemoryError::new_err(message),
        _ => PyRuntimeError::new_err(message),
    }
}
