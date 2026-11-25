//! Conversion of Arrow data to VOTable.

mod array;
mod conversion;
mod data_type;
mod error;
mod field;
mod record_batch;

pub use conversion::write_votable;
pub(crate) use error::VoConversionError;
pub(crate) use field::ArrowFieldVoExt;
