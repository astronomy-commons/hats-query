//! Conversion of Arrow data to VOTable.

mod data_type;
mod error;
mod field;

pub(crate) use error::VoConversionError;
pub(crate) use field::ArrowFieldVoExt;
