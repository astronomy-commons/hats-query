use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use std::fmt;

/// Failure to represent an Arrow column in VOTable.
#[derive(Debug)]
pub(crate) struct VoConversionError {
    kind: VoConversionErrorKind,
}

/// Reason why an Arrow column can't be represented in VOTable.
#[derive(Debug)]
pub(crate) enum VoConversionErrorKind {
    /// Type without VOTable counterpart.
    UnsupportedType(DataType),
}

impl From<VoConversionErrorKind> for DataFusionError {
    fn from(kind: VoConversionErrorKind) -> Self {
        DataFusionError::External(Box::new(VoConversionError { kind }))
    }
}

impl fmt::Display for VoConversionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            VoConversionErrorKind::UnsupportedType(data_type) => {
                write!(f, "Arrow type {data_type} can't be represented in VOTable")
            }
        }
    }
}

impl std::error::Error for VoConversionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message() {
        let error = DataFusionError::from(VoConversionErrorKind::UnsupportedType(DataType::Null));
        assert_eq!(
            error.to_string(),
            "External error: Arrow type Null can't be represented in VOTable"
        );
    }
}
