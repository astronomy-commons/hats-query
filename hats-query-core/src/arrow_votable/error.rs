use crate::adql::delimit_if_needed;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use std::fmt;

/// Failure to represent an Arrow column in VOTable, telling how to change the query to fix it.
#[derive(Debug)]
pub(crate) struct VoConversionError {
    /// Name of the column, [None] until it is attached with [with_column].
    column: Option<String>,
    kind: VoConversionErrorKind,
}

/// Reason why an Arrow column can't be represented in VOTable.
#[derive(Debug)]
pub(crate) enum VoConversionErrorKind {
    /// Type without VOTable counterpart.
    UnsupportedType(DataType),
    /// Decimal type with too many digits for VOTable float types to hold all its values exactly.
    DecimalTooLong(DataType),
    /// Unsigned 64-bit value above the maximum of VOTable long.
    LongOverflow(u64),
    /// Unsigned 64-bit array element above the maximum of VOTable long.
    ArrayElementLongOverflow(u64),
    /// Null element of a numeric array, which VOTable has no representation for.
    NullArrayElement(DataType),
    /// Formatted date or timestamp outside years 0000-9999 of DALI timestamps.
    YearOutOfRange(String),
}

impl From<VoConversionErrorKind> for DataFusionError {
    fn from(kind: VoConversionErrorKind) -> Self {
        DataFusionError::External(Box::new(VoConversionError { column: None, kind }))
    }
}

/// Attaches the column name to a [VoConversionError], or as a context to other errors.
pub(super) fn with_column(error: DataFusionError, column: &str) -> DataFusionError {
    let context = || format!("Column '{column}'");
    match error {
        DataFusionError::External(error) => match error.downcast::<VoConversionError>() {
            Ok(mut error) => {
                error.column.get_or_insert_with(|| column.to_owned());
                DataFusionError::External(error)
            }
            Err(error) => DataFusionError::External(error).context(context()),
        },
        error => error.context(context()),
    }
}

/// Turns a [VoConversionErrorKind::LongOverflow] of array elements into
/// [VoConversionErrorKind::ArrayElementLongOverflow], keeps other errors as they are.
pub(super) fn in_array_element(error: DataFusionError) -> DataFusionError {
    match error {
        DataFusionError::External(error) => match error.downcast::<VoConversionError>() {
            Ok(mut error) => {
                if let VoConversionErrorKind::LongOverflow(value) = error.kind {
                    error.kind = VoConversionErrorKind::ArrayElementLongOverflow(value);
                }
                DataFusionError::External(error)
            }
            Err(error) => DataFusionError::External(error),
        },
        error => error,
    }
}

impl fmt::Display for VoConversionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Column as written in ADQL examples
        let identifier = self
            .column
            .as_deref()
            .map_or("<column>".into(), delimit_if_needed);
        match &self.column {
            Some(column) => write!(f, "Column '{column}' ")?,
            None => write!(f, "A column ")?,
        }
        match &self.kind {
            VoConversionErrorKind::UnsupportedType(DataType::Null) => write!(
                f,
                "has no type, for example it is a bare NULL. \
                CAST it to a type, e.g. CAST(NULL AS INTEGER)."
            ),
            VoConversionErrorKind::UnsupportedType(
                data_type @ (DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)),
            ) => write!(
                f,
                "has type {data_type}, which VOTable can't represent. \
                CAST it to VARCHAR to get the values as strings, \
                e.g. CAST({identifier} AS VARCHAR)."
            ),
            VoConversionErrorKind::UnsupportedType(data_type) => write!(
                f,
                "has type {data_type}, which VOTable can't represent. Exclude it from the query."
            ),
            VoConversionErrorKind::DecimalTooLong(data_type) => write!(
                f,
                "has type {data_type}, which VOTable float types can't hold exactly: they hold \
                decimals with up to 15 significant digits and a scale from 0 to 22, or with up \
                to 6 digits. CAST it to DOUBLE PRECISION to accept rounding, or to VARCHAR to \
                get the exact values as strings."
            ),
            VoConversionErrorKind::LongOverflow(value) => write!(
                f,
                "has value {value}, above the maximum of VOTable long, {max}. Replace such \
                values with NULL, e.g. CASE WHEN {identifier} <= {max} THEN {identifier} END, \
                exclude such rows, or CAST the column to DOUBLE PRECISION to accept rounding \
                or to VARCHAR to get the exact values as strings.",
                max = i64::MAX,
            ),
            VoConversionErrorKind::ArrayElementLongOverflow(value) => write!(
                f,
                "has an array element {value}, above the maximum of VOTable long, {max}. \
                Exclude such rows or the column from the query.",
                max = i64::MAX,
            ),
            VoConversionErrorKind::NullArrayElement(element_type) => write!(
                f,
                "has an array with a null element, which VOTable can't represent for \
                {element_type} elements. Exclude such rows or the column from the query."
            ),
            VoConversionErrorKind::YearOutOfRange(value) => write!(
                f,
                "has value {value} outside years 0000-9999, which DALI timestamps can't \
                represent. Replace such values with NULL using CASE, exclude such rows, or \
                CAST the column to VARCHAR to get the values as strings."
            ),
        }
    }
}

impl std::error::Error for VoConversionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::TimeUnit;

    fn message(kind: VoConversionErrorKind, column: &str) -> String {
        match with_column(kind.into(), column) {
            DataFusionError::External(error) => error.to_string(),
            other => panic!("Not external: {other:?}"),
        }
    }

    #[test]
    fn messages() {
        assert_eq!(
            message(VoConversionErrorKind::UnsupportedType(DataType::Null), "n"),
            "Column 'n' has no type, for example it is a bare NULL. \
            CAST it to a type, e.g. CAST(NULL AS INTEGER)."
        );
        assert_eq!(
            message(
                VoConversionErrorKind::UnsupportedType(DataType::Time64(TimeUnit::Microsecond)),
                "Time"
            ),
            "Column 'Time' has type Time64(µs), which VOTable can't represent. \
            CAST it to VARCHAR to get the values as strings, e.g. CAST(\"Time\" AS VARCHAR)."
        );
        assert_eq!(
            message(
                VoConversionErrorKind::DecimalTooLong(DataType::Decimal128(38, 10)),
                "d"
            ),
            "Column 'd' has type Decimal128(38, 10), which VOTable float types can't hold \
            exactly: they hold decimals with up to 15 significant digits and a scale from 0 to \
            22, or with up to 6 digits. CAST it to DOUBLE PRECISION to accept rounding, or to \
            VARCHAR to get the exact values as strings."
        );
        assert_eq!(
            message(VoConversionErrorKind::LongOverflow(u64::MAX), "x"),
            "Column 'x' has value 18446744073709551615, above the maximum of VOTable long, \
            9223372036854775807. Replace such values with NULL, e.g. CASE WHEN x <= \
            9223372036854775807 THEN x END, exclude such rows, or CAST the column to DOUBLE \
            PRECISION to accept rounding or to VARCHAR to get the exact values as strings."
        );
        assert_eq!(
            message(
                VoConversionErrorKind::ArrayElementLongOverflow(u64::MAX),
                "a"
            ),
            "Column 'a' has an array element 18446744073709551615, above the maximum of VOTable \
            long, 9223372036854775807. Exclude such rows or the column from the query."
        );
        assert_eq!(
            message(
                VoConversionErrorKind::NullArrayElement(DataType::Float64),
                "v"
            ),
            "Column 'v' has an array with a null element, which VOTable can't represent for \
            Float64 elements. Exclude such rows or the column from the query."
        );
        assert_eq!(
            message(
                VoConversionErrorKind::YearOutOfRange("+10000-01-01".into()),
                "t"
            ),
            "Column 't' has value +10000-01-01 outside years 0000-9999, which DALI timestamps \
            can't represent. Replace such values with NULL using CASE, exclude such rows, or \
            CAST the column to VARCHAR to get the values as strings."
        );
    }

    #[test]
    fn column_attached_once() {
        let error = with_column(
            with_column(VoConversionErrorKind::LongOverflow(1).into(), "inner"),
            "outer",
        );
        assert!(error.to_string().contains("Column 'inner'"), "{error}");
    }

    #[test]
    fn other_errors_get_context() {
        let error = with_column(DataFusionError::Internal("oops".into()), "x");
        assert!(matches!(error, DataFusionError::Context(..)), "{error:?}");
        assert!(error.to_string().contains("Column 'x'"), "{error}");
    }
}
