use crate::arrow_votable::error::VoConversionErrorKind;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, FieldRef};
use datafusion::common::{DataFusionError, Result as DFResult};
use std::sync::Arc;
use votable::datatype::Datatype;
use votable::field::ArraySize;

/// VOTable representation of an Arrow type.
pub(super) trait ArrowDataTypeVoExt {
    /// Returns the Arrow type whose values are serialized as VOTable values of this type.
    ///
    /// Each storage type maps to a single VOTable datatype and arraysize, see
    /// [ArrowDataTypeVoExt::vo_datatype].
    /// Some Arrow types have no exact VOTable counterpart and require data conversion:
    /// - signed and unsigned integers are widened to the smallest signed VOTable integer type
    ///   holding all their values, except for uint64, which is mapped to long, and values above
    ///   its maximum are an error
    /// - float16 is widened to float
    /// - decimals are mapped to float or double holding all their values exactly, decimals
    ///   too long for that are an error, see [decimal_storage]
    /// - dates and timestamps are stored as DALI strings, see [ArrowDataTypeVoExt::xtype]
    /// - dictionary-encoded arrays are stored as their value type
    /// - list views are stored as lists of the same offset type
    /// - strings are stored as UTF-8 `char` arrays, which VOTable allows starting from
    ///   version 1.6
    ///
    /// Arrays are supported only as one-dimensional lists of numeric or boolean values.
    /// Returns an error for types which can't be represented in VOTable, for example structs,
    /// nested lists, or lists of strings.
    fn storage_type(&self) -> DFResult<ArrowDataType>;

    /// Returns the VOTable datatype and arraysize the values of this type are serialized as.
    fn vo_datatype(&self) -> DFResult<(Datatype, Option<ArraySize>)>;

    /// Returns the DALI xtype of this type, see DALI 1.1, Section 3.3.
    ///
    /// xtype tells how to interpret the primitive values a column is stored as. For example,
    /// dates and timestamps are stored as `char[*]`, and xtype "timestamp" tells that these
    /// strings are ISO-8601 timestamps.
    /// Each xtype requires the values to be converted to its serialization before writing,
    /// and [ArrowDataTypeVoExt::storage_type] to return the type of this serialization.
    /// To support a new xtype, add the Arrow types having it here, and their storage to
    /// [ArrowDataTypeVoExt::storage_type].
    fn xtype(&self) -> Option<&'static str>;
}

impl ArrowDataTypeVoExt for ArrowDataType {
    fn storage_type(&self) -> DFResult<ArrowDataType> {
        let storage = match self {
            ArrowDataType::Boolean
            | ArrowDataType::UInt8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::Float32
            | ArrowDataType::Float64
            | ArrowDataType::Utf8
            | ArrowDataType::Binary
            | ArrowDataType::FixedSizeBinary(_) => self.clone(),
            ArrowDataType::Int8 => ArrowDataType::Int16,
            ArrowDataType::UInt16 => ArrowDataType::Int32,
            ArrowDataType::UInt32 | ArrowDataType::UInt64 => ArrowDataType::Int64,
            ArrowDataType::Float16 => ArrowDataType::Float32,
            ArrowDataType::Decimal32(precision, scale)
            | ArrowDataType::Decimal64(precision, scale)
            | ArrowDataType::Decimal128(precision, scale)
            | ArrowDataType::Decimal256(precision, scale) => decimal_storage(*precision, *scale)
                .ok_or_else(|| VoConversionErrorKind::DecimalTooLong(self.clone()))?,
            ArrowDataType::LargeUtf8
            | ArrowDataType::Utf8View
            | ArrowDataType::Date32
            | ArrowDataType::Date64
            | ArrowDataType::Timestamp(..) => ArrowDataType::Utf8,
            ArrowDataType::LargeBinary | ArrowDataType::BinaryView => ArrowDataType::Binary,
            ArrowDataType::Dictionary(_key, value) => value.storage_type()?,
            ArrowDataType::List(item) | ArrowDataType::ListView(item) => {
                ArrowDataType::List(element(item).ok_or_else(|| unsupported(self))?)
            }
            ArrowDataType::LargeList(item) | ArrowDataType::LargeListView(item) => {
                ArrowDataType::LargeList(element(item).ok_or_else(|| unsupported(self))?)
            }
            ArrowDataType::FixedSizeList(item, n) => {
                ArrowDataType::FixedSizeList(element(item).ok_or_else(|| unsupported(self))?, *n)
            }
            _ => return Err(unsupported(self)),
        };
        Ok(storage)
    }

    fn vo_datatype(&self) -> DFResult<(Datatype, Option<ArraySize>)> {
        let storage = self.storage_type()?;
        if let Some(datatype) = primitive_vo_datatype(&storage) {
            return Ok((datatype, None));
        }
        let vo_datatype = match &storage {
            ArrowDataType::Utf8 => (Datatype::CharASCII, Some(ArraySize::Variable1D)),
            ArrowDataType::Binary => (Datatype::Byte, Some(ArraySize::Variable1D)),
            ArrowDataType::FixedSizeBinary(n) => (Datatype::Byte, Some(fixed_size(*n)?)),
            ArrowDataType::List(item) | ArrowDataType::LargeList(item) => {
                (element_vo_datatype(item)?, Some(ArraySize::Variable1D))
            }
            ArrowDataType::FixedSizeList(item, n) => {
                (element_vo_datatype(item)?, Some(fixed_size(*n)?))
            }
            _ => return Err(not_storage(&storage)),
        };
        Ok(vo_datatype)
    }

    fn xtype(&self) -> Option<&'static str> {
        match self {
            ArrowDataType::Date32 | ArrowDataType::Date64 | ArrowDataType::Timestamp(..) => {
                Some("timestamp")
            }
            ArrowDataType::Dictionary(_key, value) => value.xtype(),
            _ => None,
        }
    }
}

/// Returns the floating-point type holding every value of a decimal type exactly, [None] if
/// there is no such type.
///
/// Values of a decimal type with `precision` significant digits round-trip through float32 if
/// they have at most 6 digits and fit the float32 normal range, and through float64 if they
/// have at most 15 digits, see `FLT_DIG` and `DBL_DIG` of C.
/// The Arrow cast to float divides the decimal integer by `10_f64.powi(scale)`, which is exact,
/// making the cast a single correctly rounded division, only for scales from 0 to 22, so
/// float64 is used for these scales only. The float32 margin covers the rounding errors of any
/// scale.
fn decimal_storage(precision: u8, scale: i8) -> Option<ArrowDataType> {
    let (precision, scale) = (i32::from(precision), i32::from(scale));
    // Values are from 10^-scale to 10^(precision - scale), float32 normal range is from
    // 1.2e-38 to 3.4e38
    if precision <= 6 && scale <= 37 && precision - scale <= 38 {
        Some(ArrowDataType::Float32)
    } else if precision <= 15 && (0..=22).contains(&scale) {
        Some(ArrowDataType::Float64)
    } else {
        None
    }
}

/// Returns the storage of an array element, [None] if it isn't a numeric or boolean scalar.
fn element(item: &FieldRef) -> Option<FieldRef> {
    let storage = item.data_type().storage_type().ok()?;
    primitive_vo_datatype(&storage)?;
    Some(Arc::new(item.as_ref().clone().with_data_type(storage)))
}

/// Returns the VOTable datatype of a numeric or boolean storage type, [None] for other types.
fn primitive_vo_datatype(storage: &ArrowDataType) -> Option<Datatype> {
    let datatype = match storage {
        ArrowDataType::Boolean => Datatype::Logical,
        ArrowDataType::UInt8 => Datatype::Byte,
        ArrowDataType::Int16 => Datatype::ShortInt,
        ArrowDataType::Int32 => Datatype::Int,
        ArrowDataType::Int64 => Datatype::LongInt,
        ArrowDataType::Float32 => Datatype::Float,
        ArrowDataType::Float64 => Datatype::Double,
        _ => return None,
    };
    Some(datatype)
}

/// Returns the VOTable datatype of a list element of a storage type.
fn element_vo_datatype(item: &ArrowField) -> DFResult<Datatype> {
    primitive_vo_datatype(item.data_type()).ok_or_else(|| not_storage(item.data_type()))
}

fn fixed_size(n: i32) -> DFResult<ArraySize> {
    u32::try_from(n)
        .map(ArraySize::new_fixed_1d)
        .map_err(|_| DataFusionError::Internal(format!("Negative fixed array size {n}")))
}

fn unsupported(data_type: &ArrowDataType) -> DataFusionError {
    VoConversionErrorKind::UnsupportedType(data_type.clone()).into()
}

/// Error for a type which is expected to be a [ArrowDataTypeVoExt::storage_type], but isn't.
pub(super) fn not_storage(data_type: &ArrowDataType) -> DataFusionError {
    DataFusionError::Internal(format!(
        "Arrow type {data_type} is not a VOTable storage type"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Fields, TimeUnit};

    /// Maps an Arrow type, formatting the result as (datatype, arraysize, xtype).
    fn strings(data_type: &ArrowDataType) -> (String, Option<String>, Option<String>) {
        let (datatype, arraysize) = data_type.vo_datatype().unwrap();
        (
            datatype.to_string(),
            arraysize.map(|size| size.to_string()),
            data_type.xtype().map(String::from),
        )
    }

    fn item(data_type: ArrowDataType) -> FieldRef {
        Arc::new(ArrowField::new_list_field(data_type, true))
    }

    fn dictionary(value: ArrowDataType) -> ArrowDataType {
        ArrowDataType::Dictionary(Box::new(ArrowDataType::Int32), Box::new(value))
    }

    #[test]
    fn scalars() {
        for (data_type, expected) in [
            (ArrowDataType::Boolean, "boolean"),
            (ArrowDataType::UInt8, "unsignedByte"),
            (ArrowDataType::Int8, "short"),
            (ArrowDataType::Int16, "short"),
            (ArrowDataType::UInt16, "int"),
            (ArrowDataType::Int32, "int"),
            (ArrowDataType::UInt32, "long"),
            (ArrowDataType::Int64, "long"),
            (ArrowDataType::UInt64, "long"),
            (ArrowDataType::Float16, "float"),
            (ArrowDataType::Float32, "float"),
            (ArrowDataType::Float64, "double"),
            (ArrowDataType::Decimal32(6, 2), "float"),
            (ArrowDataType::Decimal128(6, 37), "float"),
            (ArrowDataType::Decimal128(6, -32), "float"),
            (ArrowDataType::Decimal64(7, 2), "double"),
            (ArrowDataType::Decimal128(15, 0), "double"),
            (ArrowDataType::Decimal256(15, 22), "double"),
            (dictionary(ArrowDataType::Float32), "float"),
        ] {
            assert_eq!(
                strings(&data_type),
                (expected.to_string(), None, None),
                "{data_type}"
            );
        }
    }

    #[test]
    fn variable_arrays() {
        for (data_type, expected) in [
            (ArrowDataType::Utf8, "char"),
            (ArrowDataType::LargeUtf8, "char"),
            (ArrowDataType::Utf8View, "char"),
            (ArrowDataType::Binary, "unsignedByte"),
            (ArrowDataType::List(item(ArrowDataType::Float64)), "double"),
            (
                ArrowDataType::LargeList(item(ArrowDataType::Int16)),
                "short",
            ),
            (dictionary(ArrowDataType::Utf8), "char"),
        ] {
            assert_eq!(
                strings(&data_type),
                (expected.to_string(), Some("*".to_string()), None),
                "{data_type}"
            );
        }
    }

    #[test]
    fn fixed_arrays() {
        assert_eq!(
            strings(&ArrowDataType::FixedSizeList(
                item(ArrowDataType::Float32),
                3
            )),
            ("float".to_string(), Some("3".to_string()), None)
        );
        assert_eq!(
            strings(&ArrowDataType::FixedSizeBinary(16)),
            ("unsignedByte".to_string(), Some("16".to_string()), None)
        );
    }

    #[test]
    fn timestamps() {
        for data_type in [
            ArrowDataType::Date32,
            ArrowDataType::Date64,
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            dictionary(ArrowDataType::Date32),
        ] {
            assert_eq!(
                strings(&data_type),
                (
                    "char".to_string(),
                    Some("*".to_string()),
                    Some("timestamp".to_string())
                ),
                "{data_type}"
            );
        }
    }

    #[test]
    fn unrepresentable() {
        for data_type in [
            ArrowDataType::Null,
            ArrowDataType::Time64(TimeUnit::Microsecond),
            ArrowDataType::Struct(Fields::from(vec![ArrowField::new(
                "a",
                ArrowDataType::Int32,
                true,
            )])),
            ArrowDataType::List(item(ArrowDataType::Utf8)),
            ArrowDataType::List(item(ArrowDataType::List(item(ArrowDataType::Int32)))),
            ArrowDataType::List(item(ArrowDataType::Timestamp(TimeUnit::Second, None))),
            // Too many digits for float64
            ArrowDataType::Decimal128(16, 0),
            ArrowDataType::Decimal128(38, 10),
            // Scales making the Arrow cast to float64 inexact, too many digits for float32
            ArrowDataType::Decimal128(7, 23),
            ArrowDataType::Decimal128(7, -1),
            // Out of float32 range, scale making the Arrow cast to float64 inexact
            ArrowDataType::Decimal128(6, 38),
            ArrowDataType::Decimal128(6, -33),
        ] {
            assert!(data_type.vo_datatype().is_err(), "{data_type}");
        }
    }
}
