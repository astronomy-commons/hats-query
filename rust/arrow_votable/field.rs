use datafusion::arrow::datatypes::{DataType, Field as ArrowField};
use datafusion::common::{DataFusionError, Result as DFResult};
use votable::datatype::Datatype;
use votable::field::{ArraySize, Field};

/// Maps an Arrow field to a VOTable FIELD, as used in VOTable output and TAP_SCHEMA.columns.
///
/// Returns an error for types which can't be represented in VOTable, see [storage].
pub(crate) fn vo_field(field: &ArrowField) -> DFResult<Field> {
    let data_type = field.data_type();
    let (datatype, arraysize) = storage(data_type)?;
    let mut vo_field = Field::new(field.name(), datatype);
    vo_field.arraysize = arraysize;
    vo_field.xtype = xtype(data_type).map(String::from);
    Ok(vo_field)
}

/// Returns the primitive VOTable datatype and arraysize the values of an Arrow type are stored as.
///
/// Some Arrow types have no exact VOTable counterpart and require data conversion:
/// - signed and unsigned integers are widened to the smallest signed VOTable integer type
///   holding all their values, except for uint64, which is mapped to long and may overflow
/// - float16 is widened to float
/// - decimals are mapped to double, potentially losing precision
/// - dates and timestamps are stored as strings, see [xtype]
/// - dictionary-encoded arrays are stored as their value type
///
/// Arrays are supported only as one-dimensional lists of numeric or boolean values.
/// Returns an error for types which can't be represented in VOTable, for example structs,
/// nested lists, or lists of strings.
fn storage(data_type: &DataType) -> DFResult<(Datatype, Option<ArraySize>)> {
    let storage = match data_type {
        DataType::Boolean => (Datatype::Logical, None),
        DataType::UInt8 => (Datatype::Byte, None),
        DataType::Int8 | DataType::Int16 => (Datatype::ShortInt, None),
        DataType::UInt16 | DataType::Int32 => (Datatype::Int, None),
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => (Datatype::LongInt, None),
        DataType::Float16 | DataType::Float32 => (Datatype::Float, None),
        DataType::Float64
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => (Datatype::Double, None),
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(..) => (Datatype::CharASCII, Some(ArraySize::Variable1D)),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            (Datatype::Byte, Some(ArraySize::Variable1D))
        }
        DataType::FixedSizeBinary(n) => (Datatype::Byte, Some(fixed_size(*n)?)),
        DataType::Dictionary(_key, value) => storage(value)?,
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => (
            element_datatype(item.data_type())?,
            Some(ArraySize::Variable1D),
        ),
        DataType::FixedSizeList(item, n) => {
            (element_datatype(item.data_type())?, Some(fixed_size(*n)?))
        }
        _ => return Err(unrepresentable(data_type)),
    };
    Ok(storage)
}

/// Returns the DALI xtype of an Arrow type, see DALI 1.1, Section 3.3.
///
/// xtype tells how to interpret the primitive values a column is stored as, see [storage].
/// For example, dates and timestamps are stored as `char[*]`, and xtype "timestamp" tells
/// that these strings are ISO-8601 timestamps.
/// Each xtype requires the values to be converted to its serialization before writing,
/// and [storage] to return the datatype and arraysize of this serialization.
/// To support a new xtype, add the Arrow types having it here, and their storage to [storage].
fn xtype(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => Some("timestamp"),
        DataType::Dictionary(_key, value) => xtype(value),
        _ => None,
    }
}

/// Returns the VOTable datatype of an array element, which must be a scalar without xtype.
fn element_datatype(data_type: &DataType) -> DFResult<Datatype> {
    match (storage(data_type)?, xtype(data_type)) {
        ((datatype, None), None) => Ok(datatype),
        _ => Err(unrepresentable(data_type)),
    }
}

fn fixed_size(n: i32) -> DFResult<ArraySize> {
    u32::try_from(n)
        .map(ArraySize::new_fixed_1d)
        .map_err(|_| DataFusionError::Internal(format!("Negative fixed array size {n}")))
}

fn unrepresentable(data_type: &DataType) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "Arrow type {data_type} can't be represented in VOTable"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{FieldRef, Fields, TimeUnit};
    use std::sync::Arc;

    /// Maps an Arrow field of the given type, formatting the result as (datatype, arraysize, xtype).
    fn strings(data_type: &DataType) -> (String, Option<String>, Option<String>) {
        let field = vo_field(&ArrowField::new("x", data_type.clone(), true)).unwrap();
        (
            field.datatype.to_string(),
            field.arraysize.map(|size| size.to_string()),
            field.xtype,
        )
    }

    fn item(data_type: DataType) -> FieldRef {
        Arc::new(ArrowField::new_list_field(data_type, true))
    }

    fn dictionary(value: DataType) -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(value))
    }

    #[test]
    fn name() {
        let field = vo_field(&ArrowField::new("ra", DataType::Float64, false)).unwrap();
        assert_eq!(field.name, "ra");
    }

    #[test]
    fn scalars() {
        for (data_type, expected) in [
            (DataType::Boolean, "boolean"),
            (DataType::UInt8, "unsignedByte"),
            (DataType::Int8, "short"),
            (DataType::Int16, "short"),
            (DataType::UInt16, "int"),
            (DataType::Int32, "int"),
            (DataType::UInt32, "long"),
            (DataType::Int64, "long"),
            (DataType::UInt64, "long"),
            (DataType::Float16, "float"),
            (DataType::Float32, "float"),
            (DataType::Float64, "double"),
            (DataType::Decimal128(10, 2), "double"),
            (dictionary(DataType::Float32), "float"),
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
            (DataType::Utf8, "char"),
            (DataType::LargeUtf8, "char"),
            (DataType::Utf8View, "char"),
            (DataType::Binary, "unsignedByte"),
            (DataType::List(item(DataType::Float64)), "double"),
            (DataType::LargeList(item(DataType::Int16)), "short"),
            (dictionary(DataType::Utf8), "char"),
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
            strings(&DataType::FixedSizeList(item(DataType::Float32), 3)),
            ("float".to_string(), Some("3".to_string()), None)
        );
        assert_eq!(
            strings(&DataType::FixedSizeBinary(16)),
            ("unsignedByte".to_string(), Some("16".to_string()), None)
        );
    }

    #[test]
    fn timestamps() {
        for data_type in [
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            dictionary(DataType::Date32),
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
            DataType::Null,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Struct(Fields::from(vec![ArrowField::new(
                "a",
                DataType::Int32,
                true,
            )])),
            DataType::List(item(DataType::Utf8)),
            DataType::List(item(DataType::List(item(DataType::Int32)))),
            DataType::List(item(DataType::Timestamp(TimeUnit::Second, None))),
        ] {
            let field = ArrowField::new("x", data_type.clone(), true);
            assert!(vo_field(&field).is_err(), "{data_type}");
        }
    }
}
