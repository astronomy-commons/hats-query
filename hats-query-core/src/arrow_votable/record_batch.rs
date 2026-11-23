use crate::arrow_votable::array::ArrowArrayVoExt as _;
use crate::arrow_votable::data_type::not_storage;
use crate::arrow_votable::error::{VoConversionErrorKind, with_column};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, UInt8Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use votable::impls::VOTableValue;

/// VOTable representation of an Arrow record batch.
pub(super) trait ArrowRecordBatchVoExt {
    /// Returns the rows of this batch as VOTable values, matching the FIELDs of
    /// [crate::arrow_votable::field::ArrowFieldVoExt::vo_field] for the batch schema.
    fn vo_rows(&self) -> DFResult<Vec<Vec<VOTableValue>>>;
}

impl ArrowRecordBatchVoExt for RecordBatch {
    fn vo_rows(&self) -> DFResult<Vec<Vec<VOTableValue>>> {
        let schema = self.schema();
        let names: Vec<_> = schema.fields().iter().map(|field| field.name()).collect();
        let arrays = self
            .columns()
            .iter()
            .zip(&names)
            .map(|(column, name)| column.storage_array().map_err(|e| with_column(e, name)))
            .collect::<DFResult<Vec<_>>>()?;
        (0..self.num_rows())
            .map(|row| {
                arrays
                    .iter()
                    .zip(&names)
                    .map(|(array, name)| {
                        value(array.as_ref(), row).map_err(|e| with_column(e, name))
                    })
                    .collect()
            })
            .collect()
    }
}

/// Converts the value at `i` of a storage array.
fn value(array: &dyn Array, i: usize) -> DFResult<VOTableValue> {
    if array.is_null(i) {
        return Ok(VOTableValue::Null);
    }
    let value = match array.data_type() {
        DataType::Boolean => VOTableValue::Bool(array.as_boolean().value(i)),
        DataType::UInt8 => VOTableValue::Byte(array.as_primitive::<UInt8Type>().value(i)),
        DataType::Int16 => VOTableValue::Short(array.as_primitive::<Int16Type>().value(i)),
        DataType::Int32 => VOTableValue::Int(array.as_primitive::<Int32Type>().value(i)),
        DataType::Int64 => VOTableValue::Long(array.as_primitive::<Int64Type>().value(i)),
        DataType::Float32 => VOTableValue::Float(array.as_primitive::<Float32Type>().value(i)),
        DataType::Float64 => VOTableValue::Double(array.as_primitive::<Float64Type>().value(i)),
        DataType::Utf8 => VOTableValue::String(array.as_string::<i32>().value(i).to_owned()),
        DataType::Binary => VOTableValue::ByteArray(array.as_binary::<i32>().value(i).to_vec()),
        DataType::FixedSizeBinary(_) => {
            VOTableValue::ByteArray(array.as_fixed_size_binary().value(i).to_vec())
        }
        DataType::List(_) => elements(array.as_list::<i32>().value(i).as_ref())?,
        DataType::LargeList(_) => elements(array.as_list::<i64>().value(i).as_ref())?,
        DataType::FixedSizeList(..) => elements(array.as_fixed_size_list().value(i).as_ref())?,
        data_type => return Err(not_storage(data_type)),
    };
    Ok(value)
}

/// Converts the elements of an array value, a numeric or boolean storage array.
///
/// Null elements are representable for logical arrays only.
fn elements(array: &dyn Array) -> DFResult<VOTableValue> {
    if let DataType::Boolean = array.data_type() {
        return Ok(VOTableValue::BooleanArray(
            array.as_boolean().iter().collect(),
        ));
    }
    if array.null_count() > 0 {
        return Err(VoConversionErrorKind::NullArrayElement(array.data_type().clone()).into());
    }
    let value = match array.data_type() {
        DataType::UInt8 => {
            VOTableValue::ByteArray(array.as_primitive::<UInt8Type>().values().to_vec())
        }
        DataType::Int16 => {
            VOTableValue::ShortArray(array.as_primitive::<Int16Type>().values().to_vec())
        }
        DataType::Int32 => {
            VOTableValue::IntArray(array.as_primitive::<Int32Type>().values().to_vec())
        }
        DataType::Int64 => {
            VOTableValue::LongArray(array.as_primitive::<Int64Type>().values().to_vec())
        }
        DataType::Float32 => {
            VOTableValue::FloatArray(array.as_primitive::<Float32Type>().values().to_vec())
        }
        DataType::Float64 => {
            VOTableValue::DoubleArray(array.as_primitive::<Float64Type>().values().to_vec())
        }
        data_type => return Err(not_storage(data_type)),
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
        FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, LargeListArray, LargeListViewArray, ListArray, ListViewArray, StringArray,
        UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{Field, FieldRef, Schema};
    use std::sync::Arc;
    use votable::impls::VOTableValue as V;

    /// Converts a single column, returns its values.
    fn column(array: ArrayRef) -> Vec<V> {
        let schema = Schema::new(vec![Field::new("x", array.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();
        batch
            .vo_rows()
            .unwrap()
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect()
    }

    fn item(data_type: DataType) -> FieldRef {
        Arc::new(Field::new_list_field(data_type, true))
    }

    #[test]
    fn rows_of_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
            ],
        )
        .unwrap()
        .slice(1, 2);
        assert_eq!(
            batch.vo_rows().unwrap(),
            [
                vec![V::Null, V::String("y".into())],
                vec![V::Int(3), V::Null]
            ]
        );
    }

    #[test]
    fn scalars() {
        assert_eq!(
            column(Arc::new(BooleanArray::from(vec![Some(true), None]))),
            [V::Bool(true), V::Null]
        );
        assert_eq!(
            column(Arc::new(UInt8Array::from(vec![255]))),
            [V::Byte(255)]
        );
        assert_eq!(
            column(Arc::new(Int8Array::from(vec![-128]))),
            [V::Short(-128)]
        );
        assert_eq!(column(Arc::new(Int16Array::from(vec![-1]))), [V::Short(-1)]);
        assert_eq!(
            column(Arc::new(UInt16Array::from(vec![u16::MAX]))),
            [V::Int(u16::MAX.into())]
        );
        assert_eq!(column(Arc::new(Int32Array::from(vec![-1]))), [V::Int(-1)]);
        assert_eq!(
            column(Arc::new(UInt32Array::from(vec![u32::MAX]))),
            [V::Long(u32::MAX.into())]
        );
        assert_eq!(column(Arc::new(Int64Array::from(vec![-1]))), [V::Long(-1)]);
        assert_eq!(
            column(Arc::new(UInt64Array::from(vec![i64::MAX as u64]))),
            [V::Long(i64::MAX)]
        );
        assert_eq!(
            column(Arc::new(Float32Array::from(vec![0.5]))),
            [V::Float(0.5)]
        );
        assert_eq!(
            column(Arc::new(Float64Array::from(vec![0.5]))),
            [V::Double(0.5)]
        );
        let decimals = Decimal128Array::from(vec![12_345])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(column(Arc::new(decimals)), [V::Double(123.45)]);
    }

    #[test]
    fn strings_and_binaries() {
        assert_eq!(
            column(Arc::new(StringArray::from(vec![
                Some("Ångström ✓ 🌌 Ыё"),
                None
            ]))),
            [V::String("Ångström ✓ 🌌 Ыё".into()), V::Null]
        );
        let binaries: Vec<Option<&[u8]>> = vec![Some(b"\x00\xff"), None];
        assert_eq!(
            column(Arc::new(BinaryArray::from(binaries))),
            [V::ByteArray(vec![0, 255]), V::Null]
        );
        let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(b"\x01\x02"), None].into_iter(),
            2,
        )
        .unwrap();
        assert_eq!(column(Arc::new(fixed)), [V::ByteArray(vec![1, 2]), V::Null]);
    }

    #[test]
    fn variable_arrays() {
        let doubles = ListArray::new(
            item(DataType::Float64),
            OffsetBuffer::from_lengths([2, 0, 0]),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
            Some(NullBuffer::from(vec![true, true, false])),
        );
        assert_eq!(
            column(Arc::new(doubles)),
            [
                V::DoubleArray(vec![1.0, 2.0]),
                V::DoubleArray(vec![]),
                V::Null
            ]
        );

        let shorts = LargeListArray::new(
            item(DataType::Int8),
            OffsetBuffer::from_lengths([1, 2]),
            Arc::new(Int8Array::from(vec![-1, 0, 1])),
            None,
        );
        assert_eq!(
            column(Arc::new(shorts)),
            [V::ShortArray(vec![-1]), V::ShortArray(vec![0, 1])]
        );

        let longs = ListViewArray::new(
            item(DataType::Int64),
            ScalarBuffer::from(vec![1, 0]),
            ScalarBuffer::from(vec![2, 1]),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            None,
        );
        assert_eq!(
            column(Arc::new(longs)),
            [V::LongArray(vec![2, 3]), V::LongArray(vec![1])]
        );

        let ints = LargeListViewArray::new(
            item(DataType::UInt16),
            ScalarBuffer::from(vec![2, 0]),
            ScalarBuffer::from(vec![1, 2]),
            Arc::new(UInt16Array::from(vec![1, 2, 65_535])),
            Some(NullBuffer::from(vec![true, false])),
        );
        assert_eq!(column(Arc::new(ints)), [V::IntArray(vec![65_535]), V::Null]);
    }

    #[test]
    fn fixed_arrays() {
        let floats = FixedSizeListArray::new(
            item(DataType::Float32),
            2,
            Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            Some(NullBuffer::from(vec![true, false])),
        );
        assert_eq!(
            column(Arc::new(floats)),
            [V::FloatArray(vec![1.0, 2.0]), V::Null]
        );

        let bytes = FixedSizeListArray::new(
            item(DataType::UInt8),
            1,
            Arc::new(UInt8Array::from(vec![1, 2, 3])),
            None,
        )
        .slice(1, 2);
        assert_eq!(
            column(Arc::new(bytes)),
            [V::ByteArray(vec![2]), V::ByteArray(vec![3])]
        );
    }

    #[test]
    fn null_boolean_elements() {
        let booleans = ListArray::new(
            item(DataType::Boolean),
            OffsetBuffer::from_lengths([3]),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            None,
        );
        assert_eq!(
            column(Arc::new(booleans)),
            [V::BooleanArray(vec![Some(true), None, Some(false)])]
        );
    }

    #[test]
    fn null_numeric_elements_are_unrepresentable() {
        let elements: [ArrayRef; 2] = [
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(Float64Array::from(vec![Some(1.0), None])),
        ];
        for values in elements {
            let list = ListArray::new(
                item(values.data_type().clone()),
                OffsetBuffer::from_lengths([2]),
                values,
                None,
            );
            let schema = Schema::new(vec![Field::new("x", list.data_type().clone(), true)]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(list)]).unwrap();
            let error = batch.vo_rows().unwrap_err().to_string();
            assert!(
                error.contains("Column 'x' has an array with a null element"),
                "{error}"
            );
        }
    }
}
