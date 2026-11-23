use crate::arrow_votable::data_type::ArrowDataTypeVoExt as _;
use crate::arrow_votable::error::{VoConversionErrorKind, in_array_element};
use chrono::{NaiveDate, NaiveTime};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray, OffsetSizeTrait, PrimitiveArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{CastOptions, cast_with_options, max, min};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, ArrowTimestampType, DataType, Date32Type, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt64Type,
};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::{DataFusionError, Result as DFResult};
use std::ops::Range;
use std::sync::Arc;

/// UTC as an Arrow timestamp time zone, timezone-aware timestamps are converted to it before
/// formatting, which writes them with "Z".
const ARROW_UTC: &str = "+00:00";

/// Arrow array convertible to the values of a VOTable column.
pub(super) trait ArrowArrayVoExt {
    /// Converts the array to its
    /// [crate::arrow_votable::data_type::ArrowDataTypeVoExt::storage_type].
    ///
    /// Dates and timestamps are formatted by the Arrow cast kernel as DALI timestamps, see
    /// DALI 1.1, Section 3.3.3, with as many fraction digits as needed:
    /// - Date32 dates as "YYYY-MM-DD", Date64 dates as timestamps without time zone, for example
    ///   "2020-01-02T00:00:00"
    /// - timestamps with a time zone, which DALI calls civil values, in UTC with "Z", for
    ///   example "2020-01-02T03:04:05.5Z"
    /// - timestamps without time zone as they are, without "Z", for example
    ///   "2020-01-02T03:04:05.5", because DALI astronomical values never have a time zone
    ///
    /// Dates and timestamps outside years 0000–9999 are an error, DALI timestamps have
    /// four-digit years.
    fn storage_array(&self) -> DFResult<ArrayRef>;
}

impl ArrowArrayVoExt for ArrayRef {
    fn storage_array(&self) -> DFResult<ArrayRef> {
        let data_type = self.data_type();
        match data_type {
            DataType::Dictionary(_key, value) => strict_cast(self, value)?.storage_array(),
            // Arrow timestamp values are UTC whatever the time zone is, which is display-only
            DataType::Timestamp(unit, Some(tz)) if tz.as_ref() != ARROW_UTC => {
                strict_cast(self, &DataType::Timestamp(*unit, Some(ARROW_UTC.into())))?
                    .storage_array()
            }
            // Date64 values should be whole days, but may have time of day, which a cast to
            // Date32 would drop
            DataType::Date64 => {
                strict_cast(self, &DataType::Timestamp(TimeUnit::Millisecond, None))?
                    .storage_array()
            }
            DataType::Date32 => {
                check_dali_date_range(self.as_primitive::<Date32Type>())?;
                strict_cast(self, &DataType::Utf8)
            }
            DataType::Timestamp(unit, _) => {
                match unit {
                    TimeUnit::Second => {
                        check_dali_date_range(self.as_primitive::<TimestampSecondType>())
                    }
                    TimeUnit::Millisecond => {
                        check_dali_date_range(self.as_primitive::<TimestampMillisecondType>())
                    }
                    TimeUnit::Microsecond => {
                        check_dali_date_range(self.as_primitive::<TimestampMicrosecondType>())
                    }
                    TimeUnit::Nanosecond => {
                        check_dali_date_range(self.as_primitive::<TimestampNanosecondType>())
                    }
                }?;
                strict_cast(self, &DataType::Utf8)
            }
            // The cast to long would fail with a message about the value only
            DataType::UInt64 => {
                if let Some(value) = max(self.as_primitive::<UInt64Type>())
                    && i64::try_from(value).is_err()
                {
                    return Err(VoConversionErrorKind::LongOverflow(value).into());
                }
                strict_cast(self, &DataType::Int64)
            }
            DataType::List(_) => list_storage(self.as_list::<i32>()),
            DataType::LargeList(_) => list_storage(self.as_list::<i64>()),
            // The cast collects only the elements of the rows, its offsets start at 0
            DataType::ListView(item) => {
                strict_cast(self, &DataType::List(Arc::clone(item)))?.storage_array()
            }
            DataType::LargeListView(item) => {
                strict_cast(self, &DataType::LargeList(Arc::clone(item)))?.storage_array()
            }
            // A fixed-size list slice slices its child array too
            DataType::FixedSizeList(..) => {
                let DataType::FixedSizeList(item, size) = data_type.storage_type()? else {
                    return Err(DataFusionError::Internal(format!(
                        "No fixed-size list storage of {data_type}"
                    )));
                };
                let list = self.as_fixed_size_list();
                Ok(Arc::new(FixedSizeListArray::try_new(
                    item,
                    size,
                    list.values().storage_array().map_err(in_array_element)?,
                    list.nulls().cloned(),
                )?))
            }
            _ => {
                let storage = data_type.storage_type()?;
                if &storage == data_type {
                    return Ok(Arc::clone(self));
                }
                strict_cast(self, &storage)
            }
        }
    }
}

/// Converts a list or a large list to its
/// [crate::arrow_votable::data_type::ArrowDataTypeVoExt::storage_type], a list of the same
/// offset type.
///
/// A list slice shares the whole child array, only the elements of its rows are converted.
fn list_storage<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> DFResult<ArrayRef> {
    let (DataType::List(item) | DataType::LargeList(item)) = list.data_type().storage_type()?
    else {
        return Err(DataFusionError::Internal(format!(
            "No list storage of {}",
            list.data_type()
        )));
    };
    let offsets = list.offsets();
    let first = offsets[0].as_usize();
    let last = offsets[offsets.len() - 1].as_usize();
    let values = list
        .values()
        .slice(first, last - first)
        .storage_array()
        .map_err(in_array_element)?;
    // Offsets into the elements of the rows
    let offsets = if first == 0 {
        offsets.clone()
    } else {
        OffsetBuffer::new(offsets.iter().map(|offset| *offset - offsets[0]).collect())
    };
    Ok(Arc::new(GenericListArray::<O>::try_new(
        item,
        offsets,
        values,
        list.nulls().cloned(),
    )?))
}

/// Checks that Date32 or timestamp values are in years 0000–9999, which DALI timestamps can
/// represent, see DALI 1.1, Section 3.3.3.
fn check_dali_date_range<T: DaliDateRange>(array: &PrimitiveArray<T>) -> DFResult<()> {
    // Nanosecond timestamps can't hold these years, all their values are in years 1677–2262
    let Some(range) = T::dali_date_range() else {
        return Ok(());
    };
    let outside = match (min(array), max(array)) {
        (Some(lowest), _) if !range.contains(&lowest) => lowest,
        (_, Some(highest)) if !range.contains(&highest) => highest,
        // All values are in these years, or null
        _ => return Ok(()),
    };
    let i = array
        .iter()
        .position(|value| value == Some(outside))
        .ok_or_else(|| DataFusionError::Internal(format!("No value {outside:?} in the array")))?;
    let formatted = ArrayFormatter::try_new(array, &FormatOptions::new())
        .and_then(|formatter| formatter.value(i).try_to_string())
        // Values beyond the range of Arrow formatting
        .unwrap_or_else(|_| format!("{:?} {} since 1970-01-01", array.value(i), T::UNIT));
    Err(VoConversionErrorKind::YearOutOfRange(formatted).into())
}

/// Arrow date or timestamp type, with values limited to years 0000–9999 in DALI timestamps.
trait DaliDateRange: ArrowPrimitiveType {
    /// Unit of the values, counted since 1970-01-01, e.g. "days".
    const UNIT: &'static str;

    /// Values of years 0000–9999, [None] if the type can't hold them.
    fn dali_date_range() -> Option<Range<Self::Native>>;
}

impl DaliDateRange for Date32Type {
    const UNIT: &'static str = "days";

    fn dali_date_range() -> Option<Range<i32>> {
        let first_day = |year| NaiveDate::from_ymd_opt(year, 1, 1).map(Self::from_naive_date);
        Some(first_day(0)?..first_day(10_000)?)
    }
}

impl DaliDateRange for TimestampSecondType {
    const UNIT: &'static str = "seconds";

    fn dali_date_range() -> Option<Range<i64>> {
        timestamp_dali_date_range::<Self>()
    }
}

impl DaliDateRange for TimestampMillisecondType {
    const UNIT: &'static str = "milliseconds";

    fn dali_date_range() -> Option<Range<i64>> {
        timestamp_dali_date_range::<Self>()
    }
}

impl DaliDateRange for TimestampMicrosecondType {
    const UNIT: &'static str = "microseconds";

    fn dali_date_range() -> Option<Range<i64>> {
        timestamp_dali_date_range::<Self>()
    }
}

impl DaliDateRange for TimestampNanosecondType {
    const UNIT: &'static str = "nanoseconds";

    fn dali_date_range() -> Option<Range<i64>> {
        timestamp_dali_date_range::<Self>()
    }
}

/// [DaliDateRange::dali_date_range] of a timestamp type.
fn timestamp_dali_date_range<T: ArrowTimestampType>() -> Option<Range<i64>> {
    let first_midnight = |year| {
        let midnight = NaiveDate::from_ymd_opt(year, 1, 1)?.and_time(NaiveTime::MIN);
        T::from_naive_datetime(midnight, None)
    };
    Some(first_midnight(0)?..first_midnight(10_000)?)
}

/// Casts an array, failing on values which can't be converted, instead of replacing them with
/// nulls or, for formatted temporal values, with error messages.
fn strict_cast(array: &ArrayRef, to: &DataType) -> DFResult<ArrayRef> {
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::new().with_display_error(false),
    };
    Ok(cast_with_options(array, to, &options)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Date32Array, Date64Array, Decimal128Array, DictionaryArray, Float16Array, Int8Array,
        Int16Array, Int32Array, Int64Array, LargeListArray, LargeListViewArray, ListArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
        UInt64Array,
    };
    use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{Field, Float32Type, Float64Type, Int16Type, Int32Type};
    use half::f16;

    fn strings(array: ArrayRef) -> Vec<Option<String>> {
        let storage = array.storage_array().unwrap();
        assert_eq!(storage.data_type(), &DataType::Utf8);
        storage
            .as_string::<i32>()
            .iter()
            .map(|value| value.map(String::from))
            .collect()
    }

    #[test]
    fn unchanged() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let storage = array.storage_array().unwrap();
        assert!(Arc::ptr_eq(&array, &storage));
    }

    #[test]
    fn widened() {
        let array: ArrayRef = Arc::new(Int8Array::from(vec![Some(-128), None]));
        let storage = array.storage_array().unwrap();
        assert_eq!(storage.data_type(), &DataType::Int16);
        assert_eq!(
            storage.as_primitive::<Int16Type>(),
            &Int16Array::from(vec![Some(-128), None])
        );

        let array: ArrayRef = Arc::new(Float16Array::from(vec![f16::from_f32(0.5)]));
        let storage = array.storage_array().unwrap();
        assert_eq!(storage.as_primitive::<Float32Type>().value(0), 0.5);
    }

    #[test]
    fn uint64_overflow() {
        let array: ArrayRef = Arc::new(UInt64Array::from(vec![i64::MAX as u64]));
        assert_eq!(array.storage_array().unwrap().data_type(), &DataType::Int64);

        // The error tells the largest value
        let array: ArrayRef = Arc::new(UInt64Array::from(vec![
            Some(i64::MAX as u64 + 1),
            None,
            Some(u64::MAX),
        ]));
        let error = array.storage_array().unwrap_err().to_string();
        assert!(error.contains("value 18446744073709551615"), "{error}");
    }

    #[test]
    fn list_slice() {
        // The first row, outside the slice, has a value above the maximum of VOTable long
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::UInt64, true)),
            OffsetBuffer::from_lengths([1, 2, 0]),
            Arc::new(UInt64Array::from(vec![u64::MAX, 1, 2])),
            Some(NullBuffer::from(vec![true, true, false])),
        ));
        let error = list.storage_array().unwrap_err().to_string();
        assert!(
            error.contains("array element 18446744073709551615"),
            "{error}"
        );

        let storage = list.slice(1, 2).storage_array().unwrap();
        assert_eq!(
            storage.as_list::<i32>(),
            &ListArray::new(
                Arc::new(Field::new_list_field(DataType::Int64, true)),
                OffsetBuffer::from_lengths([2, 0]),
                Arc::new(Int64Array::from(vec![1, 2])),
                Some(NullBuffer::from(vec![true, false])),
            )
        );
    }

    #[test]
    fn list_offsets_from_zero() {
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int8, true)),
            OffsetBuffer::from_lengths([1, 2]),
            Arc::new(Int8Array::from(vec![1, 2, 3])),
            None,
        ));
        let slice = list.slice(0, 1);
        let storage = slice.storage_array().unwrap();
        assert_eq!(
            storage.as_list::<i32>(),
            &ListArray::new(
                Arc::new(Field::new_list_field(DataType::Int16, true)),
                OffsetBuffer::from_lengths([1]),
                Arc::new(Int16Array::from(vec![1])),
                None,
            )
        );
        // The offsets buffer is shared, not copied
        let offsets_ptr = |array: &ArrayRef| array.as_list::<i32>().offsets().inner().as_ptr();
        assert_eq!(offsets_ptr(&storage), offsets_ptr(&slice));
    }

    #[test]
    fn large_list_view_slice() {
        // Rows out of order, the last one outside the slice
        let list: ArrayRef = Arc::new(LargeListViewArray::new(
            Arc::new(Field::new_list_field(DataType::Int8, true)),
            ScalarBuffer::from(vec![2, 0, 1]),
            ScalarBuffer::from(vec![1, 2, 1]),
            Arc::new(Int8Array::from(vec![Some(1), None, Some(-128)])),
            None,
        ));
        let storage = list.slice(0, 2).storage_array().unwrap();
        assert_eq!(
            storage.as_list::<i64>(),
            &LargeListArray::new(
                Arc::new(Field::new_list_field(DataType::Int16, true)),
                OffsetBuffer::from_lengths([1, 2]),
                Arc::new(Int16Array::from(vec![Some(-128), Some(1), None])),
                None,
            )
        );
    }

    #[test]
    fn large_list_slice() {
        let list: ArrayRef = Arc::new(LargeListArray::new(
            Arc::new(Field::new_list_field(DataType::Int8, true)),
            OffsetBuffer::from_lengths([1, 2]),
            Arc::new(Int8Array::from(vec![Some(1), None, Some(-128)])),
            None,
        ));
        let storage = list.slice(1, 1).storage_array().unwrap();
        assert_eq!(
            storage.as_list::<i64>(),
            &LargeListArray::new(
                Arc::new(Field::new_list_field(DataType::Int16, true)),
                OffsetBuffer::from_lengths([2]),
                Arc::new(Int16Array::from(vec![None, Some(-128)])),
                None,
            )
        );
    }

    #[test]
    fn fixed_size_list_slice() {
        let list: ArrayRef = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new_list_field(DataType::UInt16, false)),
            2,
            Arc::new(UInt16Array::from(vec![1, 2, 65_535, 4])),
            None,
        ));
        let storage = list.slice(1, 1).storage_array().unwrap();
        assert_eq!(
            storage.as_fixed_size_list(),
            &FixedSizeListArray::new(
                Arc::new(Field::new_list_field(DataType::Int32, false)),
                2,
                Arc::new(Int32Array::from(vec![65_535, 4])),
                None,
            )
        );
    }

    #[test]
    fn dictionary() {
        let array: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(1), None, Some(0)]),
                Arc::new(Date32Array::from(vec![0, 1])),
            )
            .unwrap(),
        );
        assert_eq!(
            strings(array),
            [Some("1970-01-02".into()), None, Some("1970-01-01".into())]
        );
    }

    #[test]
    fn dates() {
        let array: ArrayRef = Arc::new(Date32Array::from(vec![Some(20_000), None, Some(-1)]));
        assert_eq!(
            strings(array),
            [Some("2024-10-04".into()), None, Some("1969-12-31".into())]
        );

        // Date64 keeps the time of day of values which aren't whole days
        let array: ArrayRef = Arc::new(Date64Array::from(vec![86_400_000, 86_401_500]));
        assert_eq!(
            strings(array),
            [
                Some("1970-01-02T00:00:00".into()),
                Some("1970-01-02T00:00:01.500".into())
            ]
        );
    }

    /// Converts decimals to their float storage, checks the type and that the decimals are
    /// restored from the floats exactly.
    fn assert_decimals_round_trip(values: &[i128], precision: u8, scale: i8, expected: DataType) {
        let decimals = Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .unwrap();
        let array: ArrayRef = Arc::new(decimals);
        let storage = array.storage_array().unwrap();
        assert_eq!(storage.data_type(), &expected);
        let floats: Vec<f64> = match storage.data_type() {
            DataType::Float32 => storage
                .as_primitive::<Float32Type>()
                .values()
                .iter()
                .map(|&value| f64::from(value))
                .collect(),
            _ => storage.as_primitive::<Float64Type>().values().to_vec(),
        };
        for (&value, float) in values.iter().zip(floats) {
            let restored = if scale >= 0 {
                float * 10_f64.powi(scale.into())
            } else {
                float / 10_f64.powi((-scale).into())
            };
            assert_eq!(
                restored.round() as i128,
                value,
                "Decimal({precision}, {scale}) {value} -> {float:e}"
            );
        }
    }

    #[test]
    fn decimals_to_float32() {
        let values: Vec<i128> = (-999_999..=999_999).step_by(7).collect();
        for scale in [-32, 0, 3, 6] {
            assert_decimals_round_trip(&values, 6, scale, DataType::Float32);
        }
    }

    #[test]
    fn decimals_to_float64() {
        // Pseudo-random values with up to 15 digits, and the extremes
        let mut values = vec![0, 1, -1, 999_999_999_999_999, -999_999_999_999_999];
        let mut state: u64 = 1;
        for _ in 0..100_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let value = i128::from(state >> 11) % 1_000_000_000_000_000;
            values.push(if state & 1 == 0 { value } else { -value });
        }
        for scale in [0, 5, 15] {
            assert_decimals_round_trip(&values, 15, scale, DataType::Float64);
        }
    }

    /// Timestamps without time zone are written without "Z".
    #[test]
    fn timestamps() {
        let array: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(1), None]));
        assert_eq!(strings(array), [Some("1970-01-01T00:00:01".into()), None]);

        let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_500]));
        assert_eq!(strings(array), [Some("1970-01-01T00:00:01.500".into())]);

        // As many fraction digits as needed, whatever the unit is
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_000,
            1_000_000_001,
        ]));
        assert_eq!(
            strings(array),
            [
                Some("1970-01-01T00:00:01".into()),
                Some("1970-01-01T00:00:01.000000001".into())
            ]
        );
    }

    /// Timestamps with time zone are written in UTC with "Z", Arrow values are UTC whatever the
    /// time zone is.
    #[test]
    fn timestamps_with_time_zone() {
        for tz in ["+00:00", "+02:00", "UTC", "Europe/Paris"] {
            let array: ArrayRef =
                Arc::new(TimestampSecondArray::from(vec![Some(1), None]).with_timezone(tz));
            assert_eq!(
                strings(array),
                [Some("1970-01-01T00:00:01Z".into()), None],
                "{tz}"
            );
        }

        let array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![1_000_000_001]).with_timezone("+02:00"));
        assert_eq!(
            strings(array),
            [Some("1970-01-01T00:00:01.000000001Z".into())]
        );
    }

    /// Days and seconds of 0000-01-01 and 10000-01-01 since the Unix epoch.
    const DAY_0000: i32 = -719_528;
    const DAY_10000: i32 = 2_932_897;
    const SECOND_0000: i64 = -62_167_219_200;
    const SECOND_10000: i64 = 253_402_300_800;

    #[test]
    fn dali_years() {
        let array: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(DAY_0000),
            None,
            Some(DAY_10000 - 1),
        ]));
        assert_eq!(
            strings(array),
            [Some("0000-01-01".into()), None, Some("9999-12-31".into())]
        );

        let array: ArrayRef = Arc::new(TimestampSecondArray::from(vec![
            SECOND_0000,
            SECOND_10000 - 1,
        ]));
        assert_eq!(
            strings(array),
            [
                Some("0000-01-01T00:00:00".into()),
                Some("9999-12-31T23:59:59".into())
            ]
        );

        let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            SECOND_10000 * 1000 - 1,
        ]));
        assert_eq!(strings(array), [Some("9999-12-31T23:59:59.999".into())]);

        // Years are checked in UTC, which is written
        let array: ArrayRef =
            Arc::new(TimestampSecondArray::from(vec![SECOND_10000 - 3600]).with_timezone("+02:00"));
        assert_eq!(strings(array), [Some("9999-12-31T23:00:00Z".into())]);
    }

    /// The error tells the offending value, as Arrow formats it.
    #[test]
    fn outside_dali_years_value() {
        let array: ArrayRef = Arc::new(
            TimestampSecondArray::from(vec![Some(0), None, Some(SECOND_10000)])
                .with_timezone("+02:00"),
        );
        let error = array.storage_array().unwrap_err().to_string();
        assert!(error.contains("value +10000-01-01T00:00:00Z"), "{error}");

        let array: ArrayRef = Arc::new(Date32Array::from(vec![DAY_0000 - 1]));
        let error = array.storage_array().unwrap_err().to_string();
        assert!(error.contains("value -0001-12-31"), "{error}");

        // Beyond the years Arrow formats
        let array: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
        let error = array.storage_array().unwrap_err().to_string();
        assert!(
            error.contains("value 2147483647 days since 1970-01-01 outside"),
            "{error}"
        );
    }

    /// Values under nulls are arbitrary and not checked.
    #[test]
    fn outside_dali_years_under_null() {
        let array = TimestampSecondArray::new(
            ScalarBuffer::from(vec![0, SECOND_10000]),
            Some(NullBuffer::from(vec![true, false])),
        );
        assert_eq!(
            strings(Arc::new(array)),
            [Some("1970-01-01T00:00:00".into()), None]
        );
    }

    #[test]
    fn outside_dali_years() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Date32Array::from(vec![Some(0), None, Some(DAY_10000)])),
            Arc::new(Date32Array::from(vec![DAY_0000 - 1])),
            Arc::new(Date64Array::from(vec![SECOND_10000 * 1000])),
            Arc::new(Date64Array::from(vec![(SECOND_0000 - 1) * 1000])),
            Arc::new(TimestampSecondArray::from(vec![SECOND_10000])),
            Arc::new(TimestampSecondArray::from(vec![SECOND_0000 - 1])),
            Arc::new(TimestampMillisecondArray::from(vec![SECOND_10000 * 1000])),
            Arc::new(TimestampSecondArray::from(vec![SECOND_10000]).with_timezone("+02:00")),
        ];
        for array in arrays {
            let result = array.storage_array();
            assert!(
                matches!(&result, Err(e) if e.to_string().contains("outside years 0000-9999")),
                "{array:?}: {result:?}"
            );
        }
    }

    #[test]
    fn timestamp_out_of_range() {
        for tz in [None, Some("+02:00")] {
            let array = TimestampSecondArray::from(vec![i64::MAX]);
            let array: ArrayRef = match tz {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            };
            let result = array.storage_array();
            assert!(result.is_err(), "{tz:?}: {result:?}");
        }
    }
}
