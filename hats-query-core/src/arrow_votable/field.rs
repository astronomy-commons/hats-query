use crate::arrow_votable::data_type::ArrowDataTypeVoExt as _;
use crate::arrow_votable::error::with_column;
use datafusion::arrow::datatypes::Field as ArrowField;
use datafusion::common::Result as DFResult;
use votable::field::Field;

/// VOTable representation of an Arrow field.
pub(crate) trait ArrowFieldVoExt {
    /// Returns the VOTable FIELD of this field, as used in VOTable output and
    /// TAP_SCHEMA.columns.
    ///
    /// Returns an error for types which can't be represented in VOTable, see
    /// [crate::arrow_votable::data_type::ArrowDataTypeVoExt::storage_type].
    fn vo_field(&self) -> DFResult<Field>;
}

impl ArrowFieldVoExt for ArrowField {
    fn vo_field(&self) -> DFResult<Field> {
        let data_type = self.data_type();
        let (datatype, arraysize) = data_type
            .vo_datatype()
            .map_err(|e| with_column(e, self.name()))?;
        let mut vo_field = Field::new(self.name(), datatype);
        vo_field.arraysize = arraysize;
        vo_field.xtype = data_type.xtype().map(String::from);
        Ok(vo_field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

    #[test]
    fn attributes() {
        let field = ArrowField::new("t", ArrowDataType::Date32, true)
            .vo_field()
            .unwrap();
        assert_eq!(field.name, "t");
        assert_eq!(field.datatype.to_string(), "char");
        assert_eq!(
            field.arraysize.map(|size| size.to_string()).as_deref(),
            Some("*")
        );
        assert_eq!(field.xtype.as_deref(), Some("timestamp"));
    }

    #[test]
    fn column_in_error() {
        let field = ArrowField::new("x", ArrowDataType::Time64(TimeUnit::Microsecond), true);
        let error = field.vo_field().unwrap_err();
        assert!(error.to_string().contains("Column 'x'"), "{error}");
    }
}
