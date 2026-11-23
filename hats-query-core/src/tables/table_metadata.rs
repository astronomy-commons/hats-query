use crate::adql::delimit_if_needed;
use crate::arrow_votable::{VoConversionError, vo_field};
use datafusion::arrow::datatypes::{FieldRef, Fields, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DFResult};
use std::borrow::Cow;
use std::sync::Arc;
use votable::desc::Description;
use votable::field::Field;

/// Metadata of the table.
pub struct TableMetadata {
    /// Columns, in the table order.
    pub(crate) columns: Vec<ColumnMetadata>,
}

impl TableMetadata {
    /// Creates metadata of the schema columns, see [ColumnMetadata::new].
    pub(crate) fn from_schema(schema: &Schema) -> DFResult<Self> {
        let columns = schema
            .fields()
            .iter()
            .cloned()
            .map(ColumnMetadata::new)
            .collect::<DFResult<_>>()?;
        Ok(Self { columns })
    }

    /// VO metadata of the columns VOTable can represent, in the table order.
    pub fn vo_columns(&self) -> impl Iterator<Item = &VoColumnMetadata> {
        self.columns.iter().filter_map(|column| column.vo.as_ref())
    }

    /// Arrow schema of the columns.
    pub(crate) fn schema(&self) -> SchemaRef {
        let fields: Fields = self
            .columns
            .iter()
            .map(|column| column.arrow.clone())
            .collect();
        Arc::new(Schema::new(fields))
    }
}

/// Metadata of the table column.
pub struct ColumnMetadata {
    /// Arrow field, authoritative for the column type.
    pub(crate) arrow: FieldRef,
    /// VO metadata, [None] if VOTable can't represent the column type.
    pub(crate) vo: Option<VoColumnMetadata>,
}

impl ColumnMetadata {
    /// Creates column metadata with the VO metadata of the Arrow field, see
    /// [VoColumnMetadata::new].
    pub(crate) fn new(arrow: FieldRef) -> DFResult<Self> {
        let vo = match VoColumnMetadata::new(&arrow) {
            Ok(vo) => Some(vo),
            Err(DataFusionError::External(err)) if err.is::<VoConversionError>() => {
                tracing::warn!(
                    "Column '{}' of type {} has no VOTable type, it is not listed in TAP_SCHEMA \
                    and VOSI tables",
                    arrow.name(),
                    arrow.data_type()
                );
                None
            }
            Err(err) => return Err(err.context(format!("Column '{}'", arrow.name()))),
        };
        Ok(Self { arrow, vo })
    }
}

/// VO metadata of the table column: its VOTable FIELD and flags.
pub struct VoColumnMetadata {
    /// VOTable FIELD, with unit, UCD, description, etc.
    pub(crate) field: Field,
    /// Whether the column is indexed, so it is efficient to filter by.
    pub(crate) indexed: bool,
    /// Whether the column is principal, i.e. among the columns to show by default.
    pub(crate) principal: bool,
}

impl VoColumnMetadata {
    /// Creates column metadata with the VOTable FIELD mapped from the Arrow field,
    /// neither indexed nor principal.
    ///
    /// Returns an error for types which can't be represented in VOTable, see [vo_field].
    fn new(arrow: &FieldRef) -> DFResult<Self> {
        Ok(Self {
            field: vo_field(arrow)?,
            indexed: false,
            principal: false,
        })
    }

    /// Name as an ADQL identifier, delimited if it isn't a regular one, e.g. "\"_healpix_29\"".
    pub fn adql_name(&self) -> Cow<'_, str> {
        delimit_if_needed(&self.field.name)
    }

    /// VOTable datatype, e.g. "double".
    pub fn datatype(&self) -> String {
        self.field.datatype.to_string()
    }

    /// VOTable arraysize, e.g. "*".
    pub fn arraysize(&self) -> Option<String> {
        self.field.arraysize.as_ref().map(ToString::to_string)
    }

    /// VOTable extended type, e.g. "timestamp".
    pub fn xtype(&self) -> Option<&str> {
        self.field.xtype.as_deref()
    }

    /// Human-readable summary.
    pub fn description(&self) -> Option<&str> {
        self.field
            .description
            .as_ref()
            .map(Description::get_content_unwrapped)
    }

    /// Data model reference.
    pub fn utype(&self) -> Option<&str> {
        self.field.utype.as_deref()
    }

    /// Physical unit, e.g. "deg".
    pub fn unit(&self) -> Option<&str> {
        self.field.unit.as_deref()
    }

    /// Unified Content Descriptor, e.g. "pos.eq.ra".
    pub fn ucd(&self) -> Option<&str> {
        self.field.ucd.as_deref()
    }

    /// Whether filtering by the column is efficient.
    pub fn indexed(&self) -> bool {
        self.indexed
    }

    /// Whether the column is among those to show by default.
    pub fn principal(&self) -> bool {
        self.principal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field as ArrowField};

    #[test]
    fn from_schema() {
        let schema = Schema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("ra", DataType::Float64, false),
        ]);
        let metadata = TableMetadata::from_schema(&schema).unwrap();
        let names: Vec<_> = metadata
            .vo_columns()
            .map(|column| column.field.name.as_str())
            .collect();
        assert_eq!(names, ["id", "ra"]);
        assert!(
            metadata
                .vo_columns()
                .all(|column| !column.indexed && !column.principal)
        );
    }

    #[test]
    fn unrepresentable() {
        let schema = Schema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("nothing", DataType::Null, true),
        ]);
        let metadata = TableMetadata::from_schema(&schema).unwrap();
        assert_eq!(*metadata.schema(), schema);
        let listed: Vec<_> = metadata
            .vo_columns()
            .map(|column| column.field.name.as_str())
            .collect();
        assert_eq!(listed, ["id"]);
    }
}
