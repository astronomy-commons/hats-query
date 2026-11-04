use crate::arrow_votable::vo_field;
use datafusion::arrow::datatypes::{FieldRef, Fields, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use std::sync::Arc;
use votable::field::Field;

/// Metadata of the table.
pub(crate) struct TableMetadata {
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
pub(crate) struct ColumnMetadata {
    /// Arrow field, authoritative for the column type.
    pub(crate) arrow: FieldRef,
    /// VOTable FIELD, with unit, UCD, description, etc.
    pub(crate) vo: Field,
    /// Whether the column is indexed, so it is efficient to filter by,
    /// as reported in TAP_SCHEMA.columns and VOSI tables.
    pub(crate) indexed: bool,
    /// Whether the column is principal, i.e. among the columns to show by default,
    /// as reported in TAP_SCHEMA.columns and VOSI tables.
    pub(crate) principal: bool,
}

impl ColumnMetadata {
    /// Creates column metadata with the VOTable FIELD mapped from the Arrow field,
    /// neither indexed nor principal.
    pub(crate) fn new(arrow: FieldRef) -> DFResult<Self> {
        let vo = vo_field(&arrow).map_err(|e| e.context(format!("Column '{}'", arrow.name())))?;
        Ok(Self {
            arrow,
            vo,
            indexed: false,
            principal: false,
        })
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
            .columns
            .iter()
            .map(|column| (column.arrow.name().as_str(), column.vo.name.as_str()))
            .collect();
        assert_eq!(names, [("id", "id"), ("ra", "ra")]);
        assert!(
            metadata
                .columns
                .iter()
                .all(|column| !column.indexed && !column.principal)
        );
    }

    #[test]
    fn unrepresentable() {
        let schema = Schema::new(vec![ArrowField::new("nothing", DataType::Null, true)]);
        let Err(err) = TableMetadata::from_schema(&schema) else {
            panic!("expected an error");
        };
        assert!(err.to_string().contains("Column 'nothing'"), "{err}");
    }
}
