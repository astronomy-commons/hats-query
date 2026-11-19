//! Served tables grouped by schema, like a VODataService tableset.

use crate::adql::schema_of;
use crate::tables::NamedTable;
use datafusion::common::Result as DFResult;
use std::collections::BTreeMap;

/// Tables grouped by DataFusion schema, e.g. "schema.table".
pub struct TableSet {
    /// Tables by schema name.
    schemas: BTreeMap<String, Vec<NamedTable>>,
}

impl TableSet {
    /// Groups the tables by schema, keeping the order of the tables within each schema.
    ///
    /// Returns an error if a table name has no schema part.
    pub fn new(tables: impl IntoIterator<Item = NamedTable>) -> DFResult<Self> {
        let mut schemas = BTreeMap::new();
        for table in tables {
            schemas
                .entry(schema_of(table.name())?)
                .or_insert_with(Vec::new)
                .push(table);
        }
        Ok(Self { schemas })
    }

    /// (schema name, tables) pairs, sorted by schema name.
    pub fn schemas(&self) -> impl Iterator<Item = (&str, &[NamedTable])> {
        self.schemas
            .iter()
            .map(|(name, tables)| (name.as_str(), tables.as_slice()))
    }

    /// All tables, schema by schema.
    pub fn tables(&self) -> impl Iterator<Item = &NamedTable> {
        self.schemas.values().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::{TableMetadata, TableSource};
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    #[test]
    fn grouped_by_schema() {
        let empty = Arc::new(TableMetadata::from_schema(&Schema::empty()).unwrap());
        let table_set =
            TableSet::new(["b.x", "a.y", "b.z"].map(|name| {
                NamedTable::new(name.to_owned(), empty.clone(), TableSource::TapSchema)
            }))
            .unwrap();
        let schemas: Vec<_> = table_set
            .schemas()
            .map(|(name, tables)| {
                (
                    name,
                    tables.iter().map(NamedTable::name).collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(schemas, [("a", vec!["a.y"]), ("b", vec!["b.x", "b.z"])]);
        let tables: Vec<_> = table_set.tables().map(NamedTable::name).collect();
        assert_eq!(tables, ["a.y", "b.x", "b.z"]);
    }
}
