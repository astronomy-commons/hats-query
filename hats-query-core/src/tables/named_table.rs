//! Served tables with their names.

use crate::tables::{TableMetadata, TableSource, VoColumnMetadata};
use std::sync::Arc;

/// Served table: its qualified name, column metadata and origin of rows.
pub struct NamedTable {
    /// Qualified name, e.g. "gaia_dr3.gaia_source".
    name: String,
    /// Column metadata.
    metadata: Arc<TableMetadata>,
    /// Origin of the rows.
    source: TableSource,
}

impl NamedTable {
    /// Served table `name`, e.g. "schema.table", with the rows from `source`.
    pub fn new(name: String, metadata: Arc<TableMetadata>, source: TableSource) -> Self {
        Self {
            name,
            metadata,
            source,
        }
    }

    /// Qualified name, e.g. "gaia_dr3.gaia_source".
    pub fn name(&self) -> &str {
        &self.name
    }

    /// VO metadata of the columns VOTable can represent, in the table order.
    pub fn vo_columns(&self) -> impl Iterator<Item = &VoColumnMetadata> {
        self.metadata.vo_columns()
    }

    /// Origin of the rows.
    pub fn source(&self) -> &TableSource {
        &self.source
    }
}
