use crate::arrow_votable::field::ArrowFieldVoExt as _;
use crate::arrow_votable::record_batch::ArrowRecordBatchVoExt as _;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use itertools::Itertools as _;
use std::io::Write;
use votable::data::Data;
use votable::impls::mem::InMemTableDataRows;
use votable::info::Info;
use votable::resource::Resource;
use votable::table::Table;
use votable::votable::{VOTable, Version};

/// VOTable document of query results with a BINARY2 table.
///
/// The document follows DALI 1.1, Section 4.4, and TAP 1.1, Section 4.9. It is VOTable 1.5,
/// with strings stored as UTF-8 `char` arrays, which is a VOTable 1.6 feature.
pub(crate) fn record_batches_to_votable(
    schema: &Schema,
    batches: &[RecordBatch],
) -> DFResult<VOTable<InMemTableDataRows>> {
    let mut table = Table::new();
    for field in schema.fields() {
        table = table.push_field(field.vo_field()?);
    }
    let values = batches
        .iter()
        .map(|batch| batch.vo_rows())
        .flatten_ok()
        .collect::<DFResult<_>>()?;
    let data = Data::new_empty()
        .set_tabledata(InMemTableDataRows::new(values))
        .to_binary2()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let resource = Resource::new()
        .set_type("results")
        .push_info(Info::new("QUERY_STATUS", "OK"))
        .push_table(table.set_data(data));
    Ok(VOTable::new(Version::V1_5, resource))
}

/// Writes record batches as a VOTable XML document of query results with a BINARY2 table.
///
/// Serializing and writing are synchronous and CPU-heavy, async code should run it with
/// `tokio::task::spawn_blocking`.
pub fn write_votable(schema: &Schema, batches: &[RecordBatch], writer: impl Write) -> DFResult<()> {
    record_batches_to_votable(schema, batches)?
        .wrap()
        .to_ivoa_xml_writer(writer)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;
    use votable::data::DataElem;
    use votable::impls::VOTableValue as V;
    use votable::table::TableElem;
    use votable::votable::VOTableWrapper;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(ids: Vec<Option<i32>>, names: Vec<Option<&str>>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ];
        RecordBatch::try_new(schema(), columns).unwrap()
    }

    /// Serializes the document and parses it back with the votable crate, returns the rows of
    /// its table.
    fn round_trip_rows(votable: VOTable<InMemTableDataRows>) -> Vec<Vec<V>> {
        parsed_rows(&votable.wrap().to_ivoa_xml_bytes().unwrap())
    }

    /// Parses a VOTable XML document with the votable crate, returns the rows of its table.
    fn parsed_rows(bytes: &[u8]) -> Vec<Vec<V>> {
        let parsed = VOTableWrapper::<InMemTableDataRows>::from_ivoa_xml_bytes(bytes)
            .unwrap()
            .unwrap();
        let data = parsed.get_first_table().unwrap().data.as_ref().unwrap();
        match &data.data {
            DataElem::Binary2(binary2) => binary2.stream.content.as_ref().unwrap().rows.clone(),
            other => panic!("Not BINARY2: {other:?}"),
        }
    }

    #[test]
    fn round_trip() {
        let batches = [
            batch(vec![Some(1), None], vec![Some("ra"), Some("Ångström ✓ 🌌")]),
            batch(vec![Some(3)], vec![None]),
        ];
        let votable = record_batches_to_votable(&schema(), &batches).unwrap();
        assert_eq!(
            round_trip_rows(votable),
            [
                vec![V::Int(1), V::String("ra".into())],
                vec![V::Null, V::String("Ångström ✓ 🌌".into())],
                vec![V::Int(3), V::Null],
            ]
        );
    }

    #[test]
    fn structure() {
        let votable = record_batches_to_votable(&schema(), &[]).unwrap();
        assert_eq!(votable.version, Version::V1_5);

        let [resource] = votable.resources.as_slice() else {
            panic!("Expected a single resource");
        };
        assert_eq!(resource.type_.as_deref(), Some("results"));
        let infos: Vec<_> = resource
            .infos
            .iter()
            .map(|info| (info.name.as_str(), info.value.as_str()))
            .collect();
        assert_eq!(infos, [("QUERY_STATUS", "OK")]);

        let table = votable.get_first_table().unwrap();
        let fields: Vec<_> = table
            .elems
            .iter()
            .map(|elem| match elem {
                TableElem::Field(field) => (field.name.clone(), field.datatype.to_string()),
                other => panic!("Not a FIELD: {other:?}"),
            })
            .collect();
        assert_eq!(
            fields,
            [
                ("id".to_string(), "int".to_string()),
                ("name".to_string(), "char".to_string())
            ]
        );
        assert!(matches!(
            table.data.as_ref().unwrap().data,
            DataElem::Binary2(_)
        ));
    }

    #[test]
    fn no_rows() {
        let votable = record_batches_to_votable(&schema(), &[]).unwrap();
        assert!(round_trip_rows(votable).is_empty());
    }

    #[test]
    fn write() {
        let batches = [
            batch(vec![Some(1)], vec![Some("ra")]),
            batch(vec![None], vec![None]),
        ];
        let mut bytes = Vec::new();
        write_votable(&schema(), &batches, &mut bytes).unwrap();
        assert_eq!(
            parsed_rows(&bytes),
            [
                vec![V::Int(1), V::String("ra".into())],
                vec![V::Null, V::Null]
            ]
        );
    }

    #[test]
    fn unrepresentable_column() {
        let schema = Schema::new(vec![Field::new("t", DataType::Null, true)]);
        let error = record_batches_to_votable(&schema, &[]).unwrap_err();
        assert!(error.to_string().contains("Column 't'"), "{error}");
    }
}
