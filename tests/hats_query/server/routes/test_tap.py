from datetime import UTC, datetime
from xml.etree import ElementTree

from pyvo.dal.tap import IVOA_DATETIME_FORMAT


def test_availability(client, vosi_availability_schema) -> None:
    """Verify that /tap/availability is a valid VOSI availability document saying the service is up"""
    response = client.get("/tap/availability")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/xml")
    vosi_availability_schema.validate(response.text)
    availability = vosi_availability_schema.to_dict(response.text)
    assert availability["vosi:available"] is True
    up_since = datetime.strptime(availability["vosi:upSince"], IVOA_DATETIME_FORMAT)
    assert up_since.replace(tzinfo=UTC) <= datetime.now(UTC)
    assert availability["vosi:note"] == ["service is accepting queries"]


def test_capabilities(client, vosi_capabilities_schema) -> None:
    """Verify that /tap/capabilities is a valid VOSI capabilities document"""
    response = client.get("/tap/capabilities")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/xml")
    vosi_capabilities_schema.validate(response.text)


def test_tables(client, vosi_tables_schema) -> None:
    """Verify that /tap/tables is a valid VOSI tables document listing the configured and TAP_SCHEMA tables"""
    config = client.app.state.config
    response = client.get("/tap/tables")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/xml")
    vosi_tables_schema.validate(response.text)
    tableset = ElementTree.fromstring(response.text)
    assert {
        schema.findtext("name"): {table.findtext("name") for table in schema.findall("table")}
        for schema in tableset.findall("schema")
    } == {
        "small_sky": {table.name for table in config.tap.tables},
        "TAP_SCHEMA": {
            "TAP_SCHEMA.schemas",
            "TAP_SCHEMA.tables",
            "TAP_SCHEMA.columns",
            "TAP_SCHEMA.keys",
            "TAP_SCHEMA.key_columns",
        },
    }

    columns = {
        table.findtext("name"): {column.findtext("name"): column for column in table.findall("column")}
        for table in tableset.iter("table")
    }
    ra = columns["small_sky.object"]["ra"]
    assert ra.findtext("dataType") == "double"
    assert ra.get("std") is None
    indexed = {
        name
        for name, column in columns["small_sky.object"].items()
        if "indexed" in [flag.text for flag in column.findall("flag")]
    }
    assert indexed == {"id", '"_healpix_29"'}
    table_name = columns["TAP_SCHEMA.columns"]["table_name"]
    assert table_name.get("std") == "true"
    data_type = table_name.find("dataType")
    assert (data_type.text, data_type.get("arraysize")) == ("char", "*")


def test_examples(client) -> None:
    """Verify that /tap/examples lists the configured examples, parsed as XML the way pyvo does"""
    config = client.app.state.config
    response = client.get("/tap/examples")
    assert response.status_code == 200
    examples = ElementTree.fromstring(response.text).findall(".//*[@typeof='example']")
    assert [
        (
            example.find(".//*[@property='name']").text,
            example.find(".//*[@property='query']").text,
            [table.text for table in example.findall(".//*[@property='table']")],
        )
        for example in examples
    ] == [(example.name, example.query.strip(), example.tables) for example in config.tap.examples]
