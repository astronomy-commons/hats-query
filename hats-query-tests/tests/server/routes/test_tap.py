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
    # Valid VOSI tables document
    response = client.get("/tap/tables")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/xml")
    vosi_tables_schema.validate(response.text)

    # The tables of hats-query.example.toml and TAP_SCHEMA
    document = ElementTree.fromstring(response.text)
    tables = {table.findtext("name"): table for table in document.iter("table")}
    assert set(tables) == {
        "small_sky.object",
        "small_sky.source",
        "small_sky.nested",
        "TAP_SCHEMA.schemas",
        "TAP_SCHEMA.tables",
        "TAP_SCHEMA.columns",
        "TAP_SCHEMA.keys",
        "TAP_SCHEMA.key_columns",
    }

    # Column metadata, checked in detail by the TAP_SCHEMA unit tests of hats-query-core
    columns = {column.findtext("name"): column for column in tables["small_sky.object"].iter("column")}
    ra = columns["ra"]
    assert ra.findtext("dataType") == "double"
    assert ra.findtext("unit") == "deg"
    assert ra.findtext("ucd") == "pos.eq.ra;meta.main"
    assert ra.findtext("flag") == "principal"

    # No "lc" struct column, which VOTable can't represent
    nested_columns = [column.findtext("name") for column in tables["small_sky.nested"].iter("column")]
    assert "id" in nested_columns
    assert "lc" not in nested_columns


def test_examples(client) -> None:
    """Verify that /tap/examples lists the configured examples, parsed as XML the way pyvo does"""
    response = client.get("/tap/examples")
    assert response.status_code == 200
    document = ElementTree.fromstring(response.text)
    names = [element.text for element in document.findall(".//*[@property='name']")]
    # The examples of hats-query.example.toml
    assert names == ["Objects in a cone", "Object by id"]
