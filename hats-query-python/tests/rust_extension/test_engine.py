import io
import warnings
from pathlib import Path

import numpy as np
import pytest
from astropy.io.votable import parse
from astropy.io.votable.tree import VOTableFile
from astropy.table import Table, vstack
from astropy.utils.exceptions import AstropyUserWarning
from hats_query._extension import Engine

# Test data of the HATS library, mounted at /hats
HATS_TEST_DATA = Path(__file__).parents[3] / "test-data" / "git-submodules" / "hats" / "tests" / "data"
# Served tables, by their server paths
TABLES = {
    "small_sky.object": "/hats/small_sky_o1_collection",
    "small_sky.source": "/hats/small_sky_source",
    # With "lc" struct column, which VOTable can't represent
    "small_sky.nested": "/hats/small_sky_nested",
}


@pytest.fixture
def engine() -> Engine:
    """Engine serving the TABLES of the HATS test data"""
    return Engine(mounts={"/hats": str(HATS_TEST_DATA)}, tables=TABLES)


async def test_read_bytes(engine) -> None:
    """Verify that the engine reads a mounted file"""
    properties = await engine.read_bytes("/hats/small_sky_o1_collection/collection.properties")
    # Compare to the file on disk, git may check it out with CRLF line endings
    assert properties == (HATS_TEST_DATA / "small_sky_o1_collection/collection.properties").read_bytes()
    assert properties.startswith(b"#HATS Collection")


async def test_query_tap_schema(engine) -> None:
    """Verify that TAP_SCHEMA.tables lists the served tables"""
    table_names = [
        name
        async for batch in engine.async_query("SELECT table_name FROM TAP_SCHEMA.tables")
        for name in batch.column("table_name").to_pylist()
    ]
    assert set(table_names) == TABLES.keys() | {
        "TAP_SCHEMA.schemas",
        "TAP_SCHEMA.tables",
        "TAP_SCHEMA.columns",
        "TAP_SCHEMA.keys",
        "TAP_SCHEMA.key_columns",
    }


def test_query_hats_table(engine) -> None:
    """Verify that a served HATS collection is queryable"""
    table = engine.query("SELECT id, ra, dec FROM small_sky.object")
    # hats_nrows of the collection's primary catalog
    assert table.num_rows == 131

    table = engine.query("SELECT id, ra, dec FROM small_sky.object WHERE id = 707")
    assert table.to_pylist() == [{"id": 707, "ra": 308.5, "dec": -69.5}]


def test_query_unknown_format(engine) -> None:
    """Verify that an unknown result format is rejected"""
    with pytest.raises(ValueError, match="Unknown format 'csv'"):
        engine.query("SELECT id FROM small_sky.object", format="csv")


def query_votable(engine: Engine, adql: str) -> VOTableFile:
    """Runs the query, parses its VOTable results with astropy, failing on any VOTable issue"""
    document = engine.query(adql, format="votable")
    assert isinstance(document, bytes)
    return parse(io.BytesIO(document), verify="exception")


@pytest.mark.parametrize(
    ("table_name", "catalog", "key"),
    [
        ("small_sky.object", "small_sky_o1_collection/small_sky_order1", "id"),
        ("small_sky.source", "small_sky_source", "source_id"),
    ],
)
def test_query_votable_matches_parquet(engine, table_name, catalog, key) -> None:
    """Verify that VOTable results of a whole table equal its HATS parquet files read by astropy"""
    paths = sorted((HATS_TEST_DATA / catalog / "dataset").glob("Norder=*/Dir=*/Npix=*.parquet"))
    with warnings.catch_warnings():
        # No astropy string widths in parquet metadata
        warnings.filterwarnings("ignore", "No table::len::", AstropyUserWarning)
        expected = vstack([Table.read(path, format="parquet") for path in paths])
    expected.sort(["_healpix_29", key])

    votable = query_votable(engine, f'SELECT * FROM {table_name} ORDER BY "_healpix_29", {key}')
    actual = votable.get_first_table().to_table()

    assert actual.colnames == expected.colnames
    assert len(actual) == len(expected)
    for name in expected.colnames:
        np.testing.assert_array_equal(
            np.ma.getmaskarray(actual[name]), np.ma.getmaskarray(expected[name]), err_msg=f"{name} mask"
        )
        # astropy reads variable-length VOTable strings as objects
        if expected[name].dtype.kind != "U":
            assert actual[name].dtype == expected[name].dtype, name
        np.testing.assert_array_equal(actual[name], expected[name], err_msg=name)


def test_query_column_without_votable_type(engine) -> None:
    """Verify that a column of a type VOTable can't represent is queryable, but not in TAP_SCHEMA"""
    columns = engine.query("SELECT column_name FROM TAP_SCHEMA.columns WHERE table_name = 'small_sky.nested'")
    column_names = columns.column("column_name").to_pylist()
    assert "id" in column_names
    assert "lc" not in column_names

    # hats_nrows of the catalog
    assert engine.query("SELECT lc FROM small_sky.nested").num_rows == 131


@pytest.mark.parametrize(
    ("mounts", "tables", "match"),
    [
        ({"hats": "."}, {}, "must start with '/'"),
        ({"/hats": "missing"}, {}, "missing"),
        ({"/hats": "."}, {"t": "/hats/t"}, "must be qualified with a schema"),
        ({"/hats": "."}, {"s.t": "/hats/../t"}, r"\.\."),
    ],
)
def test_invalid_engine_arguments(mounts, tables, match) -> None:
    """Verify that invalid mounts and tables are rejected when the engine is built"""
    with pytest.raises(ValueError, match=match):
        Engine(mounts=mounts, tables=tables)
