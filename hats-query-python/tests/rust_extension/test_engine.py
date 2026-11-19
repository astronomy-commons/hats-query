from pathlib import Path

import pytest
from hats_query._extension import Engine

# Test data of the HATS library, mounted at /hats
HATS_TEST_DATA = Path(__file__).parents[3] / "test-data" / "git-submodules" / "hats" / "tests" / "data"
# Served tables, by their server paths
TABLES = {
    "small_sky.object": "/hats/small_sky_o1_collection",
    "small_sky.source": "/hats/small_sky_source",
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
        async for batch in engine.query("SELECT table_name FROM TAP_SCHEMA.tables")
        for name in batch.column("table_name").to_pylist()
    ]
    assert set(table_names) == TABLES.keys() | {
        "TAP_SCHEMA.schemas",
        "TAP_SCHEMA.tables",
        "TAP_SCHEMA.columns",
        "TAP_SCHEMA.keys",
        "TAP_SCHEMA.key_columns",
    }


async def test_query_hats_table(engine) -> None:
    """Verify that a served HATS collection is queryable"""
    batches = [batch async for batch in engine.query("SELECT id, ra, dec FROM small_sky.object")]
    num_rows = sum(batch.num_rows for batch in batches)
    # hats_nrows of the collection's primary catalog
    assert num_rows == 131

    rows = [
        row
        async for batch in engine.query("SELECT id, ra, dec FROM small_sky.object WHERE id = 707")
        for row in batch.to_pylist()
    ]
    assert rows == [{"id": 707, "ra": 308.5, "dec": -69.5}]


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
