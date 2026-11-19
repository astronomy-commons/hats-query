from pathlib import Path

from hats_query._extension import build_engine

from hats_query.server.config import Config


async def test_build_engine(example_config_path) -> None:
    """Verify that an engine built from the example config reads a mounted file"""
    config = Config.load(example_config_path)
    engine = build_engine(config.mounts, config.tap.tables)
    properties = await engine.read_bytes("/hats/small_sky_o1_collection/collection.properties")
    # Compare to the file on disk, git may check it out with CRLF line endings
    (mount,) = config.mounts
    assert properties == (Path(mount.source) / "small_sky_o1_collection/collection.properties").read_bytes()
    assert properties.startswith(b"#HATS Collection")


async def test_query_tap_schema(example_config_path) -> None:
    """Verify that TAP_SCHEMA.tables lists the configured tables"""
    config = Config.load(example_config_path)
    engine = build_engine(config.mounts, config.tap.tables)
    table_names = [
        name
        async for batch in engine.query("SELECT table_name FROM TAP_SCHEMA.tables")
        for name in batch.column("table_name").to_pylist()
    ]
    assert set(table_names) == {table.name for table in config.tap.tables} | {
        "TAP_SCHEMA.schemas",
        "TAP_SCHEMA.tables",
        "TAP_SCHEMA.columns",
        "TAP_SCHEMA.keys",
        "TAP_SCHEMA.key_columns",
    }
