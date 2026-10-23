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
