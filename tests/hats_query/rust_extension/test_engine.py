from hats_query._extension import build_engine

from hats_query.server.config import Config


async def test_build_engine(example_config_path) -> None:
    """Verify that an engine built from the example config reads a mounted file"""
    engine = build_engine(Config.load(example_config_path).mounts)
    properties = await engine.read_bytes("/hats/small_sky_o1_collection/collection.properties")
    assert properties.startswith(b"#HATS Collection\n")
