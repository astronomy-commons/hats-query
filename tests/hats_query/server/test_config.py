from pathlib import Path

from hats_query.server.config import Config

EXAMPLE_CONFIG = Path(__file__).parents[3] / "hats-query.example.toml"


def test_example_config() -> None:
    """Verify that the example config file loads"""
    Config.load(EXAMPLE_CONFIG)
