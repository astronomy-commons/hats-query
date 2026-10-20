from pathlib import Path

from hats_query.server.config import Config

EXAMPLE_CONFIG = Path(__file__).parents[3] / "hats-query.example.toml"


def test_example_config(monkeypatch) -> None:
    """Verify that the example config file loads"""
    # The example's mount sources do not exist here
    monkeypatch.setattr(Path, "is_dir", lambda self: True)
    Config.load(EXAMPLE_CONFIG)
