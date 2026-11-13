from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def example_config_path() -> Path:
    """The example config file, hats-query.example.toml"""
    return Path(__file__).parents[2] / "hats-query.example.toml"
