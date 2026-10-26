from hats_query.server.config import Config


def test_example_config(example_config_path) -> None:
    """Verify that the example config file loads"""
    Config.load(example_config_path)
