"""Run the server: ``hats-query --config hats-query.toml``."""

import argparse
import os
from collections.abc import Sequence

import uvicorn
from pydantic_settings import CliSettingsSource

from hats_query.server import create_app
from hats_query.server.config import CONFIG_FILE_ENV_VAR, RESOLVED_CONFIG_ENV_VAR, Config


def main(args: Sequence[str] | None = None) -> None:
    """Parse the command line and the config, and run the server.

    Parameters
    ----------
    args : sequence of str, optional
        Command-line arguments, `sys.argv` by default.
    """
    parser = argparse.ArgumentParser(prog="hats-query", description="Serve HATS catalogs over TAP.")
    parser.add_argument(
        "-c",
        "--config",
        default=os.environ.get(CONFIG_FILE_ENV_VAR),
        help=f"TOML configuration file, ${CONFIG_FILE_ENV_VAR} by default",
    )
    cli = CliSettingsSource(Config, root_parser=parser)
    parsed = parser.parse_args(args)
    config = Config.load(parsed.config, _cli_settings_source=cli(parsed_args=parsed))

    run_kwargs = {"host": config.server.address, "port": config.server.port, "log_level": config.log.level}
    if config.server.developer_mode:
        os.environ[RESOLVED_CONFIG_ENV_VAR] = config.model_dump_json(by_alias=True)
        uvicorn.run("hats_query.server:create_app", factory=True, reload=True, **run_kwargs)
    else:
        uvicorn.run(create_app(config), **run_kwargs)


if __name__ == "__main__":
    main()
