"""Server configuration."""

import json
import os
import tomllib
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_PREFIX = "HATS_QUERY_"
"""Prefix of the environment variables."""

CONFIG_FILE_ENV_VAR = f"{ENV_PREFIX}CONFIG"
"""Environment variable with the config file path, overridden by ``--config``."""

RESOLVED_CONFIG_ENV_VAR = f"{ENV_PREFIX}RESOLVED_CONFIG"
"""Environment variable passing the resolved config to the auto-reloaded app, developer mode only."""


class ServerConfig(BaseModel):
    """The ``[server]`` section."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    address: str = "0.0.0.0"
    port: int = 80
    developer_mode: bool = False
    """Reload on code changes and return tracebacks in error responses."""
    contact: str | None = None
    """Who runs this deployment."""


class LogConfig(BaseModel):
    """The ``[log]`` section."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    level: Literal["critical", "error", "warning", "info", "debug", "trace"] = "info"


class MountConfig(BaseModel):
    """A ``[[mount]]``: the directory `source` served under the url prefix `path`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    source: Path

    @field_validator("source")
    @classmethod
    def _existing_directory(cls, source: Path) -> Path:
        source = source.resolve()
        if not source.is_dir():
            raise ValueError(f"{source} is not a directory")
        return source


class TapExampleConfig(BaseModel):
    """A ``[[tap.table.example]]``: a query offered at /tap/examples."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    query: str


class TapTableConfig(BaseModel):
    """A ``[[tap.table]]``: the HATS catalog at `path` published as the table `name`."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_by_name=True, validate_by_alias=True)

    name: str
    path: str
    examples: list[TapExampleConfig] = Field(default_factory=list, alias="example")


class TapConfig(BaseModel):
    """The ``[tap]`` section."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_by_name=True, validate_by_alias=True)

    tables: list[TapTableConfig] = Field(default_factory=list, alias="table")


class Config(BaseSettings):
    """The whole configuration.

    Construct it with `Config.load` to read a config file, or directly to
    use the environment and the defaults only.
    """

    model_config = SettingsConfigDict(
        extra="forbid",
        frozen=True,
        validate_by_name=True,
        validate_by_alias=True,
        env_prefix=ENV_PREFIX,
        env_nested_delimiter="__",
    )

    server: ServerConfig = ServerConfig()
    log: LogConfig = LogConfig()
    mounts: list[MountConfig] = Field(default_factory=list, alias="mount")
    tap: TapConfig = TapConfig()

    @classmethod
    def settings_customise_sources(
        cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
    ):
        """Make the config file, which `load` passes as keyword arguments, the lowest priority."""
        return env_settings, dotenv_settings, file_secret_settings, init_settings

    @classmethod
    def load(cls, config_file: str | os.PathLike | None = None, **kwargs) -> Self:
        """Read the config file, if given, under the environment and the command line.

        Parameters
        ----------
        config_file : str or os.PathLike, optional
            The TOML file. A relative mount `source` in it is resolved against
            the file's directory.
        **kwargs
            Passed to `Config`, e.g. ``_cli_settings_source``.

        Returns
        -------
        Config
            The resolved configuration.
        """
        if config_file is None:
            return cls(**kwargs)
        config_file = Path(config_file)
        with open(config_file, "rb") as f:
            data = tomllib.load(f)
        # Resolve relative mount sources against the config file's directory.
        for mount in data.get("mount", []):
            if isinstance(mount.get("source"), str):
                mount["source"] = config_file.parent / mount["source"]
        return cls(**data, **kwargs)

    @classmethod
    def from_env(cls) -> Self:
        """Take the config passed in developer mode, or load the file named by ``$HATS_QUERY_CONFIG``.

        Returns
        -------
        Config
            The resolved configuration.
        """
        resolved = os.environ.get(RESOLVED_CONFIG_ENV_VAR)
        if resolved is None:
            return cls.load(os.environ.get(CONFIG_FILE_ENV_VAR))
        return cls.model_validate(json.loads(resolved))
