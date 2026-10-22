"""Server configuration."""

import json
import os
import tomllib
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_PREFIX = "HATS_QUERY_"
"""Prefix of the environment variables."""

CONFIG_FILE_ENV_VAR = f"{ENV_PREFIX}CONFIG"
"""Environment variable with the config file path, overridden by ``--config``."""

RESOLVED_CONFIG_ENV_VAR = f"{ENV_PREFIX}RESOLVED_CONFIG"
"""Environment variable passing the resolved config to the auto-reloaded app, developer mode only."""


def _absolute_virtual_path(value: str) -> str:
    """Require an absolute path without ``..`` segments and fold the POSIX ``//`` root into ``/``."""
    path = PurePosixPath(value)
    if not path.is_absolute():
        raise ValueError(f"{path} must start with '/'")
    if ".." in path.parts:
        raise ValueError(f"{path} must not have '..' segments")
    if path.root == "//":
        path = PurePosixPath("/", *path.parts[1:])
    return str(path)


VirtualPath = Annotated[str, AfterValidator(_absolute_virtual_path)]
"""An absolute path in the mount namespace, normalized without touching any filesystem."""


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

    path: VirtualPath
    source: str
    """A local directory, or a ``scheme://`` URL of a remote one."""

    @field_validator("source")
    @classmethod
    def _existing_local_directory(cls, source: str) -> str:
        """Turn a local source into an absolute path to an existing directory, keep a remote one as is."""
        scheme = urlsplit(source).scheme
        if scheme == "file":
            raise ValueError(f"{source}: give a local source as a plain path, not a file: URL")
        if scheme:
            return source
        resolved = Path(source).resolve()
        if not resolved.is_dir():
            raise ValueError(f"{resolved} is not a directory")
        return str(resolved)


class TapTableConfig(BaseModel):
    """A ``[[tap.table]]``: the HATS catalog at `path` published as the table `name`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    path: VirtualPath


class TapExampleConfig(BaseModel):
    """A ``[[tap.example]]``: a query offered at /tap/examples."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    query: str
    tables: list[str] = Field(default_factory=list)
    """Tables the query is about, out of the ``[[tap.table]]`` names."""


class TapConfig(BaseModel):
    """The ``[tap]`` section."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_by_name=True, validate_by_alias=True)

    tables: list[TapTableConfig] = Field(alias="table", min_length=1)
    examples: list[TapExampleConfig] = Field(default_factory=list, alias="example")

    @model_validator(mode="after")
    def _examples_name_known_tables(self) -> Self:
        known = {table.name for table in self.tables}
        for example in self.examples:
            unknown = [table for table in example.tables if table not in known]
            if unknown:
                raise ValueError(f"example {example.name!r} names unknown tables: {', '.join(unknown)}")
        return self


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
    tap: TapConfig

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
        # Resolve relative local mount sources against the config file's directory, URLs are kept.
        for mount in data.get("mount", []):
            # A missing or non-string source is skipped here and reported by MountConfig validation
            source = mount.get("source")
            if isinstance(source, str) and not urlsplit(source).scheme:
                mount["source"] = str(config_file.parent / source)
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
