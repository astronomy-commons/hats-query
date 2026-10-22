import pytest
from pydantic import ValidationError

from hats_query.server.config import Config, MountConfig, TapTableConfig


def test_example_config(example_config_path) -> None:
    """Verify that the example config file loads"""
    Config.load(example_config_path)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/", "/"),
        ("/hats", "/hats"),
        ("/hats/", "/hats"),
        ("//hats//x/./", "/hats/x"),
    ],
)
def test_virtual_path_normalized(path, expected) -> None:
    """Verify that virtual paths drop repeated and trailing slashes and "." segments"""
    assert TapTableConfig(name="t", path=path).path == expected


@pytest.mark.parametrize("path", ["hats", "/hats/../x"])
def test_virtual_path_rejected(path) -> None:
    """Verify that relative virtual paths and ".." segments are rejected"""
    with pytest.raises(ValidationError):
        TapTableConfig(name="t", path=path)


def test_mount_local_source(tmp_path) -> None:
    """Verify that a local source becomes an absolute path to an existing directory"""
    assert MountConfig(path="/m", source=str(tmp_path)).source == str(tmp_path.resolve())


def test_mount_local_source_missing(tmp_path) -> None:
    """Verify that a missing local directory is rejected"""
    with pytest.raises(ValidationError):
        MountConfig(path="/m", source=str(tmp_path / "missing"))


def test_mount_file_url_rejected(tmp_path) -> None:
    """Verify that a local source must be a plain path, not a file: URL"""
    with pytest.raises(ValidationError):
        MountConfig(path="/m", source=tmp_path.as_uri())


def test_mount_remote_source_kept() -> None:
    """Verify that a remote source is neither resolved nor checked"""
    assert MountConfig(path="/m", source="s3://bucket/prefix").source == "s3://bucket/prefix"
