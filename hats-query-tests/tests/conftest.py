import subprocess
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import httpx2
import pytest
import xmlschema

IVOA_SCHEMAS_DIR = Path(__file__).parents[2] / "test-data" / "ivoa-schemas"
# Imported namespaces from local files, not their remote schemaLocation
IVOA_SCHEMA_LOCATIONS = {
    "http://www.ivoa.net/xml/VOResource/v1.0": str(IVOA_SCHEMAS_DIR / "VOResource-v1.3.xsd"),
    "http://www.ivoa.net/xml/VODataService/v1.1": str(IVOA_SCHEMAS_DIR / "VODataService-v1.3.xsd"),
    "http://www.ivoa.net/xml/TAPRegExt/v1.0": str(IVOA_SCHEMAS_DIR / "TAPRegExt-v1.1.xsd"),
    "http://www.ivoa.net/xml/STC/stc-v1.30.xsd": str(IVOA_SCHEMAS_DIR / "stc-stub.xsd"),
}


@pytest.fixture(scope="session")
def example_config_path() -> Path:
    """The example config file, hats-query.example.toml"""
    return Path(__file__).parents[2] / "hats-query.example.toml"


@pytest.fixture(scope="session")
def vosi_availability_schema() -> xmlschema.XMLSchema:
    """VOSI availability XML schema"""
    return xmlschema.XMLSchema(IVOA_SCHEMAS_DIR / "VOSIAvailability-v1.0.xsd")


@pytest.fixture(scope="session")
def vosi_capabilities_schema() -> xmlschema.XMLSchema:
    """VOSI capabilities XML schema, with the TAPRegExt capability type"""
    return xmlschema.XMLSchema(
        str(IVOA_SCHEMAS_DIR / "VOSICapabilities-v1.0.xsd"),
        locations=IVOA_SCHEMA_LOCATIONS,
        allow="local",
    )


@pytest.fixture(scope="session")
def vosi_tables_schema() -> xmlschema.XMLSchema:
    """VOSI tables XML schema"""
    return xmlschema.XMLSchema(
        str(IVOA_SCHEMAS_DIR / "VOSITables-v1.1.xsd"),
        locations=IVOA_SCHEMA_LOCATIONS,
        allow="local",
    )


@pytest.fixture(scope="session")
def server_command() -> list[str]:
    """Command running the hats-query server under test, without arguments"""
    # Build upfront, so the server's startup timeout doesn't include compilation
    subprocess.run(["cargo", "build", "--quiet", "--package", "hats-query"], check=True)
    return ["cargo", "run", "--quiet", "--package", "hats-query", "--"]


@pytest.fixture(scope="session")
def live_server_url(server_command, example_config_path) -> Iterator[str]:
    """Base URL of a hats-query server running in a subprocess with the example config"""
    # The port of hats-query.example.toml
    url = "http://127.0.0.1:8321"
    process = subprocess.Popen(
        [
            *server_command,
            "--config",
            str(example_config_path),
        ]
    )
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError(f"Live server exited with code {process.returncode}")
        try:
            urllib.request.urlopen(f"{url}/doc")
            break
        except OSError:
            time.sleep(0.1)
    else:
        process.kill()
        process.wait()
        raise RuntimeError("Failed to start live server")
    yield url

    # Wait up to 10 seconds for a graceful exit, then kill and fail
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise RuntimeError("Live server did not shut down on SIGTERM") from None


@pytest.fixture
def client(live_server_url) -> Iterator[httpx2.Client]:
    """HTTP client of the live server"""
    with httpx2.Client(base_url=live_server_url) as client:
        yield client
