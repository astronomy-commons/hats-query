import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest
import xmlschema
from fastapi.testclient import TestClient

from hats_query.server import create_app
from hats_query.server.config import Config

IVOA_SCHEMAS_DIR = Path(__file__).parent.parent / "data" / "ivoa-schemas"
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


@pytest.fixture
def client(example_config_path) -> TestClient:
    """Test client for a fresh hats-query application with the example config"""
    return TestClient(create_app(Config.load(example_config_path)))


@pytest.fixture(scope="module")
def live_server_url(example_config_path):
    """Base URL of a hats-query server running in a subprocess with the example config"""
    port = "8321"
    url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "hats_query",
            "--config",
            str(example_config_path),
            "--server.port",
            str(port),
            "--log.level",
            "warning",
        ]
    )
    for _ in range(100):
        try:
            urllib.request.urlopen(f"{url}/doc")
            break
        except OSError:
            time.sleep(0.1)
    else:
        process.terminate()
        raise RuntimeError("Failed to start live server")
    yield url
    process.terminate()
