import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest
import xmlschema
from fastapi.testclient import TestClient

from hats_query.server import create_app

IVOA_SCHEMAS_DIR = Path(__file__).parent.parent / "data" / "ivoa_schemas"


@pytest.fixture(scope="session")
def vosi_availability_schema() -> xmlschema.XMLSchema:
    """VOSI availability XML schema"""
    return xmlschema.XMLSchema(IVOA_SCHEMAS_DIR / "VOSIAvailability-v1.0.xsd")


@pytest.fixture
def client() -> TestClient:
    """Test client for a fresh hats-query application"""
    return TestClient(create_app())


@pytest.fixture(scope="module")
def live_server_url():
    """Base URL of a hats-query server running in a subprocess"""
    port = "8321"
    url = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [sys.executable, "-m", "hats_query", "--server.port", str(port), "--log.level", "warning"]
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
