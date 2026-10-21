import pytest
from fastapi.testclient import TestClient

from hats_query.server import create_app


@pytest.fixture
def client() -> TestClient:
    """Test client for a fresh hats-query application"""
    return TestClient(create_app())
