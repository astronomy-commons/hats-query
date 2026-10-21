from datetime import UTC, datetime

from pyvo.dal.tap import IVOA_DATETIME_FORMAT


def test_availability(client, vosi_availability_schema) -> None:
    """Verify that /tap/availability is a valid VOSI availability document saying the service is up"""
    response = client.get("/tap/availability")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/xml")
    vosi_availability_schema.validate(response.text)
    availability = vosi_availability_schema.to_dict(response.text)
    assert availability["vosi:available"] is True
    up_since = datetime.strptime(availability["vosi:upSince"], IVOA_DATETIME_FORMAT)
    assert up_since.replace(tzinfo=UTC) <= datetime.now(UTC)
    assert availability["vosi:note"] == ["service is accepting queries"]


def test_capabilities(client, vosi_capabilities_schema) -> None:
    """Verify that /tap/capabilities is a valid VOSI capabilities document"""
    response = client.get("/tap/capabilities")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/xml")
    vosi_capabilities_schema.validate(response.text)
