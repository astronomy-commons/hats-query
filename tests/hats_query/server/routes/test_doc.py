def test_doc(client) -> None:
    """Verify that /doc is an HTML page pointing to the TAP service"""
    response = client.get("/doc")
    assert '<a href="/tap">/tap</a>' in response.text
