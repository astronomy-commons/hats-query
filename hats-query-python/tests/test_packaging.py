import hats_query


def test_version():
    """Check to see that we can get the package version"""
    assert hats_query.__version__ is not None
