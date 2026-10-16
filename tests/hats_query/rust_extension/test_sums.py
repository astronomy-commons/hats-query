from hats_query import sum_as_string


def test_sum_as_string() -> None:
    """Verify the output of the `sum_as_string` function"""
    assert sum_as_string(38, 4) == "42"
