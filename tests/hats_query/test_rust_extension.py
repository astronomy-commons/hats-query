from hats_query import sum_as_string, async_read_parquet
import asyncio

def test_sum_as_string() -> None:
    """Verify the output of the `sum_as_string` function"""
    assert sum_as_string(38, 4) == "42"

def test_read_parquet() -> None:
    results = asyncio.run(async_read_parquet("/home/delucchi/git/properties/hats/tests/data/small_sky/dataset/Norder=0/Dir=0/Npix=11.parquet"))
    assert len(results) == 1
    assert len(results[0]) == 131