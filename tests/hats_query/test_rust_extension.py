import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hats_query import async_read_parquet, sum_as_string


def test_sum_as_string() -> None:
    """Verify the output of the `sum_as_string` function"""
    assert sum_as_string(38, 4) == "42"


async def test_read_parquet(tmp_path) -> None:
    """Verify the output of the `read_parquet` function"""
    test_path = tmp_path / "test.parquet"
    pq.write_table(
        pa.Table.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]}),
        test_path,
        row_group_size=2,
    )
    results = await async_read_parquet(str(test_path))
    assert len(results) == 2
    assert (len(results[0]), len(results[1])) == (2, 1)


async def test_read_parquet_missing_file(tmp_path) -> None:
    """Verify that reading a non-existing file raises a Python exception"""
    with pytest.raises(ValueError, match="No files found"):
        await async_read_parquet(str(tmp_path / "missing.parquet"))
