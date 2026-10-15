import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hats_query import ParquetReader, read_parquet, sum_as_string


def test_sum_as_string() -> None:
    """Verify the output of the `sum_as_string` function"""
    assert sum_as_string(38, 4) == "42"


@pytest.mark.asyncio
async def test_read_parquet(tmp_path) -> None:
    """Verify the output of the `read_parquet` function"""
    test_path = tmp_path / "test.parquet"
    pq.write_table(
        pa.Table.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]}),
        test_path,
        row_group_size=2,
    )
    results = await read_parquet(str(test_path))
    assert len(results) == 2
    assert (len(results[0]), len(results[1])) == (2, 1)


@pytest.mark.asyncio
async def test_read_parquet_missing_file(tmp_path) -> None:
    """Verify that reading a non-existing file raises a Python exception"""
    with pytest.raises(ValueError, match="No files found"):
        await read_parquet(str(tmp_path / "missing.parquet"))


async def test_parquet_reader(tmp_path) -> None:
    """Verify `ParquetReader` async iterator"""
    a = list(range(99))
    b = ["x", "y", "z"] * 33

    test_path = tmp_path / "test.parquet"
    pq.write_table(
        pa.Table.from_pydict({"a": a, "b": b}),
        test_path,
        row_group_size=50,
    )

    reader = ParquetReader(
        test_path,
        sql="SELECT * FROM t WHERE a % 2 = 0 and b IN ('x', 'y')",
        table_name="t",
    )

    i = 0
    output_a = []
    async for batch in reader:
        i += 1
        assert isinstance(batch, pa.RecordBatch)
        output_a.extend(batch["a"].to_pylist())
    # Could be larger than number of row groups because of parallel parsing by DataFusion
    assert i >= 2
    assert output_a == [a_ for a_ in a if a_ % 2 == 0 and b[a_] in ("x", "y")]
