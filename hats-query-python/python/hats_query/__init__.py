from importlib.metadata import version

from ._extension import ParquetReader, read_parquet, sum_as_string
from .example_module import greetings, meaning

__version__ = version("hats-query")

__all__ = ["greetings", "meaning", "__version__", "sum_as_string", "read_parquet", "ParquetReader"]
