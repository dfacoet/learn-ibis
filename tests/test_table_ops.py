"""
Tests for table operations.
"""

from datetime import date, datetime

import pyarrow as pa
import pytest

from learn_ibis import (
    get_connection,
    read_table_from_backend,
    write_table_to_backend,
)

# Test data cases
TEST_CASES = [
    # Simple table
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 32],
        "active": [True, False, True, True, False],
    },
    # Table with datetime and date columns
    {
        "date_col": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
        "datetime_col": [
            datetime(2023, 1, 1, 12, 0),
            datetime(2023, 1, 2, 14, 30),
            datetime(2023, 1, 3, 9, 15),
        ],
        "value": [10.5, 20.3, 15.7],
    },
    # Table with null values
    {
        "id": [1, 2, None, 4, 5],
        "name": ["Alice", None, "Charlie", "Diana", None],
        "score": [85.5, None, 92.0, None, 78.3],
        "active": [True, False, None, True, False],
    },
    # Table with different data types
    {
        "int_col": [1, 2, 3],
        "float_col": [1.1, 2.2, 3.3],
        "string_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "date_col": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
        "datetime_col": [
            datetime(2023, 1, 1, 12, 0),
            datetime(2023, 1, 2, 14, 30),
            datetime(2023, 1, 3, 9, 15),
        ],
    },
]


@pytest.mark.parametrize("data", TEST_CASES)
def test_write_read_table(backend, data):
    """Test writing and reading a table preserves data integrity."""
    original_table = pa.Table.from_pydict(data)
    connection = get_connection(backend)
    write_table_to_backend(original_table, "test_table", backend, connection)
    result_table = read_table_from_backend("test_table", backend, connection)
    assert result_table.to_pydict() == original_table.to_pydict()


@pytest.mark.parametrize("data", TEST_CASES)
def test_write_return_table(backend, data):
    """Test that write_table_to_backend returns a table that can be converted to PyArrow."""
    original_table = pa.Table.from_pydict(data)
    connection = get_connection(backend)
    ibis_table = write_table_to_backend(
        original_table, "test_table", backend, connection
    )
    result_table = ibis_table.to_pyarrow()
    assert result_table.to_pydict() == original_table.to_pydict()
