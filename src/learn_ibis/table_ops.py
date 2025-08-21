"""
Table operations for learn-ibis module.
"""

from typing import Union

import ibis
import pandas as pd
import pyarrow as pa

from .config import get_db_settings


def write_table_to_backend(
    table: Union[pa.Table, pd.DataFrame],
    table_name: str,
    backend: str = "duckdb",
    connection=None,
):
    """
    Write a PyArrow table or pandas DataFrame to a backend and return the ibis table.

    Args:
        table: PyArrow table or pandas DataFrame to write
        table_name: Name for the table in the backend
        backend: Backend to use ("duckdb" or "postgres")
        connection: Existing ibis connection (optional)

    Returns:
        ibis table expression
    """
    if connection is None:
        connection = _get_connection(backend)

    # Convert to PyArrow if it's a pandas DataFrame
    if isinstance(table, pd.DataFrame):
        table = pa.Table.from_pandas(table)

    # Create table in backend with overwrite=True to handle existing tables
    ibis_table = connection.create_table(table_name, table, overwrite=True)

    return ibis_table


def read_table_from_backend(
    table_name: str,
    backend: str = "duckdb",
    connection=None,
) -> pa.Table:
    """
    Read a table from a backend and return as PyArrow table.

    Args:
        table_name: Name of the table to read
        backend: Backend to use ("duckdb" or "postgres")
        connection: Existing ibis connection (optional)

    Returns:
        PyArrow table
    """
    if connection is None:
        connection = _get_connection(backend)

    # Get the table
    table_expr = connection.table(table_name)

    # Convert to PyArrow table
    return table_expr.to_pyarrow()


def get_connection(backend: str):
    """Get an ibis connection for the specified backend."""
    if backend == "duckdb":
        return ibis.duckdb.connect(":memory:")
    elif backend == "postgres":
        settings = get_db_settings()
        return ibis.postgres.connect(**settings.get_ibis_connection_params())
    else:
        raise ValueError(f"Unsupported backend: {backend}")


def _get_connection(backend: str):
    """Internal function for getting connections."""
    return get_connection(backend)
