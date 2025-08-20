import os

import ibis
import pyarrow as pa
from postgres_jsonb_example import create_jsonb_table

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if DB_CONNECTION_STRING is None:
    raise ValueError(
        "DB_CONNECTION_STRING environment variable not set. Run 'source scripts/start_db.sh' first."
    )

con = ibis.connect(DB_CONNECTION_STRING)
table_name = create_jsonb_table(con)


def run_query(
    con,
    table_name: str,
    query_func,
) -> pa.Table:
    """
    Run a deferred Ibis query function on a table and return result as PyArrow table.

    Args:
        con: Ibis connection to the database
        table_name: Name of the table to query
        query_func: Function that takes a table and returns a query expression

    Returns:
        PyArrow table with query results
    """
    # Get the actual table
    table = con.table(table_name)

    # Apply the query function to the actual table
    query = query_func(table)

    # Execute the query
    result = query.to_pyarrow()
    return result


def build_query(table):
    """
    Create a deferred Ibis query function that filters non-null JSONB values.

    Returns:
        Function that takes a table and returns a query expression
    """
    return table.mutate(
        {"ord_col": ibis.cases((ibis._["string_col"] == "A", 1), else_=0)}
    )


# Run the query
result_table = run_query(con, table_name, build_query)
print("Query result (PyArrow table):")
print(result_table)
