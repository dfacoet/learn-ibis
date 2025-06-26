from typing import Literal

import ibis
import pandas as pd
import pyarrow
import pytest
import sqlalchemy


@pytest.fixture
def table_name() -> str:
    return "_test_table"


@pytest.fixture
def values() -> list[float | None]:
    return [1.0, float("nan"), None]


def assert_table_equal(result: pyarrow.Table, expected: pyarrow.Table):
    assert result.schema.equals(expected.schema)
    assert result.num_rows == expected.num_rows
    for col in result.column_names:
        assert result[col].is_null().equals(expected[col].is_null())
    result_df = result.to_pandas()
    expected_df = expected.to_pandas()
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "backend", ["duckdb", pytest.param("postgres", marks=pytest.mark.skip)]
)
def test_create_table(
    backend: Literal["duckdb", "postgres"],
    table_name: str,
    values: list[float | None],
):
    match backend:
        case "duckdb":
            con = ibis.duckdb.connect(":memory:")
        case "postgres":
            con = ibis.postgres.connect(...)  # TODO: set up postgres

    data = pyarrow.table({"value": values})

    ibis_table = con.create_table(table_name, data)
    result = ibis_table.to_pyarrow()

    assert_table_equal(result, data)


def test_table_postgres(
    table_name: str,
    values: list[float | None],
):
    db_url = ...  # TODO: set up postgres
    engine = sqlalchemy.create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text('DROP TABLE IF EXISTS "{table_name}"'))
        conn.execute(
            sqlalchemy.text(f'CREATE TABLE "{table_name}" (value DOUBLE PRECISION)')
        )
        conn.execute(
            sqlalchemy.text(f"""INSERT INTO "{table_name}"
                            (value) VALUES
                            (:value)
                            """),
            [{"value": x} for x in values],
        )
        conn.commit()

    # Check that table was written correctly
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(f'SELECT value FROM "{table_name}"'))
    assert str(result.fetchall()) == str([(x,) for x in values])

    con = ibis.postgres.connect(
        host=db_url.host,
        port=db_url.port,
        user=db_url.username,
        password=db_url.password,
        database=db_url.database,
    )

    ibis_table = con.table(table_name)
    result = ibis_table.to_pyarrow()

    expected = pyarrow.table({"value": values})

    assert_table_equal(result, expected)
