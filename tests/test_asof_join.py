from datetime import datetime, timedelta

import pyarrow
import pytest

from learn_ibis.table_ops import get_connection, write_table_to_backend

TIME_INDEX = [datetime(1989, 1, d) for d in range(1, 5)]


@pytest.mark.parametrize(
    "left, right, expected",
    [
        (
            {"time": TIME_INDEX, "A": "abcd"},
            {
                "time": sorted(
                    [
                        TIME_INDEX[0] - timedelta(hours=1),
                        TIME_INDEX[0] + timedelta(hours=1),
                    ]
                ),
                "value": ["S", "L"],
            },
            {
                "A": "abcd",
                "time": TIME_INDEX,
                "value": ["S", "L", "L", "L"],
                "time_right": [TIME_INDEX[0] - timedelta(hours=1)]
                + [TIME_INDEX[0] + timedelta(hours=1)] * 3,
            },
        )
    ],
)
def test_asof_join(backend, left, right, expected):
    pa_left = pyarrow.Table.from_pydict(left)
    pa_right = pyarrow.Table.from_pydict(right)
    pa_expected = pyarrow.Table.from_pydict(expected)

    connection = get_connection(backend)
    ibis_left = write_table_to_backend(pa_left, "left", backend, connection)
    ibis_right = write_table_to_backend(pa_right, "right", backend, connection)
    query = ibis_left.asof_join(ibis_right, on="time").order_by("time")
    result = query.to_pyarrow()

    print(result)
    print(pa_expected)
    assert result.to_pydict() == pa_expected.to_pydict()
