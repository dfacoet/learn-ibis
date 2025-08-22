from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Callable

import ibis
import pyarrow
import pytest
from pydantic import BaseModel

from learn_ibis.table_ops import get_connection, write_table_to_backend

TIME_INDEX = [datetime(1989, 1, d) for d in range(1, 5)]


class AsOfJoinTestCase(BaseModel):
    key_col: str
    left: dict
    right: dict
    expected: dict
    order_by: list[str] | None = None
    keys: list[str] | None = None
    predicates: Iterable[Callable] = ()


CASE_1 = AsOfJoinTestCase(
    key_col="time",
    left={"time": TIME_INDEX, "A": "abcd"},
    right={
        "time": sorted(
            [
                TIME_INDEX[0] - timedelta(hours=1),
                TIME_INDEX[0] + timedelta(hours=1),
            ]
        ),
        "value": ["S", "L"],
    },
    expected={
        "A": "abcd",
        "time": TIME_INDEX,
        "value": ["S", "L", "L", "L"],
        "time_right": [TIME_INDEX[0] - timedelta(hours=1)]
        + [TIME_INDEX[0] + timedelta(hours=1)] * 3,
    },
    order_by=["time"],
)

ACTIVE_EPISODES_TABLE = {
    "element_id": [
        "19218ac3-0408-4dac-91d3-dd1dd7d2b815",
        "19218ac3-0408-4dac-91d3-dd1dd7d2b815",
        "2dd358d3-8932-4ad4-ae8c-ac249d634fdf",
        "19218ac3-0408-4dac-91d3-dd1dd7d2b815",
        "2dd358d3-8932-4ad4-ae8c-ac249d634fdf",
        "2dd358d3-8932-4ad4-ae8c-ac249d634fdf",
        None,
        None,
        None,
    ],
    "execution_id": ["357e1e75-a195-4fc1-94a0-817fadd019e9"] * 9,
    "trajectory": [-1] * 9,
    "time": [
        datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
        datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
        datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
        datetime(2001, 1, 2, 0, tzinfo=timezone.utc),
        datetime(2001, 1, 2, 0, tzinfo=timezone.utc),
        datetime(2001, 1, 2, 12, tzinfo=timezone.utc),
        datetime(2001, 1, 3, 0, tzinfo=timezone.utc),
        datetime(2001, 1, 3, 12, tzinfo=timezone.utc),
        datetime(2001, 1, 4, 0, tzinfo=timezone.utc),
    ],
    "start_time": [
        datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
        datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
        datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
        datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
        datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
        datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
        None,
        None,
        None,
    ],
    "size": [10.0, 10.0, 20.0, 10.0, 20.0, 20.0, None, None, None],
}


@pytest.mark.parametrize(
    "test_case",
    [
        CASE_1,
        # CASE_1.model_copy(update={"order_by": None}),
        AsOfJoinTestCase(
            key_col="time",
            left=ACTIVE_EPISODES_TABLE,
            right={
                "run_id": ["357e1e75-a195-4fc1-94a0-817fadd019e9"] * 2,
                "element_id": [
                    "19218ac3-0408-4dac-91d3-dd1dd7d2b815",
                    "2dd358d3-8932-4ad4-ae8c-ac249d634fdf",
                ],
                "time": [
                    datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
                    datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
                ],
                "trajectory": [-1] * 2,
                "event_type": ["property_update"] * 2,
                "path": ["/ElementInfo/size"] * 2,
                "value": [11.0, 23.0],
            },
            expected=ACTIVE_EPISODES_TABLE
            | {
                "run_id": ["357e1e75-a195-4fc1-94a0-817fadd019e9"] * 6 + [None] * 3,
                "element_id_right": ACTIVE_EPISODES_TABLE["element_id"],
                "time_right": [
                    datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
                    datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
                    datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
                    datetime(2001, 1, 1, 0, tzinfo=timezone.utc),
                    datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
                    datetime(2001, 1, 1, 12, tzinfo=timezone.utc),
                    None,
                    None,
                    None,
                ],
                "trajectory_right": [-1] * 6 + [None] * 3,
                "event_type": ["property_update"] * 6 + [None] * 3,
                "path": ["/ElementInfo/size"] * 6 + [None] * 3,
                "value": [11.0, 11.0, 23.0, 11.0, 23.0, 23.0, None, None, None],
            },
            order_by=["time", "element_id"],
            keys=["trajectory", "element_id"],
            predicates=[lambda left, right: left["execution_id"] == right["run_id"]],
        ),
    ],
)
def test_asof_join(backend, test_case: AsOfJoinTestCase):
    pa_left = pyarrow.Table.from_pydict(test_case.left)
    pa_right = pyarrow.Table.from_pydict(test_case.right)
    pa_expected = pyarrow.Table.from_pydict(test_case.expected)

    connection = get_connection(backend)
    ibis_left = write_table_to_backend(pa_left, "left", backend, connection)
    ibis_right = write_table_to_backend(pa_right, "right", backend, connection)
    predicates = test_case.keys or (
        p(ibis_left, ibis_right) for p in test_case.predicates
    )
    query = ibis_left.asof_join(ibis_right, on=test_case.key_col, predicates=predicates)
    if test_case.order_by:
        query = query.order_by(test_case.key_col)

    print("@".join("!#"), "SQL query:")
    print(ibis.to_sql(query))
    result = query.to_pyarrow()

    print(result)
    print(pa_expected)
    assert result.to_pydict() == pa_expected.to_pydict()
