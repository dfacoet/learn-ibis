from datetime import datetime, timedelta

import ibis
import pyarrow

JOIN_KEYS = ["time", "size"]
VALUES = ["value"]
LEFT_COLS = JOIN_KEYS + ["A"] + VALUES
RIGHT_COLS = JOIN_KEYS + ["B"] + VALUES

time_index = [datetime(1989, 1, d) for d in range(1, 5)]

left_table = pyarrow.Table.from_pydict(
    {
        "time": time_index,
        "size": ["S"] * len(time_index),
        "A": [chr(60 + x) for x in range(len(time_index))],
        "value": [2 * x for x in range(len(time_index))],
    }
)

# right_table = pyarrow.Table.from_pydict(
#     {
#         "time": [t + timedelta(hours=1) for t in time_index],
#         "property": ["size", "A"] * 2,  # "size" or "B"
#         "value": ["L", "!", "XL", "@"],
#     }
# )
right_table = pyarrow.Table.from_pydict(
    {
        "time": sorted(
            [
                time_index[0] - timedelta(hours=1),
                time_index[0] + timedelta(hours=1),
            ]
            * 2
        ),
        "property": ["size", "A"] * 2,  # "size" or "B"
        "value": ["S", "!", "L", "@"],
    }
)

print("left:")
print(left_table.to_pandas())
print("right:")
print(right_table.to_pandas())

con = ibis.duckdb.connect(":memory:")
left = con.create_table("left_table", left_table)
right = con.create_table("right_table", right_table)

query = (
    left.drop("value")
    .asof_join(right.filter(ibis._["property"] == "size"), on="time")
    .order_by("time")
)

print(query.to_pyarrow())
print(query.to_pandas())

final = query.mutate({"size": "value"}).drop("property", "time_right", "value")
print(final.to_pandas())
