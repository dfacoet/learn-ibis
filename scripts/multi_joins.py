from datetime import datetime
from functools import reduce

import ibis
import pyarrow

JOIN_KEYS = ["time", "size"]
VALUES = ["value"]

time_index = [datetime(1989, 1, d) for d in range(1, 5)]

UNIQUE_DIM = ["S", "L", "XL"]
N = len(UNIQUE_DIM)

# Each table has columns:
# - one for each join key
# - a string columns with unique name (starting at "A")
# - value
data_tables = [
    pyarrow.Table.from_pydict(
        {
            "time": [t for t in time_index for _ in range(2)],
            "size": [s, "M"] * len(time_index),
            chr(65 + k): [chr(60 + 10 * k * x) for x in range(len(time_index))] * 2,
            "value": [N * x + k for x in range(2 * len(time_index))],
        }
    )
    for k, s in enumerate(UNIQUE_DIM)
]

for i, table in enumerate(data_tables):
    print(f"table {i}:")
    print(table.to_pandas())

con = ibis.duckdb.connect(":memory:")
tables = [con.create_table(f"table_{i}", table) for i, table in enumerate(data_tables)]

# rename value columns
tables = [t.rename({f"value_{i}": "value"}) for i, t in enumerate(tables)]

two_joined = ibis.join(tables[0], tables[1], how="outer", predicates=JOIN_KEYS).select(
    *JOIN_KEYS,
    *(c + "_right" for c in JOIN_KEYS),
    "value_0",
    "value_1",
)

print("joining two", ibis.to_sql(two_joined))

print(two_joined.to_pandas())

three_joined = ibis.join(
    two_joined, tables[2], how="outer", predicates=JOIN_KEYS, rname="{name}_rright"
).select(
    *JOIN_KEYS,
    *(c + "_right" for c in JOIN_KEYS),
    *(c + "_rright" for c in JOIN_KEYS),
    "value_0",
    "value_1",
    "value_2",
)

print("three joined columns, pre-coalesce", three_joined.columns)

three_joined = three_joined.select(
    *(
        ibis.coalesce(ibis._[c], ibis._[c + "_right"], ibis._[c + "_rright"]).name(c)
        for c in JOIN_KEYS
    ),
    "value_0",
    "value_1",
    "value_2",
)


print("joining three", ibis.to_sql(three_joined))

print(three_joined.to_pandas())

# iterating join-and-coalesce doesn't work
# https://github.com/ibis-project/ibis/issues/10293
# so we join all tables by folding over enumerate(tables),
# creating N columns for each join key with name {c}_{i}, and then coalescing all
# at the end.

_, all_joined_reduce = reduce(
    lambda x, y: (
        None,
        ibis.join(
            x[1], y[1], how="outer", predicates=JOIN_KEYS, rname=f"{{name}}_{y[0]}"
        ),
    ),
    enumerate(tables),
)

all_joined_reduce = all_joined_reduce.select(
    *(
        ibis.coalesce(ibis._[c], *(ibis._[c + f"_{i}"] for i in range(1, N))).name(c)
        for c in JOIN_KEYS
    ),
    *(f"value_{i}" for i in range(N)),
)

print(all_joined_reduce.to_pandas())
