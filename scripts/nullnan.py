import operator

import ibis
import pandas as pd
import pyarrow

VALUES = [1.0, 0.0, None, float("nan"), float("inf"), float("-inf")]

# itertools.combinations_with_replacement(VALUES, 2)
combinations = [{"A": a, "B": b} for a in VALUES for b in VALUES]

print(combinations)

data = pyarrow.Table.from_pylist(combinations)

con = ibis.duckdb.connect(":memory:")
table = con.create_table("table", data)

# Define operations as a list of tuples: (operation_symbol, operation_function)
operations = [
    ("add", operator.add),
    ("sub", operator.sub),
    ("mul", operator.mul),
    ("div", operator.truediv),
]

table = table.select(
    *["A", "B"],
    *(op_func(ibis._["A"], ibis._["B"]).name(name) for name, op_func in operations),
)

result_table = table.to_pyarrow()

for row in result_table.to_pylist():
    print(row)
