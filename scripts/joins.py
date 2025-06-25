from datetime import datetime
import ibis
import pyarrow

JOIN_KEYS = ["time", "size"]
VALUES = ["value"]
LEFT_COLS = JOIN_KEYS + ["A"] + VALUES
RIGHT_COLS = JOIN_KEYS + ["B"] + VALUES

time_index = [datetime(1989, 1, d) for d in range(1, 5)]

left_table = pyarrow.Table.from_pydict(
    {
        "time": [t for t in time_index for _ in range(2)],
        "size": ["S", "M"] * len(time_index),
        "A": [chr(60 + x) for x in range(len(time_index))] * 2,
        "value": [2 * x for x in range(2 * len(time_index))],
    }
)


right_table = pyarrow.Table.from_pydict(
    {
        "time": [t for t in time_index for _ in range(2)],
        "size": ["M", "L"] * len(time_index),
        "B": [chr(x + 100) for x in range(len(time_index))] * 2,
        "value": [1 + 2 * x for x in range(2 * len(time_index))],
    }
)

print("left:")
print(left_table.to_pandas())
print("right:")
print(right_table.to_pandas())


expected = {  # how -> expected columns
    "inner",
}

con = ibis.duckdb.connect(":memory:")
left = con.create_table("left_table", left_table)
right = con.create_table("right_table", right_table)

for how in ["inner", "left", "right", "outer"]:
    query = ibis.join(left, right, how=how, predicates=JOIN_KEYS)
    print(f"Performing a join with how={how}")

    # print("Query:")
    # print(ibis.to_sql(query))
    result = query.to_pandas()
    print(result)

    if how == "inner":
        continue
    # Coalesce and select
    print("Select (coalesced) join keys, and values")
    query = query.select(
        *(ibis.coalesce(ibis._[c], ibis._[c + "_right"]).name(c) for c in JOIN_KEYS),
        "value",
        "value_right",
        (ibis._["value"] / ibis._["value_right"]).name("ratio"),
        (ibis._["value"].fill_null(0) / ibis._["value_right"].fill_null(0)).name(
            "ratio_reg"
        ),
    )

    print(query.to_pandas())
