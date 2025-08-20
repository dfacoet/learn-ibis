import os

import ibis
import pyarrow as pa
from postgres_jsonb_example import create_jsonb_table

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
con = ibis.connect(DB_CONNECTION_STRING)

SCHEMA = ibis.schema({"string_col": "string", "jsonb_col": "json"})
table_name = create_jsonb_table(con)


table: ibis.Table = ibis.table(SCHEMA, name=table_name)
query = (
    table.filter(ibis._["string_col"].isin(set("AB")))
    .cast({"jsonb_col": "string"})
    .mutate(
        {
            "float_col": ibis._["jsonb_col"]
            .cast("string")
            .replace("null", "nan")
            .cast("float")
        }
        #     {
        #         "float_col": ibis.cases(
        #             (ibis._["jsonb_col"].isnull(), None),
        #             (ibis._["jsonb_col"] == ibis.literal("null"), float("nan")),
        #             else_=ibis._["jsonb_col"].cast("float"),
        #         ),
        #         "nan_col": ibis.ifelse(ibis._["string_col"] == "A", float("nan"), 0.0),
        #     }
    )
)


result_table = con.to_pyarrow(query)
print("Query result (PyArrow table):")
print(result_table)

for i in result_table.to_pylist():
    print(i)
# print(result_table.to_pandas())
