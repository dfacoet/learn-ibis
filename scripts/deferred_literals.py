import ibis


con = ibis.duckdb.connect(":memory:")
table = con.create_table("left_table", {"value": [0, 1, 2, 3]})
print(table.to_pandas())

for lit in [1, 1.0, ibis.literal(1.0), ibis.deferred(1.0), ibis._(1.0)]:
    print(f"\n\nUsing literal: {lit}: {type(lit)}")
    try:
        print(table.select(lit).to_pandas())
    except Exception as e:
        print(f"Error: {e}")

    try:
        print(table.select(lit + ibis._["value"]).to_pandas())
    except Exception as e:
        print(f"Error: {e}")
