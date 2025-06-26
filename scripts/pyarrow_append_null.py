import pyarrow as pyarrow

t = pyarrow.table({"A": [1.0, 2.0]})
print(t)

print(t.append_column("B", [[1.0, None]]))

t2 = t.append_column(
    pyarrow.field("B", pyarrow.float64(), nullable=True), [[None, None]]
)
print(t2)

t3 = t.append_column("C", [[None, None]])
print(t3)
