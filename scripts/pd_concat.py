import pandas as pd

parent_tables = {
    "one": pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}),
    "two": pd.DataFrame({"A": [7, 8, 9], "B": [10, 11, 12]}),
}

df = pd.concat(df.assign(parent=name) for name, df in parent_tables.items())

print(df)
