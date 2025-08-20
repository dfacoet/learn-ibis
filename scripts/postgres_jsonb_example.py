"""
Example demonstrating Ibis with PostgreSQL backend:
- Create a table with string and JSONB columns
- Populate with specific data (strings "A", "B", "C" and various JSONB values)
- Read back as PyArrow table
"""

import json

import ibis


def create_jsonb_table(con=None, table_name: str = "jsonb_example") -> str:
    """
    Create and populate a table with string and JSONB columns.

    Args:
        con: Ibis connection to PostgreSQL (if None, creates connection using connection string)
        table_name: Name of the table to create

    Returns:
        str: The name of the created table
    """
    if con is None:
        con = ibis.postgres.connect(
            "postgresql://username:password@localhost:5433/ibis"
        )
    # Drop table if it exists
    try:
        con.drop_table(table_name, force=True)
    except Exception:
        pass  # Table doesn't exist

    # Create the table schema
    schema = ibis.schema(
        {"string_col": "string", "jsonb_col": "json", "floatnan_col": "float"}
    )

    # Create the table
    con.create_table(table_name, schema=schema)

    # Insert data using raw SQL to handle JSON properly
    insert_sql = f"""
    INSERT INTO {table_name} (string_col, jsonb_col, floatnan_col) VALUES
    ('A', NULL, 1.5),
    ('B', 'null'::jsonb, 'NaN'::float),
    ('C', '3.14'::jsonb, 2.7),
    ('A', '42'::jsonb, 'NaN'::float),
    ('B', '2.718'::jsonb, 3.9),
    ('C', '100'::jsonb, 'NaN'::float)
    """

    con.raw_sql(insert_sql)
    return table_name


def main():
    # Connect to PostgreSQL database using connection string
    con = ibis.connect("postgresql://username:password@localhost:5433/ibis")

    print("Connected to PostgreSQL database")

    print("Creating table with the following data:")
    print("  string_col: 'A', 'B', 'C'")
    print(
        "  jsonb_col: PostgreSQL NULL, JSON string 'null', JSON numbers (3.14, 42, 2.718, 100)"
    )

    # Create and populate the table
    table_name = create_jsonb_table(con)
    print(f"Created and populated table '{table_name}'")

    # Read the table back using Ibis and convert directly to PyArrow
    result_table = con.table(table_name)
    schema = result_table.schema()
    print(f"Table schema: {schema}")

    arrow_table = result_table.to_pyarrow()

    print("\nPyArrow table schema:")
    print(arrow_table.schema)

    print("\nPyArrow table contents (preserving original JSON):")
    data = arrow_table.to_pydict()
    for i in range(len(data["string_col"])):
        string_val = data["string_col"][i]
        jsonb_val = data["jsonb_col"][i]
        floatnan_val = data["floatnan_col"][i]
        print(
            f"  Row {i + 1}: string_col='{string_val}', jsonb_col={jsonb_val}, floatnan_col={floatnan_val}"
        )

    print("\nPyArrow table info:")
    print(f"  Number of rows: {arrow_table.num_rows}")
    print(f"  Number of columns: {arrow_table.num_columns}")
    print(f"  Column names: {arrow_table.column_names}")

    # Show the JSONB column data types and demonstrate JSON parsing
    jsonb_col = arrow_table.column("jsonb_col")
    print(f"\nJSONB column type: {jsonb_col.type}")
    print(f"JSONB column null count: {jsonb_col.null_count}")

    print("\nParsed JSON values:")
    for i, json_str in enumerate(data["jsonb_col"]):
        if json_str is None:
            print(f"  Row {i + 1}: NULL (PostgreSQL NULL)")
        else:
            try:
                parsed = json.loads(json_str)
                print(f"  Row {i + 1}: {parsed} (type: {type(parsed).__name__})")
            except json.JSONDecodeError:
                print(f"  Row {i + 1}: {json_str} (raw string)")

    print("\nDatabase connection will be closed automatically")


if __name__ == "__main__":
    main()
