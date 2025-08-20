import os

import ibis
from postgres_jsonb_example import create_jsonb_table

DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
if DB_CONNECTION_STRING is None:
    raise ValueError(
        "DB_CONNECTION_STRING environment variable not set. Run 'source scripts/start_db.sh' first."
    )

con = ibis.connect(DB_CONNECTION_STRING)
create_jsonb_table(con)
