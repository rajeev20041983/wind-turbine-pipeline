import sqlite3

from pyspark.sql import DataFrame

_SQLITE_TYPE_MAP = {
    "int64": "INTEGER",
    "float64": "REAL",
    "bool": "INTEGER",
    "object": "TEXT",
    "datetime64[ns]": "TEXT",
}


def _sqlite_type(pandas_dtype) -> str:
    return _SQLITE_TYPE_MAP.get(str(pandas_dtype), "TEXT")


def write_to_sqlite(df: DataFrame, db_path: str, table_name: str, primary_keys: list[str]) -> None:
    pandas_df = df.toPandas()

    for col in pandas_df.columns:
        if pandas_df[col].dtype.kind == "M":  # datetime64 columns
            pandas_df[col] = pandas_df[col].astype(str)

    column_defs = ", ".join(f'"{col}" {_sqlite_type(dtype)}' for col, dtype in pandas_df.dtypes.items())
    pk_clause = ", ".join(f'"{pk}"' for pk in primary_keys)
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({column_defs}, PRIMARY KEY ({pk_clause}))'

    conn = sqlite3.connect(db_path)
    conn.execute(create_sql)
    conn.commit()

    columns = list(pandas_df.columns)
    col_names = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f'INSERT OR IGNORE INTO "{table_name}" ({col_names}) VALUES ({placeholders})'

    rows = [tuple(row) for row in pandas_df.itertuples(index=False, name=None)]
    conn.executemany(insert_sql, rows)
    conn.commit()
    conn.close()
