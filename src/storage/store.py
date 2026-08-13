import sqlite3

from pyspark.sql import DataFrame


def write_to_sqlite(df: DataFrame, db_path: str, table_name: str) -> None:
    pandas_df = df.toPandas()
    conn = sqlite3.connect(db_path)
    pandas_df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
