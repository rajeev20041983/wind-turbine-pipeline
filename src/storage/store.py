import sqlite3

from pyspark.sql import DataFrame


def write_to_sqlite(df: DataFrame, db_path: str, table_name: str) -> None:
    # Output here is small (turbines x days, at most a few hundred rows), so
    # collecting to the driver and writing via plain sqlite3 avoids pulling
    # in Spark's JDBC/Hadoop dependency chain for what's fundamentally a
    # tiny write. A genuinely large output would instead go through Spark's
    # native JDBC writer or a proper warehouse connector.
    pandas_df = df.toPandas()
    conn = sqlite3.connect(db_path)
    pandas_df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
