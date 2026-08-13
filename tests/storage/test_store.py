import os
import sqlite3
import tempfile

from src.ingestion.ingest import create_spark_session
from src.storage.store import write_to_sqlite


def test_write_to_sqlite_persists_data_queryable_outside_spark():
    spark = create_spark_session()
    data = [(1, "2022-03-01", 2.0, 3.0, 2.5)]
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]
    df = spark.createDataFrame(data, columns)

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.db")
        write_to_sqlite(df, db_path, "daily_stats")

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT turbine_id, avg_power FROM daily_stats").fetchall()
        conn.close()

    assert rows == [(1, 2.5)]
    spark.stop()


def test_write_to_sqlite_replace_mode_overwrites_not_appends():
    spark = create_spark_session()
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.db")

        first_run = spark.createDataFrame([(1, "2022-03-01", 2.0, 3.0, 2.5)], columns)
        write_to_sqlite(first_run, db_path, "daily_stats")

        second_run = spark.createDataFrame([(2, "2022-03-02", 3.0, 4.0, 3.5)], columns)
        write_to_sqlite(second_run, db_path, "daily_stats")

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT turbine_id FROM daily_stats").fetchall()
        conn.close()

    assert rows == [(2,)]
    spark.stop()
