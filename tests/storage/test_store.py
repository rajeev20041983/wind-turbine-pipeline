import os
import sqlite3
import tempfile

from src.ingestion.ingest import create_spark_session
from src.storage.store import write_to_sqlite


def test_write_to_sqlite_inserts_new_rows():
    spark = create_spark_session()
    data = [(1, "2022-03-01", 2.0, 3.0, 2.5)]
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]
    df = spark.createDataFrame(data, columns)

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.db")
        write_to_sqlite(df, db_path, "daily_stats", primary_keys=["turbine_id", "day"])

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT turbine_id, avg_power FROM daily_stats").fetchall()
        conn.close()

    assert rows == [(1, 2.5)]
    spark.stop()


def test_write_to_sqlite_running_same_data_twice_does_not_duplicate():
    spark = create_spark_session()
    data = [(1, "2022-03-01", 2.0, 3.0, 2.5)]
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]
    df = spark.createDataFrame(data, columns)

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.db")

        write_to_sqlite(df, db_path, "daily_stats", primary_keys=["turbine_id", "day"])
        write_to_sqlite(df, db_path, "daily_stats", primary_keys=["turbine_id", "day"])  # same data, run again

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM daily_stats").fetchone()[0]
        conn.close()

    assert count == 1  # not 2 - re-running did not duplicate
    spark.stop()


def test_write_to_sqlite_does_not_update_existing_row_on_conflict():
    spark = create_spark_session()
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.db")

        original = spark.createDataFrame([(1, "2022-03-01", 2.0, 3.0, 2.5)], columns)
        write_to_sqlite(original, db_path, "daily_stats", primary_keys=["turbine_id", "day"])

        # same primary key (turbine_id=1, day=2022-03-01), but different values
        conflicting = spark.createDataFrame([(1, "2022-03-01", 99.0, 99.0, 99.0)], columns)
        write_to_sqlite(conflicting, db_path, "daily_stats", primary_keys=["turbine_id", "day"])

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT avg_power FROM daily_stats WHERE turbine_id = 1").fetchone()
        conn.close()

    # original value survives - the conflicting write was ignored, not applied as an update
    assert row[0] == 2.5
    spark.stop()


def test_write_to_sqlite_new_rows_still_get_added_alongside_existing_ones():
    spark = create_spark_session()
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.db")

        first_run = spark.createDataFrame([(1, "2022-03-01", 2.0, 3.0, 2.5)], columns)
        write_to_sqlite(first_run, db_path, "daily_stats", primary_keys=["turbine_id", "day"])

        second_run = spark.createDataFrame([(2, "2022-03-02", 3.0, 4.0, 3.5)], columns)
        write_to_sqlite(second_run, db_path, "daily_stats", primary_keys=["turbine_id", "day"])

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT turbine_id FROM daily_stats ORDER BY turbine_id").fetchall()
        conn.close()

    assert rows == [(1,), (2,)]  # both rows present - new data adds, doesn't replace
    spark.stop()
