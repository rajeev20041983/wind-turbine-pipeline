import os
import sqlite3
import tempfile

from pyspark.sql import Row

from src.config import PipelineConfig
from src.ingestion.ingest import create_spark_session, read_group_file
from src.processing.clean import flag_quality_issues, split_errors
from src.storage.store import write_to_sqlite


def test_bad_value_injected_into_real_data_copy_lands_in_quality_errors_table():
    config = PipelineConfig.from_yaml("config/pipeline_config.yaml")
    spark = create_spark_session()

    real_data = read_group_file(spark, config.data_files[0])
    print("row count, real data:", real_data.count())

    fake_bad_row = spark.createDataFrame([
        Row(timestamp="2022-03-01 05:00:00", turbine_id=1, wind_speed=12.0, wind_direction=100, power_output=-99.0)
    ])
    data_with_injected_row = real_data.union(fake_bad_row)
    print("row count, after adding 1 fake bad row:", data_with_injected_row.count())

    flagged = flag_quality_issues(
        data_with_injected_row,
        config.power_output_min,
        config.power_output_max,
        config.wind_speed_min,
        config.wind_speed_max,
    )
    errors = split_errors(flagged)

    print("--- error table content ---")
    errors.show(truncate=False)

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test_errors.db")
        write_to_sqlite(errors, db_path, "quality_errors")

        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            "SELECT turbine_id, power_output FROM quality_errors WHERE power_output = -99.0"
        ).fetchall()
        conn.close()

    print("--- queried from the actual sqlite db file ---")
    print(rows)

    assert rows == [(1, -99.0)]
    spark.stop()
