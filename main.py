from src.config import PipelineConfig
from src.ingestion.ingest import create_spark_session, read_group_file
from src.processing.clean import flag_quality_issues, impute_bad_values, split_errors
from src.processing.stats import calculate_daily_stats
from src.processing.anomalies import detect_anomalies
from src.storage.store import write_to_sqlite

DB_PATH = "turbine_data.db"


def run():
    config = PipelineConfig.from_yaml("config/pipeline_config.yaml")
    spark = create_spark_session()

    raw_dfs = [read_group_file(spark, path) for path in config.data_files]
    raw = raw_dfs[0]
    for df in raw_dfs[1:]:
        raw = raw.union(df)

    flagged = flag_quality_issues(raw, config.power_output_min, config.power_output_max, config.wind_speed_min, config.wind_speed_max)
    errors = split_errors(flagged)
    clean = impute_bad_values(flagged)

    missing_count = errors.filter(errors.power_output.isNull() | errors.wind_speed.isNull()).count()
    out_of_range_count = errors.filter(errors.power_output.isNotNull() & errors.wind_speed.isNotNull()).count()

    stats = calculate_daily_stats(clean)
    anomalies = detect_anomalies(stats, config.anomaly_std_dev_threshold)

    write_to_sqlite(clean, DB_PATH, "cleaned_readings")
    write_to_sqlite(errors, DB_PATH, "quality_errors")
    write_to_sqlite(stats, DB_PATH, "daily_stats")
    write_to_sqlite(anomalies, DB_PATH, "anomalies")

    print(f"rows in: {raw.count()}")
    print(f"missing values (null power/wind): {missing_count}")
    print(f"out-of-range values (impossible readings): {out_of_range_count}")
    print(f"total flagged, preserved in quality_errors table: {errors.count()}")
    print(f"anomalies detected: {anomalies.filter(anomalies.is_anomaly).count()}")

    spark.stop()


if __name__ == "__main__":
    run()
