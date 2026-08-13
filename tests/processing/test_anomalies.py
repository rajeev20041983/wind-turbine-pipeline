from src.ingestion.ingest import create_spark_session
from src.processing.anomalies import detect_anomalies


def _turbine_rows(day, avg_powers, start_id=1):
    return [(start_id + i, day, ap - 0.5, ap + 0.5, ap) for i, ap in enumerate(avg_powers)]


def test_detect_anomalies_flags_turbine_far_from_its_peers():
    # 14 clustered turbines + 1 extreme outlier - matches the real ~15-turbine
    # group size. With too few peers (e.g. only 5), the outlier's own extreme
    # value inflates the group's std dev enough to mask itself (the 'masking
    # effect') - z-score can never cross 2 with n=5, no matter how extreme the
    # outlier is. This is why anomaly detection must run across all 15
    # turbines per day, not per 5-turbine file.
    spark = create_spark_session()
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]
    rows = _turbine_rows("2022-03-01", [2.5] * 14) + [(99, "2022-03-01", 9.0, 11.0, 10.0)]
    df = spark.createDataFrame(rows, columns)

    result = detect_anomalies(df)
    flagged = result.filter(result.is_anomaly).collect()

    assert len(flagged) == 1
    assert flagged[0].turbine_id == 99
    spark.stop()


def test_detect_anomalies_flags_nothing_on_a_uniform_day():
    spark = create_spark_session()
    data = [
        (1, "2022-03-01", 2.0, 3.0, 2.5),
        (2, "2022-03-01", 2.0, 3.0, 2.5),
        (3, "2022-03-01", 2.0, 3.0, 2.5),
    ]
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]
    df = spark.createDataFrame(data, columns)

    result = detect_anomalies(df)
    assert result.filter(result.is_anomaly).count() == 0
    spark.stop()


def test_detect_anomalies_handles_single_turbine_day_without_crashing():
    # only one turbine reporting that day -> stddev is genuinely undefined
    # (Spark returns null, not 0) -> must not crash, must not flag
    spark = create_spark_session()
    data = [(5, "2022-03-02", 2.0, 3.0, 2.5)]
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]
    df = spark.createDataFrame(data, columns)

    result = detect_anomalies(df)
    row = result.collect()[0]
    assert row.day_group_stddev == 0.0
    assert row.is_anomaly == False
    spark.stop()


def test_detect_anomalies_keeps_days_separate():
    spark = create_spark_session()
    columns = ["turbine_id", "day", "min_power", "max_power", "avg_power"]
    rows = _turbine_rows("2022-03-01", [2.5] * 14) + [(99, "2022-03-01", 9.0, 11.0, 10.0)]
    rows += [(50, "2022-03-02", 2.0, 3.0, 2.5)]
    df = spark.createDataFrame(rows, columns)

    result = detect_anomalies(df)
    day_2_row = result.filter(result.day == "2022-03-02").collect()[0]
    assert day_2_row.is_anomaly == False
    day_1_flagged = result.filter((result.day == "2022-03-01") & result.is_anomaly).count()
    assert day_1_flagged == 1
    spark.stop()
