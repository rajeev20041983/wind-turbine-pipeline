from src.ingestion.ingest import create_spark_session
from src.processing.stats import calculate_daily_stats


def test_calculate_daily_stats_computes_correct_min_max_avg_per_turbine_per_day():
    spark = create_spark_session()
    data = [
        ("2022-03-01 00:00:00", 1, 12.0, 100, 2.0),
        ("2022-03-01 01:00:00", 1, 12.0, 100, 4.0),
        ("2022-03-01 02:00:00", 1, 12.0, 100, 3.0),
        ("2022-03-02 00:00:00", 1, 12.0, 100, 10.0),
    ]
    columns = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    df = spark.createDataFrame(data, columns)

    result = calculate_daily_stats(df).orderBy("day").collect()

    march_1 = result[0]
    assert march_1.min_power == 2.0
    assert march_1.max_power == 4.0
    assert march_1.avg_power == 3.0

    march_2 = result[1]
    assert march_2.min_power == 10.0
    assert march_2.max_power == 10.0
    assert march_2.avg_power == 10.0

    spark.stop()


def test_calculate_daily_stats_keeps_turbines_separate():
    spark = create_spark_session()
    data = [
        ("2022-03-01 00:00:00", 1, 12.0, 100, 2.0),
        ("2022-03-01 00:00:00", 2, 12.0, 100, 8.0),
    ]
    columns = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    df = spark.createDataFrame(data, columns)

    result = calculate_daily_stats(df)

    turbine_1 = result.filter(result.turbine_id == 1).collect()[0]
    turbine_2 = result.filter(result.turbine_id == 2).collect()[0]
    assert turbine_1.avg_power == 2.0
    assert turbine_2.avg_power == 8.0

    spark.stop()
