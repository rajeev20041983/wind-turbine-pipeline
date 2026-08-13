from src.ingestion.ingest import create_spark_session
from src.processing.clean import flag_quality_issues, split_errors, impute_bad_values

POWER_MIN, POWER_MAX = 0.0, 10.0
WIND_MIN, WIND_MAX = 0.0, 30.0


def _sample_df(spark):
    data = [
        ("2022-03-01 00:00:00", 1, 12.0, 100, 2.5),
        ("2022-03-01 01:00:00", 1, 12.5, 100, None),
        ("2022-03-01 02:00:00", 1, 12.2, 100, 2.6),
        ("2022-03-01 03:00:00", 1, 12.1, 100, -5.0),
    ]
    columns = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    return spark.createDataFrame(data, columns)


def test_flag_quality_issues_catches_missing_and_out_of_range():
    spark = create_spark_session()
    flagged = flag_quality_issues(_sample_df(spark), POWER_MIN, POWER_MAX, WIND_MIN, WIND_MAX)
    assert flagged.filter(flagged.power_output_bad).count() == 2
    spark.stop()


def test_split_errors_preserves_original_bad_value_for_audit():
    spark = create_spark_session()
    flagged = flag_quality_issues(_sample_df(spark), POWER_MIN, POWER_MAX, WIND_MIN, WIND_MAX)
    errors = split_errors(flagged)
    negative_row = errors.filter(errors.timestamp == "2022-03-01 03:00:00").collect()[0]
    assert negative_row.power_output == -5.0
    spark.stop()


def test_split_errors_preserves_null_value_for_audit():
    spark = create_spark_session()
    flagged = flag_quality_issues(_sample_df(spark), POWER_MIN, POWER_MAX, WIND_MIN, WIND_MAX)
    errors = split_errors(flagged)
    null_row = errors.filter(errors.timestamp == "2022-03-01 01:00:00").collect()[0]
    assert null_row.power_output is None
    spark.stop()


def test_impute_bad_values_leaves_no_nulls_and_no_out_of_range_values():
    spark = create_spark_session()
    flagged = flag_quality_issues(_sample_df(spark), POWER_MIN, POWER_MAX, WIND_MIN, WIND_MAX)
    imputed = impute_bad_values(flagged)
    assert imputed.filter(imputed.power_output.isNull()).count() == 0
    assert imputed.filter((imputed.power_output < POWER_MIN) | (imputed.power_output > POWER_MAX)).count() == 0
    spark.stop()
