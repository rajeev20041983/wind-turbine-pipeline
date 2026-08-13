from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

POWER_OUTPUT_MIN = 0.0
POWER_OUTPUT_MAX = 10.0
WIND_SPEED_MIN = 0.0
WIND_SPEED_MAX = 30.0


def flag_quality_issues(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "power_output_bad",
            F.col("power_output").isNull()
            | (F.col("power_output") < POWER_OUTPUT_MIN)
            | (F.col("power_output") > POWER_OUTPUT_MAX),
        )
        .withColumn(
            "wind_speed_bad",
            F.col("wind_speed").isNull()
            | (F.col("wind_speed") < WIND_SPEED_MIN)
            | (F.col("wind_speed") > WIND_SPEED_MAX),
        )
    )


def split_errors(flagged_df: DataFrame) -> DataFrame:
    return flagged_df.filter(F.col("power_output_bad") | F.col("wind_speed_bad"))


def impute_bad_values(flagged_df: DataFrame) -> DataFrame:
    nulled = flagged_df.withColumn(
        "power_output", F.when(F.col("power_output_bad"), F.lit(None)).otherwise(F.col("power_output"))
    ).withColumn(
        "wind_speed", F.when(F.col("wind_speed_bad"), F.lit(None)).otherwise(F.col("wind_speed"))
    )

    window = Window.partitionBy("turbine_id").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)

    return (
        nulled.withColumn("power_output", F.last("power_output", ignorenulls=True).over(window))
        .withColumn("wind_speed", F.last("wind_speed", ignorenulls=True).over(window))
        .drop("power_output_bad", "wind_speed_bad")
    )
