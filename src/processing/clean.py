from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def flag_quality_issues(df: DataFrame, power_min: float, power_max: float, wind_min: float, wind_max: float) -> DataFrame:
    return (
        df.withColumn(
            "power_output_bad",
            F.col("power_output").isNull() | (F.col("power_output") < power_min) | (F.col("power_output") > power_max),
        )
        .withColumn(
            "wind_speed_bad",
            F.col("wind_speed").isNull() | (F.col("wind_speed") < wind_min) | (F.col("wind_speed") > wind_max),
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
