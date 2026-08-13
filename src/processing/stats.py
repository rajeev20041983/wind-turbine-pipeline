from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_daily_stats(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("day", F.to_date("timestamp"))
        .groupBy("turbine_id", "day")
        .agg(
            F.min("power_output").alias("min_power"),
            F.max("power_output").alias("max_power"),
            F.avg("power_output").alias("avg_power"),
        )
    )
