from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ANOMALY_STD_DEV_THRESHOLD = 2.0


def detect_anomalies(daily_stats_df: DataFrame) -> DataFrame:
    window = Window.partitionBy("day")

    return (
        daily_stats_df.withColumn("day_group_mean", F.avg("avg_power").over(window))
        .withColumn("day_group_stddev", F.stddev("avg_power").over(window))
        .withColumn(
            "z_score",
            F.when(
                F.col("day_group_stddev").isNull() | (F.col("day_group_stddev") == 0),
                F.lit(0.0),
            ).otherwise(
                (F.col("avg_power") - F.col("day_group_mean")) / F.col("day_group_stddev")
            ),
        )
        .withColumn("is_anomaly", F.abs(F.col("z_score")) > ANOMALY_STD_DEV_THRESHOLD)
        .fillna({"day_group_stddev": 0.0})
    )
