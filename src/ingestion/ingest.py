from pyspark.sql import DataFrame, SparkSession


def read_group_file(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, inferSchema=True)


def create_spark_session(app_name: str = "wind-turbine-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
