from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def discover_data_files(data_dir: str, file_pattern: str) -> list[str]:
    return sorted(str(p) for p in Path(data_dir).glob(file_pattern))


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
