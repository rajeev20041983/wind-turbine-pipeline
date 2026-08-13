from src.config import PipelineConfig
from src.ingestion.ingest import create_spark_session, read_group_file


def test_read_group_file_has_five_turbines():
    config = PipelineConfig.from_yaml("config/pipeline_config.yaml")
    spark = create_spark_session()
    df = read_group_file(spark, config.data_files[0])
    assert df.select("turbine_id").distinct().count() == 5
    spark.stop()


def test_read_group_file_has_no_duplicate_turbine_timestamp_pairs():
    config = PipelineConfig.from_yaml("config/pipeline_config.yaml")
    spark = create_spark_session()
    df = read_group_file(spark, config.data_files[0])
    assert df.count() == df.select("turbine_id", "timestamp").distinct().count()
    spark.stop()
