from src.config import PipelineConfig
from src.ingestion.ingest import create_spark_session, discover_data_files, read_group_file


def test_discover_data_files_finds_all_three_group_files():
    config = PipelineConfig.from_yaml("config/pipeline_config.yaml")
    files = discover_data_files(config.data_dir, config.file_pattern)
    assert len(files) == 3


def test_discover_data_files_ignores_non_matching_files(tmp_path):
    (tmp_path / "data_group_1.csv").write_text("id\n1")
    (tmp_path / "data_group_2.csv").write_text("id\n1")
    (tmp_path / "readme.txt").write_text("not a data file")
    (tmp_path / "other_data.csv").write_text("id\n1")

    files = discover_data_files(str(tmp_path), "data_group_*.csv")

    assert len(files) == 2
    assert all("data_group_" in f for f in files)


def test_read_group_file_has_five_turbines():
    config = PipelineConfig.from_yaml("config/pipeline_config.yaml")
    files = discover_data_files(config.data_dir, config.file_pattern)
    spark = create_spark_session()
    df = read_group_file(spark, files[0])
    assert df.select("turbine_id").distinct().count() == 5
    spark.stop()


def test_read_group_file_has_no_duplicate_turbine_timestamp_pairs():
    config = PipelineConfig.from_yaml("config/pipeline_config.yaml")
    files = discover_data_files(config.data_dir, config.file_pattern)
    spark = create_spark_session()
    df = read_group_file(spark, files[0])
    assert df.count() == df.select("turbine_id", "timestamp").distinct().count()
    spark.stop()
