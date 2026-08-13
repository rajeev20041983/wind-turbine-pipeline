from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class PipelineConfig:
    data_files: list[str]
    power_output_min: float
    power_output_max: float
    wind_speed_min: float
    wind_speed_max: float
    anomaly_std_dev_threshold: float

    @staticmethod
    def from_yaml(path: str | Path) -> "PipelineConfig":
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        return PipelineConfig(
            data_files=raw["data_files"],
            power_output_min=raw["power_output_min"],
            power_output_max=raw["power_output_max"],
            wind_speed_min=raw["wind_speed_min"],
            wind_speed_max=raw["wind_speed_max"],
            anomaly_std_dev_threshold=raw["anomaly_std_dev_threshold"],
        )
