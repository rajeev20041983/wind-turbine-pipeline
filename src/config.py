from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class PipelineConfig:
    data_files: list[str]

    @staticmethod
    def from_yaml(path: str | Path) -> "PipelineConfig":
        with open(path, "r") as f:
            raw = yaml.safe_load(f)
        return PipelineConfig(data_files=raw["data_files"])