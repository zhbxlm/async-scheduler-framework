"""DagLoader — load DAG definitions from YAML + Redis."""
from __future__ import annotations
import yaml
from typing import Any


class DagLoader:
    def __init__(self, config_dir: str = "config/dags"):
        self._config_dir = config_dir

    def load_from_yaml(self, dag_id: str) -> dict:
        """Load DAG definition from YAML file."""
        path = f"{self._config_dir}/{dag_id}.yaml"
        with open(path) as f:
            return yaml.safe_load(f)

    def load_from_redis(self, dag_id: str) -> dict:
        """Load DAG definition from Redis."""
        # TODO: implement
        return {}
