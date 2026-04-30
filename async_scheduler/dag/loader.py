"""YAML DAG loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from async_scheduler.core.models import DAGCreate, DAGNode


class DAGLoader:
    """Loads DAG definitions from YAML files."""

    def load_file(self, path: str | Path) -> DAGCreate:
        data = yaml.safe_load(Path(path).read_text())
        return self.load_dict(data)

    def load_dict(self, data: dict[str, Any]) -> DAGCreate:
        nodes = [DAGNode(**node) for node in data.get("nodes", [])]
        return DAGCreate(
            name=data["name"],
            description=data.get("description"),
            tenant_id=data.get("tenant_id"),
            max_parallelism=data.get("max_parallelism", 4),
            nodes=nodes,
        )
