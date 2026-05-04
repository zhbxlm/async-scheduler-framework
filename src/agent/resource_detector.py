"""ResourceDetector — hardware resource detection with 60s cache.

Detects CPU, memory, GPU. Optionally syncs from Ray cluster.
"""
from __future__ import annotations
import logging
import os
import subprocess
import time
from typing import Any

logger = logging.getLogger(__name__)
_CACHE_TTL = 60.0


class _Cache:
    def __init__(self, ttl: float = _CACHE_TTL):
        self._ttl = ttl
        self._data: dict = {}
        self._ts: dict = {}

    def get(self, key: str):
        if key in self._data and time.monotonic() - self._ts.get(key, 0) < self._ttl:
            return self._data[key]
        return None

    def set(self, key: str, val: Any) -> None:
        self._data[key] = val
        self._ts[key] = time.monotonic()


class ResourceDetector:
    def __init__(self) -> None:
        self._cache = _Cache()

    def detect(self, ray_node_state: Any = None) -> dict:
        cpu = self._detect_cpu()
        mem = self._detect_memory()
        gpus = self._detect_gpus()

        if ray_node_state:
            # Sync from Ray (authoritative)
            ray_resources = ray_node_state.get("resources_total", {})
            cpu = int(ray_resources.get("CPU", cpu))
            mem_gb = ray_resources.get("memory", mem.get("total_mb", 0) * 1024 * 1024) / 1024 / 1024 / 1024
            num_gpu = int(ray_resources.get("GPU", len(gpus)))
            return {
                "cpu_count": cpu,
                "total_memory_mb": int(mem_gb * 1024),
                "available_memory_mb": int(mem.get("available_mb", 0)),
                "gpus": gpus[:num_gpu],
            }

        return {
            "cpu_count": cpu,
            "total_memory_mb": mem.get("total_mb", 0),
            "available_memory_mb": mem.get("available_mb", 0),
            "gpus": gpus,
        }

    def _detect_cpu(self) -> int:
        cached = self._cache.get("cpu")
        if cached:
            return cached
        count = os.cpu_count() or 1
        self._cache.set("cpu", count)
        return count

    def _detect_memory(self) -> dict:
        total_mb = self._cache.get("mem_total")
        if total_mb is None:
            try:
                with open("/proc/meminfo") as f:
                    lines = f.read().splitlines()
                info = {l.split(":")[0].strip(): int(l.split(":")[1].strip().split()[0]) for l in lines if ":" in l}
                total_mb = info.get("MemTotal", 0) // 1024
            except Exception:
                total_mb = 0
            self._cache.set("mem_total", total_mb)

        avail_mb = 0
        try:
            with open("/proc/meminfo") as f:
                lines = f.read().splitlines()
            info = {l.split(":")[0].strip(): int(l.split(":")[1].strip().split()[0]) for l in lines if ":" in l}
            avail_mb = info.get("MemAvailable", 0) // 1024
        except Exception:
            pass

        return {"total_mb": total_mb, "available_mb": avail_mb}

    def _detect_gpus(self) -> list:
        cached = self._cache.get("gpus")
        if cached is not None:
            return cached
        gpus = []
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=index,name,memory.total,memory.used", "--format=csv,noheader,nounits"],
                timeout=10, text=True,
            )
            for line in out.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 4:
                    gpus.append({
                        "index": int(parts[0]),
                        "name": parts[1],
                        "memory_total_mb": int(parts[2]),
                        "memory_used_mb": int(parts[3]),
                    })
        except Exception:
            pass
        self._cache.set("gpus", gpus)
        return gpus
