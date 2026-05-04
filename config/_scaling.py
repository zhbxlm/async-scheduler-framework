"""config/_scaling.py — Auto-scaling + circuit-breaker config.
aligned with docs/deepwiki-reference/配置说明.md (ScalingConfig section)
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = field(default_factory=lambda: int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", "10")))
    open_duration_seconds: int = field(default_factory=lambda: int(os.getenv("CIRCUIT_OPEN_DURATION_SECONDS", "60")))
    half_open_max: int = field(default_factory=lambda: int(os.getenv("CIRCUIT_HALF_OPEN_MAX", "1")))


@dataclass
class ScalingConfig:
    up_threshold: float = field(default_factory=lambda: float(os.getenv("SCALE_UP_THRESHOLD", "0.8")))
    down_threshold: float = field(default_factory=lambda: float(os.getenv("SCALE_DOWN_THRESHOLD", "0.2")))
    cooldown_seconds: int = field(default_factory=lambda: int(os.getenv("SCALE_COOLDOWN_SECONDS", "300")))
    eager_cooldown_seconds: int = field(default_factory=lambda: int(os.getenv("SCALE_EAGER_COOLDOWN_SECONDS", "10")))
    up_max_increment: int = field(default_factory=lambda: int(os.getenv("SCALE_UP_MAX_INCREMENT", "8")))
    down_min_actors: int = field(default_factory=lambda: int(os.getenv("SCALE_DOWN_MIN_ACTORS", "1")))
    default_max_queue_depth: int = field(default_factory=lambda: int(os.getenv("DEFAULT_MAX_QUEUE_DEPTH", "1000")))
    default_max_concurrent: int = field(default_factory=lambda: int(os.getenv("DEFAULT_MAX_CONCURRENT", "8")))
    circuit: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
