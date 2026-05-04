# rebuilt from deepwiki-reference alignment
"""Scaling + circuit-breaker config."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 10
    open_duration_seconds: int = 60
    half_open_max: int = 1

    def __post_init__(self):
        self.failure_threshold    = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", self.failure_threshold))
        self.open_duration_seconds= int(os.getenv("CIRCUIT_BREAKER_OPEN_DURATION_SECONDS", self.open_duration_seconds))
        self.half_open_max        = int(os.getenv("CIRCUIT_BREAKER_HALF_OPEN_MAX", self.half_open_max))


@dataclass
class ScalingConfig:
    up_threshold: float = 0.8
    down_threshold: float = 0.2
    cooldown_seconds: int = 300
    max_scaling_increment: int = 4
    min_actors: int = 1
    circuit: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)

    def __post_init__(self):
        self.up_threshold         = float(os.getenv("SCALE_UP_THRESHOLD", self.up_threshold))
        self.down_threshold       = float(os.getenv("SCALE_DOWN_THRESHOLD", self.down_threshold))
        self.cooldown_seconds     = int(os.getenv("SCALE_COOLDOWN_SECONDS", self.cooldown_seconds))
        self.max_scaling_increment= int(os.getenv("MAX_SCALING_INCREMENT", self.max_scaling_increment))
        self.min_actors           = int(os.getenv("MIN_ACTORS", self.min_actors))
