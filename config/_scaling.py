"""Scaling + circuit breaker config."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 10
    open_duration_seconds: int = 60
    half_open_max: int = 1

    def __post_init__(self):
        self.failure_threshold = int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", self.failure_threshold))
        self.open_duration_seconds = int(os.getenv("CIRCUIT_OPEN_DURATION_SECONDS", self.open_duration_seconds))
        self.half_open_max = int(os.getenv("CIRCUIT_HALF_OPEN_MAX", self.half_open_max))


@dataclass
class ScalingConfig:
    default_max_queue_depth: int = 1000
    default_max_concurrent: int = 8
    up_threshold: float = 0.8
    down_threshold: float = 0.2
    cooldown_seconds: int = 300
    eager_cooldown_seconds: int = 10
    eager_down_cooldown_seconds: int = 10
    up_max_increment: int = 8
    down_min_actors: int = 1
    drain_default_deadline_seconds: int = 300
    dequeue_scan_limit: int = 50
    circuit: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)

    def __post_init__(self):
        self.default_max_queue_depth = int(os.getenv("DEFAULT_MAX_QUEUE_DEPTH", self.default_max_queue_depth))
        self.default_max_concurrent = int(os.getenv("DEFAULT_MAX_CONCURRENT", self.default_max_concurrent))
        self.up_threshold = float(os.getenv("SCALE_UP_THRESHOLD", self.up_threshold))
        self.down_threshold = float(os.getenv("SCALE_DOWN_THRESHOLD", self.down_threshold))
        self.cooldown_seconds = int(os.getenv("SCALE_COOLDOWN_SECONDS", self.cooldown_seconds))
        self.eager_cooldown_seconds = int(os.getenv("EAGER_SCALE_COOLDOWN_SECONDS", self.eager_cooldown_seconds))
        self.eager_down_cooldown_seconds = int(os.getenv("EAGER_SCALE_DOWN_COOLDOWN_SECONDS", self.eager_down_cooldown_seconds))
        self.up_max_increment = int(os.getenv("SCALE_UP_MAX_INCREMENT", self.up_max_increment))
        self.down_min_actors = int(os.getenv("SCALE_DOWN_MIN_ACTORS", self.down_min_actors))
        self.drain_default_deadline_seconds = int(os.getenv("DRAIN_DEFAULT_DEADLINE_SECONDS", self.drain_default_deadline_seconds))
        self.dequeue_scan_limit = int(os.getenv("DEQUEUE_SCAN_LIMIT", self.dequeue_scan_limit))
