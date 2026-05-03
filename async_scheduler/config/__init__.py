"""Advanced configuration with environment support, validation, and dotenv loading.

This module provides a more robust configuration system on top of the simple
settings module. It supports:

- Environment detection (production/staging/test/development/local)
- Dotenv file loading (via python-dotenv)
- Configuration validation with Pydantic (optional)
- Runtime overrides via context managers

Usage:

    from async_scheduler.config import config, ConfigContext

    print(config.database.url)
    print(config.environment)  # "production", "test", "local"

    # Override config for a block
    with ConfigContext(LOG_LEVEL="DEBUG"):
        run_my_job()

    # Load .env or a specific file
    config.load_dotenv(".env.production")
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generator, Literal

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from async_scheduler.settings import RuntimeSettings, load_settings


class Environment(str, Enum):
    PRODUCTION = "production"
    STAGING = "staging"
    TEST = "test"
    DEVELOPMENT = "development"
    LOCAL = "local"


def detect_environment() -> Environment:
    env_value = os.environ.get("ENVIRONMENT", "").lower().strip()
    if env_value in ("prod", "production"):
        return Environment.PRODUCTION
    if env_value in ("stage", "staging"):
        return Environment.STAGING
    if env_value in ("test", "testing"):
        return Environment.TEST
    if env_value in ("dev", "development"):
        return Environment.DEVELOPMENT
    if env_value == "local":
        return Environment.LOCAL
    # Auto-detect only when ENVIRONMENT is unset/unknown
    if "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.argv[0]:
        return Environment.TEST
    if "unittest" in sys.argv[0] or "pytest" in sys.argv[0]:
        return Environment.TEST
    if os.environ.get("VIRTUAL_ENV") or ".venv" in sys.executable:
        return Environment.LOCAL
    if os.environ.get("USER") in ("root", "admin") or os.environ.get("HOME") == "/root":
        return Environment.PRODUCTION
    return Environment.LOCAL


@dataclass(frozen=True)
class Config:
    """Enhanced configuration wrapper that adds environment awareness."""

    inner: RuntimeSettings
    environment: Environment

    @property
    def database(self):
        return self.inner.database

    @property
    def logging(self):
        return self.inner.logging

    @property
    def backends(self):
        return self.inner.backends

    @property
    def is_production(self) -> bool:
        return self.environment == Environment.PRODUCTION

    @property
    def is_test(self) -> bool:
        return self.environment == Environment.TEST

    @property
    def is_local(self) -> bool:
        return self.environment == Environment.LOCAL

    def load_dotenv(self, path: str | None = None) -> None:
        """Load environment variables from a dotenv file.

        If path is None, tries .env.<environment> then .env.
        """
        if load_dotenv is None:
            # No‑op if dotenv is not installed
            return
        if path:
            load_dotenv(path, override=True)
            return

        # Try environment‑specific file first
        env_file = f".env.{self.environment.value}"
        if os.path.exists(env_file):
            load_dotenv(env_file, override=True)
        elif os.path.exists(".env"):
            load_dotenv(".env", override=True)


def get_config() -> Config:
    env = detect_environment()
    settings = load_settings()
    return Config(inner=settings, environment=env)


config = get_config()


@contextmanager
def ConfigContext(**overrides: Any) -> Generator[Config, None, None]:
    """Temporarily override environment variables for a block.

    Example:
        with ConfigContext(LOG_LEVEL="DEBUG"):
            do_something()
    """
    old = {}
    for key, value in overrides.items():
        old[key] = os.environ.get(key)
        os.environ[key] = str(value)
    try:
        yield get_config()
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value