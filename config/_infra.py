"""config/_infra.py — Infrastructure config (Redis + MySQL + Server).
aligned with docs/deepwiki-reference/配置说明.md
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class RedisConfig:
    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))
    password: str = field(default_factory=lambda: os.getenv("REDIS_PASSWORD", ""))
    decode_responses: bool = True
    max_connections: int = field(default_factory=lambda: int(os.getenv("REDIS_MAX_CONN", "50")))

    def __post_init__(self):
        env = os.getenv("REDIS_DECODE_RESPONSES", "true")
        self.decode_responses = env.lower() == "true"

    @property
    def url(self) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


@dataclass
class MySQLConfig:
    host: str = field(default_factory=lambda: os.getenv("MYSQL_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(os.getenv("MYSQL_PORT", "3306")))
    user: str = field(default_factory=lambda: os.getenv("MYSQL_USER", "root"))
    password: str = field(default_factory=lambda: os.getenv("MYSQL_PASSWORD", ""))
    database: str = field(default_factory=lambda: os.getenv("MYSQL_DATABASE", "ray_async"))
    pool_size: int = field(default_factory=lambda: int(os.getenv("DB_POOL_SIZE", "20")))
    max_overflow: int = field(default_factory=lambda: int(os.getenv("DB_MAX_OVERFLOW", "10")))
    pool_recycle: int = field(default_factory=lambda: int(os.getenv("DB_POOL_RECYCLE", "3600")))
    auto_create_tables: bool = False
    check_on_startup: bool = True

    def __post_init__(self):
        self.auto_create_tables = os.getenv("DB_AUTO_CREATE_TABLES", "false").lower() == "true"
        self.check_on_startup  = os.getenv("DB_CHECK_ON_STARTUP", "true").lower() == "true"

    @property
    def url(self) -> str:
        return (
            f"mysql+asyncmy://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class ServerConfig:
    api_host: str = field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    api_port: int = field(default_factory=lambda: int(os.getenv("API_PORT", "8000")))


@dataclass
class InfraConfig:
    redis: RedisConfig = field(default_factory=RedisConfig)
    mysql: MySQLConfig = field(default_factory=MySQLConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
