# rebuilt from deepwiki-reference alignment
"""Infrastructure config — Redis + MySQL + API server."""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class RedisConfig:
    bns: str = ""
    host: str = "127.0.0.1"
    port: int = 6379
    password: str = ""
    db: int = 0
    decode_responses: bool = True
    max_connections: int = 50

    def __post_init__(self):
        self.bns             = os.getenv("REDIS_BNS", self.bns)
        self.host            = os.getenv("REDIS_HOST", self.host)
        self.port            = int(os.getenv("REDIS_PORT", self.port))
        self.password        = os.getenv("REDIS_PASSWORD", self.password)
        self.db              = int(os.getenv("REDIS_DB", self.db))
        self.decode_responses= os.getenv("REDIS_DECODE_RESPONSES", str(self.decode_responses)).lower() == "true"
        self.max_connections = int(os.getenv("REDIS_MAX_CONN", self.max_connections))


@dataclass
class MySQLConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = "ray_async"
    pool_size: int = 20
    max_overflow: int = 10
    pool_recycle: int = 3600
    auto_create_tables: bool = False
    check_on_startup: bool = True

    def __post_init__(self):
        self.host               = os.getenv("MYSQL_HOST", self.host)
        self.port               = int(os.getenv("MYSQL_PORT", self.port))
        self.user               = os.getenv("MYSQL_USER", self.user)
        self.password           = os.getenv("MYSQL_PASSWORD", self.password)
        self.database           = os.getenv("MYSQL_DATABASE", self.database)
        self.pool_size          = int(os.getenv("DB_POOL_SIZE", self.pool_size))
        self.max_overflow       = int(os.getenv("DB_MAX_OVERFLOW", self.max_overflow))
        self.pool_recycle       = int(os.getenv("DB_POOL_RECYCLE", self.pool_recycle))
        self.auto_create_tables = os.getenv("DB_AUTO_CREATE_TABLES", str(self.auto_create_tables)).lower() == "true"
        self.check_on_startup   = os.getenv("DB_CHECK_ON_STARTUP", str(self.check_on_startup)).lower() == "true"


@dataclass
class ServerConfig:
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    def __post_init__(self):
        self.api_host = os.getenv("API_HOST", self.api_host)
        self.api_port = int(os.getenv("API_PORT", self.api_port))


@dataclass
class InfraConfig:
    redis: RedisConfig  = field(default_factory=RedisConfig)
    mysql: MySQLConfig  = field(default_factory=MySQLConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
