"""Modern configuration using pydantic-settings with environment variable support."""
from __future__ import annotations

from typing import Optional, Literal
from pydantic import Field, RedisDsn, PostgresDsn, MySQLDsn, HttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisConfig(BaseSettings):
    """Redis configuration."""
    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        case_sensitive=False,
    )
    
    url: Optional[RedisDsn] = Field(
        default=None,
        description="Redis URL (e.g., redis://localhost:6379/0)",
    )
    host: str = Field(default="127.0.0.1", description="Redis host")
    port: int = Field(default=6379, description="Redis port")
    password: Optional[str] = Field(default=None, description="Redis password")
    db: int = Field(default=0, description="Redis database number")
    max_connections: int = Field(default=50, description="Redis connection pool size")
    decode_responses: bool = Field(default=True, description="Decode Redis responses")
    
    @property
    def connection_string(self) -> str:
        """Get Redis connection string."""
        if self.url:
            return str(self.url)
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"


class MySQLConfig(BaseSettings):
    """MySQL configuration."""
    model_config = SettingsConfigDict(
        env_prefix="MYSQL_",
        case_sensitive=False,
    )
    
    url: Optional[MySQLDsn] = Field(
        default=None,
        description="MySQL URL (e.g., mysql://user:pass@localhost/db)",
    )
    host: str = Field(default="127.0.0.1", description="MySQL host")
    port: int = Field(default=3306, description="MySQL port")
    username: str = Field(default="root", description="MySQL username")
    password: str = Field(default="", description="MySQL password")
    database: str = Field(default="scheduler", description="MySQL database name")
    pool_size: int = Field(default=20, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")
    pool_recycle: int = Field(default=3600, description="Connection recycle seconds")
    pool_pre_ping: bool = Field(default=True, description="Pre-ping connections")
    
    @property
    def connection_string(self) -> str:
        """Get MySQL connection string."""
        if self.url:
            return str(self.url)
        auth = f"{self.username}:{self.password}@" if self.password else f"{self.username}@"
        return f"mysql://{auth}{self.host}:{self.port}/{self.database}"


class ServerConfig(BaseSettings):
    """HTTP server configuration."""
    model_config = SettingsConfigDict(
        env_prefix="SERVER_",
        case_sensitive=False,
    )
    
    host: str = Field(default="0.0.0.0", description="Server bind host")
    port: int = Field(default=8000, description="Server port")
    workers: int = Field(default=1, description="Number of worker processes")
    reload: bool = Field(default=False, description="Enable auto-reload for development")
    access_log: bool = Field(default=True, description="Enable access logging")


class DagConfig(BaseSettings):
    """DAG configuration."""
    model_config = SettingsConfigDict(
        env_prefix="DAG_",
        case_sensitive=False,
    )
    
    config_dir: str = Field(
        default="config/dags",
        description="Directory containing DAG configuration files",
    )
    auto_reload: bool = Field(
        default=True,
        description="Auto-reload DAGs on file changes",
    )
    reload_interval: int = Field(
        default=30,
        description="DAG reload check interval in seconds",
    )


class TaskConfig(BaseSettings):
    """Task execution configuration."""
    model_config = SettingsConfigDict(
        env_prefix="TASK_",
        case_sensitive=False,
    )
    
    default_priority: Literal["very_high", "high", "normal", "low", "tide"] = Field(
        default="normal",
        description="Default task priority",
    )
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    timeout_seconds: int = Field(default=3600, description="Default task timeout")
    result_ttl_seconds: int = Field(
        default=86400 * 7,
        description="How long to keep task results (7 days)",
    )
    max_concurrent: int = Field(default=100, description="Maximum concurrent tasks")
    
    @field_validator("timeout_seconds")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        """Validate timeout is reasonable."""
        if v <= 0:
            raise ValueError("Timeout must be positive")
        if v > 86400 * 7:  # 7 days
            raise ValueError("Timeout cannot exceed 7 days")
        return v


class BackgroundConfig(BaseSettings):
    """Background job configuration."""
    model_config = SettingsConfigDict(
        env_prefix="BACKGROUND_",
        case_sensitive=False,
    )
    
    class ReconcileConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="RECONCILE_")
        enabled: bool = Field(default=False, description="Enable task reconciliation")
        interval_seconds: int = Field(default=60, description="Reconciliation interval")
        stuck_max_per_tick: int = Field(default=20, description="Max stuck tasks per tick")
        stuck_task_max_age_seconds: int = Field(default=300, description="Max age for stuck tasks")
        batch_size: int = Field(default=100, description="Batch size for scanning")
    
    class CronConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="CRON_")
        enabled: bool = Field(default=False, description="Enable cron scheduler")
        poll_interval: int = Field(default=60, description="Cron check interval")
    
    reconcile: ReconcileConfig = Field(default_factory=ReconcileConfig)
    cron: CronConfig = Field(default_factory=CronConfig)


class AgentConfig(BaseSettings):
    """Node agent configuration."""
    model_config = SettingsConfigDict(
        env_prefix="AGENT_",
        case_sensitive=False,
    )
    
    node_id: str = Field(
        default_factory=lambda: "",
        description="Agent node identifier",
    )
    host: str = Field(default="127.0.0.1", description="Agent host")
    port: int = Field(default=9100, description="Agent port")
    heartbeat_interval: float = Field(default=10.0, description="Heartbeat interval")
    owner_ttl_seconds: int = Field(default=30, description="Ownership TTL")


class AppSettings(BaseSettings):
    """Main application settings."""
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )
    
    # Environment
    environment: Literal["development", "testing", "production"] = Field(
        default="development",
        description="Application environment",
    )
    debug: bool = Field(default=False, description="Debug mode")
    
    # Infrastructure
    redis: RedisConfig = Field(default_factory=RedisConfig)
    mysql: MySQLConfig = Field(default_factory=MySQLConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    
    # Application components
    dag: DagConfig = Field(default_factory=DagConfig)
    task: TaskConfig = Field(default_factory=TaskConfig)
    background: BackgroundConfig = Field(default_factory=BackgroundConfig)
    agent: AgentConfig = Field(default_factory=AgentConfig)
    
    @property
    def is_development(self) -> bool:
        return self.environment == "development"
    
    @property
    def is_production(self) -> bool:
        return self.environment == "production"
    
    @property
    def is_testing(self) -> bool:
        return self.environment == "testing"


# Global settings instance
settings = AppSettings()


# Compatibility layer for existing code
def get_settings() -> AppSettings:
    """Get settings singleton (for dependency injection)."""
    return settings


if __name__ == "__main__":
    # Print current settings
    import json
    print(json.dumps(settings.model_dump(), indent=2, default=str))