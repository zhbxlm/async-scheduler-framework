"""Compatibility layer for migrating from old config to pydantic-settings."""
from __future__ import annotations

from config.settings_pydantic import settings as new_settings


# Recreate old-style dataclasses for backward compatibility
class RedisConfig:
    def __init__(self):
        self.url = str(new_settings.redis.url) if new_settings.redis.url else None
        self.host = new_settings.redis.host
        self.port = new_settings.redis.port
        self.password = new_settings.redis.password
        self.db = new_settings.redis.db
        self.max_connections = new_settings.redis.max_connections
        self.decode_responses = new_settings.redis.decode_responses


class MySQLConfig:
    def __init__(self):
        self.url = str(new_settings.mysql.url) if new_settings.mysql.url else None
        self.host = new_settings.mysql.host
        self.port = new_settings.mysql.port
        self.username = new_settings.mysql.username
        self.password = new_settings.mysql.password
        self.database = new_settings.mysql.database
        self.pool_size = new_settings.mysql.pool_size
        self.max_overflow = new_settings.mysql.max_overflow
        self.pool_recycle = new_settings.mysql.pool_recycle
        self.pool_pre_ping = new_settings.mysql.pool_pre_ping
        self.enabled = bool(new_settings.mysql.url)


class ServerConfig:
    def __init__(self):
        self.host = new_settings.server.host
        self.port = new_settings.server.port
        self.workers = new_settings.server.workers
        self.reload = new_settings.server.reload
        self.access_log = new_settings.server.access_log


class InfraConfig:
    def __init__(self):
        self.redis = RedisConfig()
        self.mysql = MySQLConfig()
        self.server = ServerConfig()


class DagConfig:
    def __init__(self):
        self.config_dir = new_settings.dag.config_dir
        self.auto_reload = new_settings.dag.auto_reload
        self.reload_interval = new_settings.dag.reload_interval


class TaskConfig:
    def __init__(self):
        self.default_priority = new_settings.task.default_priority
        self.max_retries = new_settings.task.max_retries
        self.timeout_seconds = new_settings.task.timeout_seconds
        self.result_ttl_seconds = new_settings.task.result_ttl_seconds
        self.max_concurrent = new_settings.task.max_concurrent


class BackgroundConfig:
    class ReconcileConfig:
        def __init__(self):
            self.enabled = new_settings.background.reconcile.enabled
            self.interval_seconds = new_settings.background.reconcile.interval_seconds
            self.stuck_max_per_tick = new_settings.background.reconcile.stuck_max_per_tick
            self.stuck_task_max_age_seconds = new_settings.background.reconcile.stuck_task_max_age_seconds
            self.batch_size = new_settings.background.reconcile.batch_size
    
    class CronConfig:
        def __init__(self):
            self.enabled = new_settings.background.cron.enabled
            self.poll_interval = new_settings.background.cron.poll_interval
    
    def __init__(self):
        self.reconcile = self.ReconcileConfig()
        self.cron = self.CronConfig()


class AgentConfig:
    def __init__(self):
        self.node_id = new_settings.agent.node_id
        self.host = new_settings.agent.host
        self.port = new_settings.agent.port
        self.heartbeat_interval = new_settings.agent.heartbeat_interval
        self.owner_ttl_seconds = new_settings.agent.owner_ttl_seconds


class Settings:
    """Old-style settings singleton for backward compatibility."""
    def __init__(self):
        self.infra = InfraConfig()
        self.dag = DagConfig()
        self.task = TaskConfig()
        self.agent = AgentConfig()
        self.background = BackgroundConfig()
        
        # Import TenantConfig from the existing dataclass module
        from config._tenant import TenantConfig
        self.tenant = TenantConfig()
        
        # Convenience aliases
        self.redis = self.infra.redis
        self.mysql = self.infra.mysql


# Create singleton instance
settings = Settings()

# Also export the new settings for new code
new_settings = new_settings


if __name__ == "__main__":
    # Test compatibility
    print("✅ 兼容层测试:")
    print(f"Redis URL: {settings.redis.url}")
    print(f"MySQL enabled: {settings.mysql.enabled}")
    print(f"Task timeout: {settings.task.timeout_seconds}")
    print(f"Reconcile enabled: {settings.background.reconcile.enabled}")