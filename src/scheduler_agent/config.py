"""Agent configuration — aligned with docs/deepwiki-reference/节点代理.md"""
from __future__ import annotations
import os

class AgentConfig:
    node_id: str = os.getenv("AGENT_NODE_ID", "")
    host: str = os.getenv("AGENT_HOST", "127.0.0.1")
    port: int = int(os.getenv("AGENT_PORT", "9100"))
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    heartbeat_interval: float = float(os.getenv("AGENT_HEARTBEAT_INTERVAL", "10"))
    owner_ttl_seconds: int = int(os.getenv("AGENT_OWNER_TTL", "30"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
