"""scheduler_proxy — embed async task dispatch into any service.

    from scheduler_proxy import AsyncCommandProxy, AsyncServiceProxy
"""
from scheduler_proxy.command import AsyncCommandProxy
from scheduler_proxy.service import AsyncServiceProxy

__all__ = ["AsyncCommandProxy", "AsyncServiceProxy"]
__version__ = "0.1.0"
