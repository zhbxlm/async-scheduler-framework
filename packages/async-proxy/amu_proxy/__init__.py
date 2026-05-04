"""amu-proxy — Ray AMU Async Proxy.

Sidecar for long-running service async wrapping.
"""

__version__ = "0.1.0"

from amu_proxy.async_service_proxy import create_proxy_app, main as main_service_proxy
from amu_proxy.async_command_proxy import AsyncCommandProxy, main as main_command_proxy

__all__ = [
    "create_proxy_app",
    "main_service_proxy",
    "AsyncCommandProxy",
    "main_command_proxy",
]
