"""Unified registry interface definitions."""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional, List, Any

T = TypeVar("T")


class IRegistry(Generic[T], ABC):
    """Base interface for all registries."""
    
    @abstractmethod
    async def get(self, identifier: str, **kwargs) -> Optional[T]:
        """Get an item by identifier."""
        pass
    
    @abstractmethod
    async def set(self, identifier: str, value: T, **kwargs) -> bool:
        """Set or update an item."""
        pass
    
    @abstractmethod
    async def delete(self, identifier: str, **kwargs) -> bool:
        """Delete an item."""
        pass
    
    @abstractmethod
    async def list(self, **kwargs) -> List[str]:
        """List all item identifiers."""
        pass


class ITenantRegistry(IRegistry[T], ABC):
    """Registry with tenant isolation."""
    
    @abstractmethod
    async def get(self, tenant_id: str, item_id: str) -> Optional[T]:
        """Get an item for a specific tenant."""
        pass
    
    @abstractmethod
    async def set(self, tenant_id: str, item_id: str, value: T) -> bool:
        """Set an item for a specific tenant."""
        pass
    
    @abstractmethod
    async def delete(self, tenant_id: str, item_id: str) -> bool:
        """Delete an item for a specific tenant."""
        pass
    
    @abstractmethod
    async def list(self, tenant_id: str) -> List[str]:
        """List items for a specific tenant."""
        pass
    
    @abstractmethod
    async def list_all_tenants(self) -> List[str]:
        """List all tenants in the registry."""
        pass


# ---------------------------------------------------------------------------
# Adapter for existing registries
# ---------------------------------------------------------------------------

class RegistryAdapter:
    """Adapts existing registry classes to unified interface."""
    
    @staticmethod
    def create_tenant_adapter(registry_instance: Any, key_prefix: str) -> ITenantRegistry:
        """Create adapter for registry with tenant support."""
        class Adapter(ITenantRegistry):
            def __init__(self, registry: Any):
                self._registry = registry
            
            async def get(self, tenant_id: str, item_id: str) -> Optional[Any]:
                # Try different method signatures
                if hasattr(self._registry, 'get'):
                    return await self._registry.get(tenant_id, item_id)
                return None
            
            async def set(self, tenant_id: str, item_id: str, value: Any) -> bool:
                if hasattr(self._registry, 'set'):
                    return await self._registry.set(tenant_id, item_id, value)
                return False
            
            async def delete(self, tenant_id: str, item_id: str) -> bool:
                if hasattr(self._registry, 'delete'):
                    return await self._registry.delete(tenant_id, item_id)
                return False
            
            async def list(self, tenant_id: str) -> List[str]:
                if hasattr(self._registry, 'list'):
                    return await self._registry.list(tenant_id)
                return []
            
            async def list_all_tenants(self) -> List[str]:
                if hasattr(self._registry, 'list_all_tenants'):
                    return await self._registry.list_all_tenants()
                return []
        
        return Adapter(registry_instance)


# ---------------------------------------------------------------------------
# Decorator for error handling
# ---------------------------------------------------------------------------

def with_error_handling(registry_cls):
    """Decorator to add error handling to registry methods."""
    class ErrorHandledRegistry(registry_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            from src.common.error_handling import log_errors
        
        # We'll add method-specific decorators dynamically
        pass
    
    return ErrorHandledRegistry