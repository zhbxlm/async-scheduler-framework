"""Lifecycle management for background resources."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


class ManagedResource(ABC):
    """Base class for resources that need lifecycle management."""

    @abstractmethod
    async def start(self) -> None:
        """Start the resource."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the resource gracefully."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Resource name for logging."""
    
    @property
    def resource_type(self) -> str:
        """Resource type for shutdown ordering."""
        return "generic"
    
    @property
    def requires_drain(self) -> bool:
        """Whether this resource needs drain time before shutdown."""
        return False


class LifecycleManager:
    """Manages startup and shutdown of resources."""

    def __init__(self) -> None:
        self._resources: List[ManagedResource] = []
        self._started = False
        self._stopping = False

    def register_resource(self, resource: ManagedResource) -> None:
        """Register a resource for lifecycle management."""
        self._resources.append(resource)

    async def start_all(self) -> None:
        """Start all registered resources."""
        if self._started:
            return

        logger.info("Starting lifecycle manager with %d resources", len(self._resources))
        for resource in self._resources:
            try:
                logger.debug("Starting resource: %s", resource.name)
                await resource.start()
            except Exception as exc:
                logger.error("Failed to start resource %s: %s", resource.name, exc)
                # Continue starting other resources
                continue

        self._started = True
        logger.info("Lifecycle manager started")

    async def stop_all(self, drain_timeout: float = 30.0) -> None:
        """Stop all resources in correct order:
        1. First stop consumers (stop accepting new work)
        2. Wait for drain period (let running work complete)
        3. Stop processors and reconcilers
        4. Stop schedulers and other background tasks
        """
        if self._stopping or not self._started:
            return

        self._stopping = True
        logger.info("LifecycleManager: starting graceful shutdown")

        # Categorize resources by type
        consumers = []
        processors = []
        schedulers = []
        others = []
        
        for resource in self._resources:
            rtype = getattr(resource, 'resource_type', 'generic')
            rname = resource.name.lower()
            
            if 'consumer' in rtype.lower() or 'consumer' in rname:
                consumers.append(resource)
            elif 'processor' in rtype.lower() or 'executor' in rtype.lower() or \
                 'reconciler' in rtype.lower() or 'processor' in rname or \
                 'executor' in rname or 'reconciler' in rname:
                processors.append(resource)
            elif 'scheduler' in rtype.lower() or 'cron' in rtype.lower() or \
                 'scheduler' in rname or 'cron' in rname:
                schedulers.append(resource)
            else:
                others.append(resource)
        
        # 1. Stop consumers first (stop accepting new work)
        if consumers:
            logger.info("LifecycleManager: stopping %d consumer resource(s)", len(consumers))
            for resource in consumers:
                try:
                    logger.debug("Stopping consumer: %s", resource.name)
                    await resource.stop()
                except Exception as exc:
                    logger.warning("Error stopping consumer %s: %s", resource.name, exc)
        
        # 2. Drain period - wait for running tasks to complete
        if consumers or processors:
            logger.info("LifecycleManager: waiting %.1f seconds for drain", drain_timeout)
            import asyncio
            await asyncio.sleep(drain_timeout)
        
        # 3. Stop processors and reconcilers
        if processors:
            logger.info("LifecycleManager: stopping %d processor resource(s)", len(processors))
            for resource in processors:
                try:
                    logger.debug("Stopping processor: %s", resource.name)
                    await resource.stop()
                except Exception as exc:
                    logger.warning("Error stopping processor %s: %s", resource.name, exc)
        
        # 4. Stop schedulers
        if schedulers:
            logger.info("LifecycleManager: stopping %d scheduler resource(s)", len(schedulers))
            for resource in schedulers:
                try:
                    logger.debug("Stopping scheduler: %s", resource.name)
                    await resource.stop()
                except Exception as exc:
                    logger.warning("Error stopping scheduler %s: %s", resource.name, exc)
        
        # 5. Stop other resources
        if others:
            logger.info("LifecycleManager: stopping %d other resource(s)", len(others))
            for resource in others:
                try:
                    logger.debug("Stopping resource: %s", resource.name)
                    await resource.stop()
                except Exception as exc:
                    logger.warning("Error stopping resource %s: %s", resource.name, exc)

        self._started = False
        self._stopping = False
        logger.info("LifecycleManager: graceful shutdown complete")

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def is_stopping(self) -> bool:
        return self._stopping


# ---------------------------------------------------------------------------
# Global lifecycle manager
# ---------------------------------------------------------------------------

_lifecycle_manager: Optional[LifecycleManager] = None


def get_lifecycle_manager() -> LifecycleManager:
    """Get or create the global lifecycle manager."""
    global _lifecycle_manager
    if _lifecycle_manager is None:
        _lifecycle_manager = LifecycleManager()
    return _lifecycle_manager


# ---------------------------------------------------------------------------
# Resource adapters for existing components
# ---------------------------------------------------------------------------

class TaskReconcilerResource(ManagedResource):
    """Adapter for TaskReconciler."""

    def __init__(self, reconciler: Any) -> None:
        self._reconciler = reconciler

    @property
    def name(self) -> str:
        return "TaskReconciler"

    async def start(self) -> None:
        if hasattr(self._reconciler, 'start'):
            await self._reconciler.start()

    async def stop(self) -> None:
        if hasattr(self._reconciler, 'stop'):
            await self._reconciler.stop()


class CronSchedulerResource(ManagedResource):
    """Adapter for CronScheduler."""

    def __init__(self, scheduler: Any) -> None:
        self._scheduler = scheduler

    @property
    def name(self) -> str:
        return "CronScheduler"

    async def start(self) -> None:
        if hasattr(self._scheduler, 'start'):
            await self._scheduler.start()

    async def stop(self) -> None:
        if hasattr(self._scheduler, 'stop'):
            await self._scheduler.stop()


class CompensationServiceResource(ManagedResource):
    """Adapter for CompensationService."""

    def __init__(self, service: Any) -> None:
        self._service = service

    @property
    def name(self) -> str:
        return "CompensationService"

    async def start(self) -> None:
        if hasattr(self._service, 'start'):
            await self._service.start()

    async def stop(self) -> None:
        if hasattr(self._service, 'stop'):
            await self._service.stop()


class CallbackDispatcherResource(ManagedResource):
    """Adapter for CallbackDispatchService."""

    def __init__(self, service: Any) -> None:
        self._service = service

    @property
    def name(self) -> str:
        return "CallbackDispatcher"

    async def start(self) -> None:
        if hasattr(self._service, 'start'):
            await self._service.start()

    async def stop(self) -> None:
        if hasattr(self._service, 'stop'):
            await self._service.stop()
