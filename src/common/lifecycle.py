"""Lifecycle management for background tasks and resources."""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Task
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional

logger = logging.getLogger(__name__)


class ManagedResource(ABC):
    """Base class for resources that need lifecycle management."""
    
    @abstractmethod
    async def start(self) -> None:
        """Start the resource."""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the resource gracefully."""
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Resource name for logging."""
        pass


class LifecycleManager:
    """Manages startup and shutdown of resources and background tasks."""
    
    def __init__(self) -> None:
        self._resources: List[ManagedResource] = []
        self._tasks: List[Task[Any]] = []
        self._started = False
        self._stopping = False
    
    def register_resource(self, resource: ManagedResource) -> None:
        """Register a resource for lifecycle management."""
        self._resources.append(resource)
    
    def track_task(self, task: Task[Any]) -> None:
        """Track an asyncio task for cleanup."""
        self._tasks.append(task)
    
    def create_and_track_task(self, coro, name: str = "") -> Task[Any]:
        """Create and track an asyncio task."""
        task = asyncio.create_task(coro, name=name)
        self.track_task(task)
        return task
    
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
    
    async def stop_all(self, timeout: float = 30.0) -> None:
        """Stop all resources and cancel tracked tasks."""
        if self._stopping or not self._started:
            return
        
        self._stopping = True
        logger.info("Stopping lifecycle manager")
        
        # 1. Cancel tracked tasks
        if self._tasks:
            logger.info("Cancelling %d tracked tasks", len(self._tasks))
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete (with timeout)
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for tasks to complete")
            except Exception as exc:
                logger.warning("Error waiting for tasks: %s", exc)
        
        # 2. Stop resources in reverse order (dependency order)
        for resource in reversed(self._resources):
            try:
                logger.debug("Stopping resource: %s", resource.name)
                await resource.stop()
            except Exception as exc:
                logger.warning("Error stopping resource %s: %s", resource.name, exc)
        
        self._started = False
        self._stopping = False
        logger.info("Lifecycle manager stopped")
    
    @property
    def is_started(self) -> bool:
        return self._started
    
    @property
    def is_stopping(self) -> bool:
        return self._stopping
    
    def get_status(self) -> Dict[str, Any]:
        """Get status of all managed resources."""
        return {
            "started": self._started,
            "stopping": self._stopping,
            "resource_count": len(self._resources),
            "task_count": len(self._tasks),
            "tasks": [
                {
                    "name": task.get_name() or f"task_{i}",
                    "done": task.done(),
                    "cancelled": task.cancelled(),
                }
                for i, task in enumerate(self._tasks)
            ],
        }


# ---------------------------------------------------------------------------
# FastAPI lifespan integration
# ---------------------------------------------------------------------------

_lifecycle_manager: Optional[LifecycleManager] = None


def get_lifecycle_manager() -> LifecycleManager:
    """Get or create the global lifecycle manager."""
    global _lifecycle_manager
    if _lifecycle_manager is None:
        _lifecycle_manager = LifecycleManager()
    return _lifecycle_manager


@asynccontextmanager
async def app_lifespan() -> AsyncGenerator[Dict[str, Any], None]:
    """FastAPI lifespan context manager with lifecycle management."""
    manager = get_lifecycle_manager()
    
    try:
        # Startup
        await manager.start_all()
        yield {"lifecycle_manager": manager}
    finally:
        # Shutdown
        await manager.stop_all()


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