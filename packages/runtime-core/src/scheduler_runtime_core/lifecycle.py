"""Shared lifecycle management for independently packaged runtime roles."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, List

logger = logging.getLogger(__name__)


class ManagedResource(ABC):
    PHASE_CONSUMER = 10
    PHASE_PROCESSOR = 20
    PHASE_SCHEDULER = 30
    PHASE_OTHER = 40

    @abstractmethod
    async def start(self) -> None:
        ...

    @abstractmethod
    async def stop(self) -> None:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    def resource_type(self) -> str:
        return "generic"

    @property
    def requires_drain(self) -> bool:
        return False

    @property
    def shutdown_phase(self) -> int:
        return self.PHASE_OTHER


class LifecycleManager:
    def __init__(self) -> None:
        self._resources: List[ManagedResource] = []
        self._started = False
        self._stopping = False

    def register_resource(self, resource: ManagedResource) -> None:
        self._resources.append(resource)

    async def start_all(self) -> None:
        if self._started:
            return
        logger.info("Starting lifecycle manager with %d resources", len(self._resources))
        for resource in self._resources:
            try:
                await resource.start()
            except Exception as exc:
                logger.error("Failed to start resource %s: %s", resource.name, exc)
                continue
        self._started = True
        logger.info("Lifecycle manager started")

    async def stop_all(self, drain_timeout: float = 30.0) -> None:
        if self._stopping or not self._started:
            return
        self._stopping = True
        logger.info("LifecycleManager: starting graceful shutdown")

        consumers = []
        processors = []
        schedulers = []
        others = []

        for resource in self._resources:
            phase = getattr(resource, "shutdown_phase", self.PHASE_OTHER)
            if phase == ManagedResource.PHASE_CONSUMER:
                consumers.append(resource)
            elif phase == ManagedResource.PHASE_PROCESSOR:
                processors.append(resource)
            elif phase == ManagedResource.PHASE_SCHEDULER:
                schedulers.append(resource)
            else:
                others.append(resource)

        if consumers:
            for resource in consumers:
                try:
                    await resource.stop()
                except Exception:
                    pass

        if consumers or processors:
            import asyncio
            await asyncio.sleep(drain_timeout)

        for bucket in (processors, schedulers, others):
            for resource in bucket:
                try:
                    await resource.stop()
                except Exception:
                    pass

        self._started = False
        self._stopping = False
        logger.info("LifecycleManager: graceful shutdown complete")


class TaskReconcilerResource(ManagedResource):
    def __init__(self, reconciler: Any) -> None:
        self._reconciler = reconciler

    @property
    def name(self) -> str:
        return "TaskReconciler"

    @property
    def shutdown_phase(self) -> int:
        return self.PHASE_PROCESSOR

    async def start(self) -> None:
        if hasattr(self._reconciler, 'start'):
            await self._reconciler.start()

    async def stop(self) -> None:
        if hasattr(self._reconciler, 'stop'):
            await self._reconciler.stop()


class CronSchedulerResource(ManagedResource):
    def __init__(self, scheduler: Any) -> None:
        self._scheduler = scheduler

    @property
    def name(self) -> str:
        return "CronScheduler"

    @property
    def shutdown_phase(self) -> int:
        return self.PHASE_SCHEDULER

    async def start(self) -> None:
        if hasattr(self._scheduler, 'start'):
            await self._scheduler.start()

    async def stop(self) -> None:
        if hasattr(self._scheduler, 'stop'):
            await self._scheduler.stop()


class CompensationServiceResource(ManagedResource):
    def __init__(self, service: Any) -> None:
        self._service = service

    @property
    def name(self) -> str:
        return "CompensationService"

    @property
    def shutdown_phase(self) -> int:
        return self.PHASE_PROCESSOR

    async def start(self) -> None:
        if hasattr(self._service, 'start'):
            await self._service.start()

    async def stop(self) -> None:
        if hasattr(self._service, 'stop'):
            await self._service.stop()


class CallbackDispatcherResource(ManagedResource):
    def __init__(self, service: Any) -> None:
        self._service = service

    @property
    def name(self) -> str:
        return "CallbackDispatcher"

    @property
    def shutdown_phase(self) -> int:
        return self.PHASE_PROCESSOR

    async def start(self) -> None:
        if hasattr(self._service, 'start'):
            await self._service.start()

    async def stop(self) -> None:
        if hasattr(self._service, 'stop'):
            await self._service.stop()
