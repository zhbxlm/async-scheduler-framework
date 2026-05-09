"""task-api package-owned lifecycle manager."""
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
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

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
                logger.debug("Starting resource: %s", resource.name)
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

        def _infer_phase(resource: ManagedResource) -> int:
            rtype = getattr(resource, "resource_type", "generic")
            rname = resource.name.lower()
            if "consumer" in rtype.lower() or "consumer" in rname:
                return ManagedResource.PHASE_CONSUMER
            if ("processor" in rtype.lower() or "executor" in rtype.lower() or "reconciler" in rtype.lower() or "processor" in rname or "executor" in rname or "reconciler" in rname):
                return ManagedResource.PHASE_PROCESSOR
            if ("scheduler" in rtype.lower() or "cron" in rtype.lower() or "scheduler" in rname or "cron" in rname):
                return ManagedResource.PHASE_SCHEDULER
            return ManagedResource.PHASE_OTHER

        consumers, processors, schedulers, others = [], [], [], []
        for resource in self._resources:
            phase = getattr(resource, "shutdown_phase", None)
            if phase is None:
                phase = _infer_phase(resource)
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
                except Exception as exc:
                    logger.warning("Error stopping consumer %s: %s", resource.name, exc)
        if consumers or processors:
            import asyncio
            await asyncio.sleep(drain_timeout)
        if processors:
            for resource in processors:
                try:
                    await resource.stop()
                except Exception as exc:
                    logger.warning("Error stopping processor %s: %s", resource.name, exc)
        if schedulers:
            for resource in schedulers:
                try:
                    await resource.stop()
                except Exception as exc:
                    logger.warning("Error stopping scheduler %s: %s", resource.name, exc)
        if others:
            for resource in others:
                try:
                    await resource.stop()
                except Exception as exc:
                    logger.warning("Error stopping resource %s: %s", resource.name, exc)
        self._started = False
        self._stopping = False
        logger.info("LifecycleManager: graceful shutdown complete")
