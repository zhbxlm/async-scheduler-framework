"""BaseWorkerActor — Template-method base class for Worker actors.

Implements the three-phase hook pattern: pre_process → call → post_process.

Usage (zero-dependency mode, no Ray needed)::

    class MyWorker:
        _default_options = {"num_cpus": 1}

        def __init__(self, capability: str, config: dict | None = None):
            self.capability = capability
            self.model = load_model(config)

        def run(self, input_data: dict) -> dict:
            return {"result": self.model.infer(input_data)}

        def health_check(self) -> dict:
            return {"status": "healthy"}

Usage (BaseWorkerActor mode)::

    class MyWorker(BaseWorkerActor):
        def call(self, data: dict) -> dict:
            return {"result": "..."}
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class _BaseWorkerActorImpl:
    """Unwrapped implementation class — subclassable with plain Python.

    ``@ray.remote`` decoration is applied at the bottom of this file
    to produce the ``BaseWorkerActor`` alias when Ray is available.
    """

    _default_options: dict[str, Any] = {"num_cpus": 1}

    def __init__(self, capability: str, config: dict[str, Any] | None = None) -> None:
        """Initialize the worker.

        Parameters
        ----------
        capability : str
            The capability identifier for this worker.
        config : dict | None
            Optional configuration dictionary.
        """
        self.capability = capability
        self.config = config or {}
        self._call_count = 0

    # ------------------------------------------------------------------
    # Template method
    # ------------------------------------------------------------------

    def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        """Main entry point. Invokes pre_process → call → post_process.

        Parameters
        ----------
        input_data : dict
            Input data for processing.

        Returns
        -------
        dict
            Processed result.
        """
        processed = self.pre_process(input_data)
        raw_result = self.call(processed)
        result = self.post_process(raw_result)
        self._call_count += 1
        return result

    # ------------------------------------------------------------------
    # Hooks (override in subclass)
    # ------------------------------------------------------------------

    def pre_process(self, input_data: dict[str, Any]) -> dict[str, Any]:
        """Input pre-processing hook. Default: identity.

        Parameters
        ----------
        input_data : dict
            Raw input data.

        Returns
        -------
        dict
            Pre-processed input data.
        """
        return input_data

    def call(self, data: dict[str, Any]) -> dict[str, Any]:
        """Core business logic hook. Must be overridden.

        Parameters
        ----------
        data : dict
            Pre-processed input data.

        Returns
        -------
        dict
            Raw result.

        Raises
        ------
        NotImplementedError
            If not overridden in subclass.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement call()"
        )

    def post_process(self, result: dict[str, Any]) -> dict[str, Any]:
        """Result post-processing hook. Default: identity.

        Parameters
        ----------
        result : dict
            Raw result.

        Returns
        -------
        dict
            Post-processed result.
        """
        return result

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def health_check(self) -> dict[str, Any]:
        """Worker health status. Override to add custom checks.

        Returns
        -------
        dict
            Health status dictionary.
        """
        return {
            "status": "healthy",
            "capability": self.capability,
            "call_count": self._call_count,
        }


# ---------------------------------------------------------------------------
# Apply @ray.remote if available, else alias directly
# ---------------------------------------------------------------------------

try:
    import ray  # type: ignore[import]
    BaseWorkerActor = ray.remote(_BaseWorkerActorImpl)
except ImportError:
    BaseWorkerActor = _BaseWorkerActorImpl  # type: ignore[assignment,misc]
