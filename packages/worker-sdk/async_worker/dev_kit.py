"""WorkerDevKit — development and local-testing tools for writing Workers.

Provides helpers to run, mock and assert Workers without Ray or Redis:
  - run_local_test(): run a Worker class synchronously in-process
  - WorkerDevKit: mock capability registration and output assertions
"""
from __future__ import annotations

import json
from typing import Any, Callable, Type


def run_local_test(
    worker_class: Type,
    input_data: dict[str, Any],
    config: dict[str, Any] | None = None,
    capability: str = "local_test",
) -> Any:
    """Instantiate *worker_class* and call run() with *input_data*.

    No Ray, no Redis required.  Useful for unit testing Workers.

    Parameters
    ----------
    worker_class : Type
        The Worker class to test (must implement run()).
    input_data : dict
        Input payload dict passed to run().
    config : dict | None
        Optional init config dict passed to __init__.
    capability : str
        Capability name string for __init__ (default "local_test").

    Returns
    -------
    Any
        The return value of worker.run(input_data).

    Raises
    ------
    AttributeError
        If worker_class does not implement run().
    """
    worker = worker_class(capability=capability, config=config or {})
    if not hasattr(worker, "run"):
        raise AttributeError(f"{worker_class.__name__} must implement run(input_data)")
    return worker.run(input_data)


class WorkerDevKit:
    """Local development toolkit for Worker authoring and testing."""

    def __init__(self) -> None:
        self._mock_capabilities: dict[str, Callable] = {}

    def mock_capability(self, name: str, handler: Callable) -> None:
        """Register a mock capability handler for local dispatch simulation.

        Parameters
        ----------
        name : str
            Capability name (e.g. "cap_preprocess").
        handler : Callable
            Callable(input_data) → output_data.
        """
        self._mock_capabilities[name] = handler

    def dispatch(self, capability: str, input_data: dict[str, Any]) -> Any:
        """Dispatch input_data to the mock capability handler.

        Parameters
        ----------
        capability : str
            Capability name.
        input_data : dict
            Input data to pass to handler.

        Returns
        -------
        Any
            Result from the mock handler.

        Raises
        ------
        KeyError
            If capability not registered.
        """
        if capability not in self._mock_capabilities:
            raise KeyError(f"No mock registered for capability '{capability}'")
        return self._mock_capabilities[capability](input_data)

    def local_runner(
        self,
        worker_class: Type,
        input_data: dict[str, Any],
        config: dict[str, Any] | None = None,
        capability: str = "local_test",
    ) -> Any:
        """Run a worker locally (alias for module-level run_local_test).

        Parameters
        ----------
        worker_class : Type
            The Worker class to test.
        input_data : dict
            Input data for the worker.
        config : dict | None
            Optional configuration.
        capability : str
            Capability name.

        Returns
        -------
        Any
            Result from worker.run().
        """
        return run_local_test(worker_class, input_data, config=config, capability=capability)

    def assert_output(
        self,
        result: Any,
        expected_keys: list[str],
        *,
        allow_extra: bool = True,
    ) -> None:
        """Assert that *result* is a dict containing all *expected_keys*.

        Parameters
        ----------
        result : Any
            The output from worker.run().
        expected_keys : list[str]
            List of required top-level keys.
        allow_extra : bool
            If False, raise if result contains unexpected keys.

        Raises
        ------
        AssertionError
            On missing or unexpected keys.
        """
        if not isinstance(result, dict):
            raise AssertionError(f"Expected dict output, got {type(result).__name__}")
        missing = [k for k in expected_keys if k not in result]
        if missing:
            raise AssertionError(f"Output missing keys: {missing}. Got: {list(result.keys())}")
        if not allow_extra:
            extra = [k for k in result if k not in expected_keys]
            if extra:
                raise AssertionError(f"Output has unexpected keys: {extra}")

    def pretty_print(self, result: Any) -> None:
        """Pretty-print result as JSON.

        Parameters
        ----------
        result : Any
            Result to print.
        """
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))