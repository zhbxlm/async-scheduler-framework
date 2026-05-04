"""RayDataClient — aligned with docs/deepwiki-reference/RayData 集成.md

Encapsulates HTTP submission of tasks to a Ray cluster, bearer-token auth,
artifact runtime_env injection, and response normalisation.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from src.models.cluster import ClusterInfo
from src.models.task import TaskInfo

logger = logging.getLogger(__name__)

_DEFAULT_SUBMIT_PATH = "/api/jobs/"
_DEFAULT_TIMEOUT_SECONDS = 15


class RayDataClient:
    """HTTP client for submitting tasks natively to a Ray cluster.

    Usage::

        client = RayDataClient(api_token="my-token", timeout_seconds=15)
        result = client.submit_task(cluster, task_info,
                                    remote_code_fetcher=fetcher)
        # result = {"submission_id": ..., "cluster_id": ...,
        #           "submit_url": ..., "response": ...}
    """

    def __init__(
        self,
        *,
        api_token: str | None = None,
        submit_path: str = _DEFAULT_SUBMIT_PATH,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._api_token = api_token
        self._submit_path = submit_path
        self._timeout = timeout_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit_task(
        self,
        cluster: ClusterInfo,
        task: TaskInfo,
        *,
        remote_code_fetcher: "RemoteCodeFetcher | None" = None,
    ) -> dict[str, Any]:
        """Submit *task* to *cluster* via the Ray jobs HTTP API.

        Returns::

            {
                "submission_id": str,
                "cluster_id": str,
                "submit_url": str,
                "response": dict,
            }

        Raises:
            RuntimeError: if ``cluster.ray_head_address`` is empty.
            httpx.HTTPError: on network / HTTP error.
        """
        if not cluster.ray_head_address:
            raise RuntimeError(
                f"cluster {cluster.cluster_id!r} has no ray_head_address"
            )

        submit_url = cluster.ray_head_address.rstrip("/") + self._submit_path
        payload = self._build_payload(task, remote_code_fetcher=remote_code_fetcher)
        headers = self._build_headers()

        logger.debug(
            "RayDataClient.submit_task task_id=%s cluster=%s url=%s",
            task.task_id,
            cluster.cluster_id,
            submit_url,
        )

        with httpx.Client(timeout=self._timeout) as http:
            response = http.post(submit_url, json=payload, headers=headers)
            response.raise_for_status()

        raw = _safe_json(response)
        submission_id = (
            raw.get("submission_id")
            or raw.get("job_id")
            or task.task_id
        )

        return {
            "submission_id": submission_id,
            "cluster_id": cluster.cluster_id,
            "submit_url": submit_url,
            "response": raw,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._api_token:
            headers["Authorization"] = f"Bearer {self._api_token}"
        return headers

    @staticmethod
    def _build_payload(
        task: TaskInfo,
        *,
        remote_code_fetcher: "RemoteCodeFetcher | None" = None,
    ) -> dict[str, Any]:
        """Build the RayData submission payload from *task*.

        If ``task.input_data`` contains a non-empty ``raydata_request``
        dict, it is used verbatim.  Otherwise a standard payload is
        constructed.  When ``task.artifact_url`` is set, the artifact is
        fetched and ``runtime_env.working_dir`` is set accordingly.
        """
        # Custom override: user supplies a fully-formed RayData request
        custom = task.input_data.get("raydata_request")
        if custom and isinstance(custom, dict):
            return custom

        # Standard payload
        payload: dict[str, Any] = {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "input_data": task.input_data,
            "metadata": task.metadata_json,
            "callback_url": task.callback_url or "",
        }

        # Inject runtime_env when artifact is configured
        if task.artifact_url and remote_code_fetcher is not None:
            local_dir = remote_code_fetcher.fetch_artifact(
                task.artifact_url, task.artifact_sha256 or None
            )
            payload["runtime_env"] = {"working_dir": local_dir}

        return payload


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _safe_json(response: httpx.Response) -> dict[str, Any]:
    """Parse response JSON safely, returning empty dict on failure."""
    try:
        data = response.json()
        if isinstance(data, dict):
            return data
        return {"raw_response": data}
    except Exception:
        return {"raw_response": response.text}
