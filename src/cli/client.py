"""HTTP client for CLI."""
import os
from typing import Any
import httpx


class ApiClient:
    def __init__(self, base_url: str, api_key: str = ""):
        self._base = base_url.rstrip("/")
        self._key = api_key or os.getenv("SCHEDULER_API_KEY", "")
        self._client = httpx.Client(timeout=30.0)

    def get(self, path: str, **kwargs) -> Any:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, json: dict, **kwargs) -> Any:
        return self._request("POST", path, json=json, **kwargs)

    def put(self, path: str, json: dict, **kwargs) -> Any:
        return self._request("PUT", path, json=json, **kwargs)

    def delete(self, path: str, **kwargs) -> Any:
        return self._request("DELETE", path, **kwargs)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self._base}{path}"
        headers = kwargs.pop("headers", {})
        if self._key:
            headers["Authorization"] = f"Bearer {self._key}"
        resp = self._client.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return None
