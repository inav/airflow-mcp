"""Async HTTP client for the Airflow REST API.

Speaks both ``/api/v1`` (legacy) and ``/api/v2`` (stable on 2.4+,
*only* on 3.0+). The selected path comes from
:func:`airflow_mcp.versioning.resolve_capabilities` — the client
itself has no version knowledge beyond the ``Capabilities`` it was
constructed with.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Literal
from urllib.parse import urljoin

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import Settings
from .errors import AirflowAPIError, AirflowAuthError, AirflowConfigError, AirflowNotFoundError
from .versioning import Capabilities, override_api_version, resolve_capabilities

log = logging.getLogger(__name__)

# Status codes we transparently retry on (transient, server-side, rate-limit).
_RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504}

# Methods that are safe to retry (idempotent).
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


class AirflowClient:
    """Thin async wrapper around the Airflow REST API.

    Constructed with a :class:`Settings` and a resolved
    :class:`Capabilities`. Use as an async context manager, or call
    :meth:`start` / :meth:`aclose` explicitly.
    """

    def __init__(self, settings: Settings, caps: Capabilities) -> None:
        self._settings = settings
        self._caps = caps
        base = settings.base_url.rstrip("/") + "/"
        if not base.startswith(("http://", "https://")):
            raise AirflowConfigError(f"base_url must be http(s): got {settings.base_url!r}")
        self._base = base
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": self._settings.user_agent,
        }
        auth: tuple[str, str] | None = None
        if self._settings.auth_mode == "token":
            headers["Authorization"] = f"Bearer {self._settings.token}"
        else:
            assert self._settings.username and self._settings.password
            auth = (self._settings.username, self._settings.password)

        self._client = httpx.AsyncClient(
            base_url=self._base,
            headers=headers,
            auth=auth,
            timeout=httpx.Timeout(self._settings.timeout),
            verify=self._settings.verify_ssl,
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> AirflowClient:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------ properties

    @property
    def capabilities(self) -> Capabilities:
        return self._caps

    @property
    def api_version(self) -> Literal["v1", "v2"]:
        return self._caps.api_version

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise AirflowConfigError("AirflowClient used before start() / outside context")
        return self._client

    def _api(self, path: str) -> str:
        return urljoin(self._base, f"api/{self._caps.api_version}/{path.lstrip('/')}")

    # ------------------------------------------------------------------ internals

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Mapping[str, Any] | None = None,
    ) -> Any:
        """Issue an HTTP request and return the parsed JSON body."""
        client = self._http
        url = self._api(path)
        retrying: AsyncRetrying | None = None
        if method.upper() in _SAFE_METHODS and self._settings.max_retries > 0:
            retrying = AsyncRetrying(
                stop=stop_after_attempt(self._settings.max_retries + 1),
                wait=wait_exponential(multiplier=self._settings.retry_backoff, min=0.25, max=8.0),
                retry=retry_if_exception_type((AirflowAPIError,)),
                reraise=True,
            )

        async def _do() -> Any:
            try:
                resp = await client.request(method, url, params=params, json=json)
            except httpx.HTTPError as exc:
                raise AirflowAPIError(
                    f"transport error: {exc.__class__.__name__}: {exc}",
                    status=0,
                    endpoint=url,
                ) from exc

            if 200 <= resp.status_code < 300:
                if resp.status_code == 204 or not resp.content:
                    return None
                return resp.json()

            body = resp.text
            if resp.status_code in (401, 403):
                cls: type[AirflowAPIError] = AirflowAuthError
            elif resp.status_code == 404:
                cls = AirflowNotFoundError
            else:
                cls = AirflowAPIError
            raise cls(
                f"{resp.reason_phrase or 'request failed'}",
                status=resp.status_code,
                endpoint=url,
                body=body,
            )

        if retrying is None:
            return await _do()

        last_exc: AirflowAPIError | None = None
        async for attempt in retrying:
            with attempt:
                try:
                    return await _do()
                except AirflowAPIError as exc:
                    last_exc = exc
                    if exc.status not in _RETRY_STATUSES:
                        raise
                    log.warning(
                        "airflow api transient error status=%d endpoint=%s — retrying",
                        exc.status,
                        exc.endpoint,
                    )
                    raise
        assert last_exc is not None
        raise last_exc

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _strip_none(params: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not params:
            return None
        return {k: v for k, v in params.items() if v is not None}

    @staticmethod
    def _date_aliases_in_run(run: dict[str, Any]) -> dict[str, Any]:
        """Populate the canonical date field for a DAG run response.

        Airflow 2.4+ uses ``logical_date`` and 2.0-2.3 use ``execution_date``.
        We copy whichever the server gave us into both fields so callers
        can use one name regardless of the Airflow version.
        """
        if not run:
            return run
        ld = run.get("logical_date")
        ed = run.get("execution_date")
        if ld is None and ed is not None:
            run["logical_date"] = ed
        elif ed is None and ld is not None:
            run["execution_date"] = ld
        return run

    # ============================================================ DAG endpoints

    async def list_dags(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        only_active: bool | None = None,
        dag_id_pattern: str | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "only_active": only_active,
            "dag_id_pattern": dag_id_pattern,
            "tags": tags,
        }
        return await self._request("GET", "dags", params=self._strip_none(params))

    async def get_dag(self, dag_id: str) -> dict[str, Any]:
        return await self._request("GET", f"dags/{dag_id}")

    async def patch_dag(self, dag_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        return await self._request("PATCH", f"dags/{dag_id}", json=dict(payload))

    # ============================================================ DAG run endpoints

    async def list_dag_runs(
        self,
        dag_id: str,
        *,
        limit: int = 25,
        offset: int = 0,
        state: list[str] | None = None,
        logical_date_gte: str | None = None,
        logical_date_lte: str | None = None,
        execution_date_gte: str | None = None,
        execution_date_lte: str | None = None,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        # Default sort: canonical date field for the target version.
        if order_by is None:
            order_by = "-logical_date" if self._caps.uses_logical_date else "-execution_date"
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "state": state,
            "logical_date_gte": logical_date_gte,
            "logical_date_lte": logical_date_lte,
            "execution_date_gte": execution_date_gte,
            "execution_date_lte": execution_date_lte,
            "order_by": order_by,
        }
        # Airflow v1 ignores logical_date_*; v2 ignores execution_date_*.
        # We send both — Airflow will pick up the one it understands.
        return await self._request("GET", f"dags/{dag_id}/dagRuns", params=self._strip_none(params))

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        run = await self._request("GET", f"dags/{dag_id}/dagRuns/{dag_run_id}")
        return self._date_aliases_in_run(run)

    async def trigger_dag_run(
        self, dag_id: str, payload: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        # In v2, the field is `logical_date`; in v1, `execution_date`.
        # We accept either key in the caller's payload and rename it.
        p = dict(payload or {})
        if self._caps.uses_logical_date and "execution_date" in p and "logical_date" not in p:
            p["logical_date"] = p.pop("execution_date")
        elif not self._caps.uses_logical_date and "logical_date" in p and "execution_date" not in p:
            p["execution_date"] = p.pop("logical_date")
        run = await self._request("POST", f"dags/{dag_id}/dagRuns", json=p)
        return self._date_aliases_in_run(run)

    async def delete_dag_run(self, dag_id: str, dag_run_id: str) -> None:
        await self._request("DELETE", f"dags/{dag_id}/dagRuns/{dag_run_id}")

    async def clear_dag_run(
        self, dag_id: str, dag_run_id: str, payload: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"dags/{dag_id}/dagRuns/{dag_run_id}/clear",
            json=dict(payload or {}),
        )

    # ============================================================ Task endpoints

    async def list_tasks(self, dag_id: str) -> dict[str, Any]:
        return await self._request("GET", f"dags/{dag_id}/tasks")

    async def get_task(self, dag_id: str, task_id: str) -> dict[str, Any]:
        return await self._request("GET", f"dags/{dag_id}/tasks/{task_id}")

    async def list_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        state: list[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"state": state}
        return await self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params=self._strip_none(params),
        )

    async def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        )

    async def get_task_logs(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        *,
        try_number: int = 1,
        full_content: bool = False,
    ) -> str:
        client = self._http
        url = self._api(
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        )
        resp = await client.get(url, params={"full_content": "true" if full_content else "false"})
        if 200 <= resp.status_code < 300:
            return resp.text
        if resp.status_code in (401, 403):
            raise AirflowAuthError(
                "forbidden", status=resp.status_code, endpoint=url, body=resp.text
            )
        if resp.status_code == 404:
            raise AirflowNotFoundError(
                "not found", status=resp.status_code, endpoint=url, body=resp.text
            )
        raise AirflowAPIError(
            "log fetch failed", status=resp.status_code, endpoint=url, body=resp.text
        )

    async def set_task_instance_state(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        new_state: Literal["success", "failed", "skipped", "up_for_retry"],
    ) -> dict[str, Any]:
        # Endpoint shape differs by API version:
        #   v1: PATCH /dags/{d}/dagRuns/{r}/taskInstances/{t}  body: {"new_state": "..."}
        #   v2: POST  /dags/{d}/dagRuns/{r}/taskInstances/{t}/state  body: {"new_state": "..."}
        method = self._caps.set_task_state_method
        suffix = self._caps.set_task_state_suffix
        path = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}{suffix}"
        return await self._request(method, path, json={"new_state": new_state})

    # ============================================================ Variables

    async def list_variables(self, *, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        return await self._request("GET", "variables", params={"limit": limit, "offset": offset})

    async def get_variable(self, variable_key: str) -> dict[str, Any]:
        return await self._request("GET", f"variables/{variable_key}")

    async def set_variable(self, key: str, value: str, description: str = "") -> dict[str, Any]:
        return await self._request(
            "POST",
            "variables",
            json={"key": key, "value": value, "description": description},
        )

    async def delete_variable(self, variable_key: str) -> None:
        await self._request("DELETE", f"variables/{variable_key}")

    # ============================================================ Pools

    async def list_pools(self, *, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        return await self._request("GET", "pools", params={"limit": limit, "offset": offset})

    async def get_pool(self, pool_name: str) -> dict[str, Any]:
        return await self._request("GET", f"pools/{pool_name}")

    # ============================================================ System

    async def get_health(self) -> dict[str, Any]:
        client = self._http
        resp = await client.get(urljoin(self._base, "health"))
        if 200 <= resp.status_code < 300:
            return resp.json() if resp.content else {}
        raise AirflowAPIError(
            "health check failed",
            status=resp.status_code,
            endpoint=str(resp.url),
            body=resp.text,
        )

    async def get_version(self) -> dict[str, Any]:
        return await self._request("GET", "version")


# ---------------------------------------------------------------------------
# Capability resolution + client factory
# ---------------------------------------------------------------------------


async def _detect_version(client: AirflowClient) -> str | None:
    """Try to discover the server's version.

    Tries the currently-configured API path first; falls back to the
    other. Returns ``None`` if both fail.
    """
    try:
        data = await client.get_version()
        v = data.get("version") or data.get("airflow_version")
        if v:
            return v
    except (AirflowAPIError, AirflowAuthError, AirflowNotFoundError):
        pass

    # Try the other API path. We rebuild a one-shot client call by flipping
    # the capability's api_version temporarily.
    other: Literal["v1", "v2"] = "v1" if client.api_version == "v2" else "v2"
    log.info("version probe on /api/%s failed; trying /api/%s", client.api_version, other)
    original = client._caps  # type: ignore[attr-defined]
    try:
        client._caps = override_api_version(original, other)  # type: ignore[attr-defined]
        data = await client.get_version()
        v = data.get("version") or data.get("airflow_version")
        if v:
            # Commit the override so subsequent calls use the right path.
            return v
    except (AirflowAPIError, AirflowAuthError, AirflowNotFoundError):
        pass
    finally:
        client._caps = original  # type: ignore[attr-defined]
    return None


async def build_capabilities(
    settings: Settings, *, http_client: httpx.AsyncClient | None = None
) -> Capabilities:
    """Resolve :class:`Capabilities` from settings, with auto-detect.

    When ``http_client`` is supplied, it's used for the version probe —
    handy for testing. Otherwise a short-lived client is built and torn
    down here.
    """
    target = settings.target_version
    detected: str | None = None
    needs_probe = (target or "").strip().lower() in ("", "auto", "latest")

    if needs_probe:

        async def _probe(probe: httpx.AsyncClient) -> str | None:
            for path in ("api/v2/version", "api/v1/version"):
                try:
                    r = await probe.get(path)
                except httpx.HTTPError as exc:
                    log.warning("version probe %s failed: %s", path, exc)
                    continue
                if r.status_code == 200:
                    try:
                        data = r.json()
                    except ValueError:
                        continue
                    v = data.get("version") or data.get("airflow_version")
                    if v:
                        return v

            return None

        if http_client is not None:
            detected = await _probe(http_client)
        else:
            headers = {"Accept": "application/json", "User-Agent": settings.user_agent}
            auth = None
            if settings.auth_mode == "token" and settings.token:
                headers["Authorization"] = f"Bearer {settings.token}"
            else:
                assert settings.username and settings.password
                auth = (settings.username, settings.password)
            async with httpx.AsyncClient(
                base_url=settings.base_url.rstrip("/") + "/",
                headers=headers,
                auth=auth,
                timeout=httpx.Timeout(min(settings.timeout, 5.0)),
                verify=settings.verify_ssl,
            ) as probe:
                detected = await _probe(probe)

        if detected is None:
            log.warning(
                "could not auto-detect Airflow version from %s — "
                "falling back to default capability profile",
                settings.base_url,
            )

    caps = resolve_capabilities(target=target, detected=detected)
    return override_api_version(caps, settings.api_version)


@asynccontextmanager
async def airflow_client(settings: Settings) -> AsyncIterator[AirflowClient]:
    """Async context manager that yields a started :class:`AirflowClient`.

    Resolves the target version (auto-detect if needed) and starts the
    shared HTTP pool.
    """
    caps = await build_capabilities(settings)
    log.info(
        "airflow-mcp targeting %s (api=%s, uses_logical_date=%s)",
        caps.target_version,
        caps.api_version,
        caps.uses_logical_date,
    )
    client = AirflowClient(settings, caps)
    try:
        await client.start()
        yield client
    finally:
        await client.aclose()
