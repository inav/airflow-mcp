"""Mocked-HTTP tests for :class:`AirflowClient`.

Covers v1 (Airflow 2.2-2.3) and v2 (Airflow 2.4+ / 3.0+) client behaviour,
plus auto-detect.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from airflow_mcp.client import AirflowClient
from airflow_mcp.config import Settings
from airflow_mcp.errors import AirflowAPIError, AirflowAuthError, AirflowNotFoundError
from airflow_mcp.versioning import resolve_capabilities


def _v1_caps() -> object:
    return resolve_capabilities(target="2.2.5")


def _v2_caps() -> object:
    return resolve_capabilities(target="2.9.0")


def _v3_caps() -> object:
    return resolve_capabilities(target="3.0.0")


def _client_with(handler, *, caps=None) -> AirflowClient:
    """Build an :class:`AirflowClient` whose httpx transport is a mock.

    Mirrors what :meth:`AirflowClient.start` does in production so the auth
    header / base_url match the real client.
    """
    if caps is None:
        caps = _v1_caps()
    settings = Settings(
        base_url="http://airflow.example.com",
        username="alice",
        password="secret",
    )
    client = AirflowClient(settings, caps)
    headers = {"Accept": "application/json", "User-Agent": "test"}
    auth = httpx.BasicAuth(settings.username, settings.password)
    client._client = httpx.AsyncClient(  # type: ignore[attr-defined]
        base_url=client._base,
        headers=headers,
        auth=auth,
        transport=httpx.MockTransport(handler),
    )
    return client


# ---------------------------------------------------------------------------
# v1 behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_dags_v1_path() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"total_entries": 1, "dags": [{"dag_id": "x"}]})

    client = _client_with(handler, caps=_v1_caps())
    data = await client.list_dags(limit=10, only_active=True, dag_id_pattern="%ingest%")
    from urllib.parse import urlsplit

    assert captured["method"] == "GET"
    assert urlsplit(captured["url"]).path == "/api/v1/dags"
    assert data["total_entries"] == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_404_raises_not_found() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    client = _client_with(handler)
    with pytest.raises(AirflowNotFoundError):
        await client.get_dag("missing")
    await client.aclose()


@pytest.mark.asyncio
async def test_401_raises_auth_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="unauthorized")

    client = _client_with(handler)
    with pytest.raises(AirflowAuthError):
        await client.list_dags()
    await client.aclose()


@pytest.mark.asyncio
async def test_500_retries_then_raises() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, text="boom")

    client = _client_with(handler)
    with pytest.raises(AirflowAPIError):
        await client.list_dags()
    assert calls["n"] == 4  # 1 + max_retries(3)
    await client.aclose()


@pytest.mark.asyncio
async def test_trigger_dag_run_v1_payload() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"dag_run_id": "manual__2025"})

    client = _client_with(handler, caps=_v1_caps())
    out = await client.trigger_dag_run("my_dag", {"conf": {"k": "v"}, "note": "hi"})
    assert captured["method"] == "POST"
    assert captured["body"] == {"conf": {"k": "v"}, "note": "hi"}
    assert out["dag_run_id"] == "manual__2025"
    await client.aclose()


@pytest.mark.asyncio
async def test_set_task_instance_state_v1_uses_patch() -> None:
    """v1 sets TI state with PATCH on the TI itself."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"state": "success"})

    client = _client_with(handler, caps=_v1_caps())
    await client.set_task_instance_state("d", "r", "t", "success")
    assert captured["method"] == "PATCH"
    assert captured["body"] == {"new_state": "success"}
    # No /state suffix in v1
    assert captured["url"].rstrip("/").endswith("/taskInstances/t")
    await client.aclose()


@pytest.mark.asyncio
async def test_health_endpoint_outside_api_v() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"metadatabase": {"status": "healthy"}})

    client = _client_with(handler)
    out = await client.get_health()
    assert captured["url"].rstrip("/").endswith("/health")
    assert out["metadatabase"]["status"] == "healthy"
    await client.aclose()


# ---------------------------------------------------------------------------
# v2 behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_dags_v2_path() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"dags": [], "total_entries": 0})

    client = _client_with(handler, caps=_v2_caps())
    await client.list_dags()
    from urllib.parse import urlsplit

    assert urlsplit(captured["url"]).path == "/api/v2/dags"
    await client.aclose()


@pytest.mark.asyncio
async def test_set_task_instance_state_v2_uses_post_state_suffix() -> None:
    """v2 sets TI state with POST on the /state sub-resource."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"state": "success"})

    client = _client_with(handler, caps=_v2_caps())
    await client.set_task_instance_state("d", "r", "t", "success")
    assert captured["method"] == "POST"
    assert captured["body"] == {"new_state": "success"}
    assert captured["url"].rstrip("/").endswith("/taskInstances/t/state")
    await client.aclose()


@pytest.mark.asyncio
async def test_v3_set_task_state_uses_post() -> None:
    """3.0+ keeps the v2 endpoint shape (POST /state)."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        return httpx.Response(200, json={"state": "failed"})

    client = _client_with(handler, caps=_v3_caps())
    await client.set_task_instance_state("d", "r", "t", "failed")
    assert captured["method"] == "POST"
    await client.aclose()


# ---------------------------------------------------------------------------
# Date field aliasing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_dag_run_v2_renames_execution_date_to_logical_date() -> None:
    """v2 client should accept `execution_date` in the payload and rename to `logical_date`."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"dag_run_id": "r1"})

    client = _client_with(handler, caps=_v2_caps())
    await client.trigger_dag_run("d", {"execution_date": "2025-01-01T00:00:00Z"})
    assert "logical_date" in captured["body"]
    assert "execution_date" not in captured["body"]
    await client.aclose()


@pytest.mark.asyncio
async def test_trigger_dag_run_v1_renames_logical_date_to_execution_date() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"dag_run_id": "r1"})

    client = _client_with(handler, caps=_v1_caps())
    await client.trigger_dag_run("d", {"logical_date": "2025-01-01T00:00:00Z"})
    assert "execution_date" in captured["body"]
    assert "logical_date" not in captured["body"]
    await client.aclose()


@pytest.mark.asyncio
async def test_get_dag_run_aliases_date_fields_v2() -> None:
    """v2 server returns `logical_date`; client should also expose `execution_date`."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"dag_run_id": "r1", "logical_date": "2025-01-01T00:00:00Z"},
        )

    client = _client_with(handler, caps=_v2_caps())
    run = await client.get_dag_run("d", "r1")
    assert run["logical_date"] == "2025-01-01T00:00:00Z"
    assert run["execution_date"] == "2025-01-01T00:00:00Z"
    await client.aclose()


@pytest.mark.asyncio
async def test_get_dag_run_aliases_date_fields_v1() -> None:
    """v1 server returns `execution_date`; client should also expose `logical_date`."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"dag_run_id": "r1", "execution_date": "2025-01-01T00:00:00Z"},
        )

    client = _client_with(handler, caps=_v1_caps())
    run = await client.get_dag_run("d", "r1")
    assert run["execution_date"] == "2025-01-01T00:00:00Z"
    assert run["logical_date"] == "2025-01-01T00:00:00Z"
    await client.aclose()


@pytest.mark.asyncio
async def test_clear_dag_run_dry_run() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=[{"task_id": "t1", "state": "failed"}])

    client = _client_with(handler)
    result = await client.clear_dag_run("d", "run1", {"dry_run": True, "reset_dag_run": True})
    assert captured["body"]["dry_run"] is True
    assert isinstance(result, list)
    await client.aclose()


# ---------------------------------------------------------------------------
# Token auth
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_token_auth_sets_bearer_header() -> None:
    settings = Settings(base_url="http://airflow.example.com", token="tk_123")
    caps = resolve_capabilities(target="2.2.5")
    client = AirflowClient(settings, caps)
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Authorization"))
        return httpx.Response(200, json={"version": "2.2.5", "git_version": "abc"})

    client._client = httpx.AsyncClient(  # type: ignore[attr-defined]
        base_url=client._base,
        headers={"Authorization": f"Bearer {settings.token}"},
        transport=httpx.MockTransport(handler),
    )
    await client.get_version()
    assert seen == ["Bearer tk_123"]
    await client.aclose()
