"""Smoke tests for the Airflow MCP server (no real Airflow needed)."""

from __future__ import annotations

import asyncio
import inspect
import json
import os

# Test auth — must be set before any airflow_mcp import.
os.environ.setdefault("AIRFLOW_BASE_URL", "http://localhost:8080")
os.environ.setdefault("AIRFLOW_USERNAME", "test")
os.environ.setdefault("AIRFLOW_PASSWORD", "test")

import pytest

from airflow_mcp.config import Settings
from airflow_mcp.errors import AirflowAPIError, AirflowAuthError, AirflowNotFoundError
from airflow_mcp.server import build_server
from airflow_mcp.tools import register_all
from airflow_mcp.tools._helpers import _strip_empties, to_compact, to_json


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def test_settings_requires_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in ("AIRFLOW_TOKEN", "AIRFLOW_USERNAME", "AIRFLOW_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(ValueError, match="Authentication is required"):
        Settings(base_url="http://localhost:8080")


def test_settings_basic_auth() -> None:
    s = Settings(base_url="http://localhost:8080/", username="u", password="p")
    assert s.auth_mode == "basic"


def test_settings_token_wins() -> None:
    s = Settings(base_url="http://localhost:8080", token="tkn", username="x", password="y")
    assert s.auth_mode == "token"


def test_settings_read_only_default() -> None:
    s = Settings(username="u", password="p")
    assert s.read_only is True


def test_settings_enabled_tools_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_ENABLED_TOOLS", "list_dags,get_dag,get_health")
    s = Settings(username="u", password="p")
    assert s.enabled_tools == ["list_dags", "get_dag", "get_health"]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def test_error_to_user_message() -> None:
    err = AirflowAPIError("boom", status=500, endpoint="/x", body="hello world")
    msg = err.to_user_message()
    assert msg.startswith("[HTTP 500]")
    assert "/x" in msg
    assert "boom" in msg
    assert "hello world" in msg


def test_error_body_truncated() -> None:
    err = AirflowAPIError("boom", status=500, endpoint="/x", body="x" * 10_000)
    msg = err.to_user_message()
    assert "..." in msg


def test_auth_and_not_found_subclasses() -> None:
    assert issubclass(AirflowAuthError, AirflowAPIError)
    assert issubclass(AirflowNotFoundError, AirflowAPIError)


# ---------------------------------------------------------------------------
# Compact serializer
# ---------------------------------------------------------------------------


def test_strip_empties_drops_nulls_and_empty_containers() -> None:
    cleaned = _strip_empties(
        {
            "a": 1,
            "b": None,
            "c": "",
            "d": [],
            "e": {},
            "f": "ok",
            "g": {"x": None, "y": "yes", "z": []},
            "h": [None, "", "v", 0, False],  # 0 and False are *kept*
        }
    )
    assert cleaned == {
        "a": 1,
        "f": "ok",
        "g": {"y": "yes"},
        "h": ["v", 0, False],
    }


def test_to_compact_uses_compact_separators() -> None:
    s = to_compact({"a": 1, "b": [1, 2, 3], "c": None})
    # No spaces, no empty "c":
    assert s == '{"a":1,"b":[1,2,3]}'


def test_to_json_pretty_for_small_compact_for_large() -> None:
    small = to_json({"a": 1})
    assert "\n" in small  # pretty
    big = to_json({"x": "y" * 5000})
    assert "\n" not in big  # compact for large payloads


def test_to_json_handles_datetimes_via_default() -> None:
    from datetime import datetime

    payload = {"ts": datetime(2025, 1, 1, 0, 0, 0), "k": None}
    out = to_json(payload)
    assert "2025" in out
    parsed = json.loads(out)
    assert "k" not in parsed  # None dropped


# ---------------------------------------------------------------------------
# Server wiring
# ---------------------------------------------------------------------------


_READ_ONLY_TOOLS = {
    "list_dags",
    "get_dag",
    "list_dag_runs",
    "get_dag_run",
    "list_tasks",
    "get_task",
    "list_task_instances",
    "get_task_instance",
    "get_task_logs",
    "list_variables",
    "get_variable",
    "list_pools",
    "get_pool",
    "get_health",
    "get_version",
}

_MUTATING_TOOLS = {
    "pause_dag",
    "unpause_dag",
    "trigger_dag_run",
    "delete_dag_run",
    "clear_dag_run",
    "set_task_instance_state",
    "set_variable",
    "delete_variable",
}


def _tool_names(server) -> set[str]:  # type: ignore[no-untyped-def]
    tools = asyncio.run(server.list_tools())  # type: ignore[attr-defined]
    return {t.name for t in tools}


def test_build_server_read_only_excludes_mutating() -> None:
    """Default: read-only mode hides mutating tools."""
    server = build_server()
    names = _tool_names(server)
    assert _READ_ONLY_TOOLS.issubset(names), f"missing read-only tools: {_READ_ONLY_TOOLS - names}"
    assert names.isdisjoint(_MUTATING_TOOLS), (
        f"mutating tools leaked into read-only mode: {names & _MUTATING_TOOLS}"
    )


def test_build_server_full_mode_includes_everything() -> None:
    server = build_server(read_only=False)
    names = _tool_names(server)
    assert _READ_ONLY_TOOLS.issubset(names)
    assert _MUTATING_TOOLS.issubset(names), (
        f"missing mutating tools: {_MUTATING_TOOLS - names}"
    )


def test_enabled_tools_allowlist_overrides_read_only() -> None:
    server = build_server(
        read_only=True,
        enabled_tools=["list_dags", "get_health"],
    )
    names = _tool_names(server)
    assert names == {"list_dags", "get_health"}


def test_allowlist_includes_mutating_tool_even_in_read_only() -> None:
    """An explicit allowlist is the source of truth — it overrides read_only."""
    server = build_server(
        read_only=True,
        enabled_tools=["trigger_dag_run", "list_dags"],
    )
    names = _tool_names(server)
    assert "trigger_dag_run" in names
    assert "list_dags" in names
    # Other mutating tools NOT in the allowlist stay hidden.
    assert "delete_dag_run" not in names
    assert "set_variable" not in names


def test_all_tool_modules_have_register() -> None:
    from airflow_mcp.tools import dags, dag_runs, pools, system, tasks, variables

    for mod in (dags, dag_runs, pools, system, tasks, variables):
        assert callable(getattr(mod, "register")), f"{mod.__name__}.register missing"


def test_register_all_accepts_server() -> None:
    server = build_server()
    # Re-register against the same server would duplicate tools, so we just
    # confirm the function signature is callable with a Settings arg.
    s = Settings(username="u", password="p")
    # Build a fresh server to keep the test idempotent.
    from airflow_mcp.server import build_server as build

    server2 = build()
    register_all(server2, s)  # should not raise


def test_all_tool_functions_are_async() -> None:
    """Every tool function exposed by a module is a coroutine function."""
    from airflow_mcp.tools import dags, dag_runs, pools, system, tasks, variables

    for mod in (dags, dag_runs, pools, system, tasks, variables):
        for name, obj in vars(mod).items():
            if name.startswith("_") or not callable(obj):
                continue
            if hasattr(obj, "__wrapped__"):
                assert inspect.iscoroutinefunction(obj.__wrapped__), (
                    f"{mod.__name__}.{name} is not async"
                )


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------


def test_log_truncation_keeps_tail_lines() -> None:
    from airflow_mcp.tools.tasks import _truncate

    text = "\n".join(f"line {i}" for i in range(1000))
    out = _truncate(text, max_bytes=10_000, max_lines=10)
    assert "line 999" in out
    assert "line 0" not in out
    assert "truncated" in out
