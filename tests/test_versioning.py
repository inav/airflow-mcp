"""Tests for the version-compatibility layer."""

from __future__ import annotations

import pytest

from airflow_mcp.versioning import (
    Capabilities,
    override_api_version,
    parse_version,
    resolve_capabilities,
)


# ---------------------------------------------------------------------------
# parse_version
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2.2.5", (2, 2, 5)),
        ("v3.0.0", (3, 0, 0)),
        ("2.10.0", (2, 10, 0)),
        ("2.4.0.dev0", (2, 4, 0)),
        ("  2.9.0  ", (2, 9, 0)),
    ],
)
def test_parse_version_valid(raw: str, expected: tuple[int, int, int]) -> None:
    assert parse_version(raw) == expected


@pytest.mark.parametrize("raw", ["", None, "2.2", "foo", "2.2.5.6.7"])
def test_parse_version_invalid(raw: object) -> None:
    assert parse_version(raw if isinstance(raw, str) else None) is None


# ---------------------------------------------------------------------------
# resolve_capabilities
# ---------------------------------------------------------------------------


def test_target_2_2_5_uses_v1() -> None:
    caps = resolve_capabilities(target="2.2.5")
    assert caps.api_version == "v1"
    assert caps.uses_logical_date is False
    assert caps.uses_execution_date is True
    assert caps.set_task_state_method == "PATCH"
    assert caps.set_task_state_suffix == ""


def test_target_2_3_0_uses_v1() -> None:
    caps = resolve_capabilities(target="2.3.0")
    assert caps.api_version == "v1"


def test_target_2_4_0_uses_v2() -> None:
    caps = resolve_capabilities(target="2.4.0")
    assert caps.api_version == "v2"
    assert caps.uses_logical_date is True
    assert caps.uses_execution_date is True  # kept as alias
    assert caps.set_task_state_method == "POST"
    assert caps.set_task_state_suffix == "/state"


def test_target_2_9_0_uses_v2() -> None:
    caps = resolve_capabilities(target="2.9.0")
    assert caps.api_version == "v2"


def test_target_3_0_0_uses_v2_no_execution_date() -> None:
    caps = resolve_capabilities(target="3.0.0")
    assert caps.api_version == "v2"
    assert caps.uses_logical_date is True
    assert caps.uses_execution_date is False


def test_auto_uses_detected() -> None:
    caps = resolve_capabilities(target="auto", detected="2.7.0")
    assert caps.target_version == "2.7.0"
    assert caps.api_version == "v2"


def test_auto_with_no_detected_defaults_to_legacy() -> None:
    caps = resolve_capabilities(target="auto", detected=None)
    assert caps.api_version == "v1"
    assert caps.parsed == (2, 2, 5)


def test_invalid_target_falls_back_to_auto() -> None:
    caps = resolve_capabilities(target="not-a-version", detected="2.9.0")
    assert caps.target_version == "2.9.0"


def test_target_and_detected_diverge_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        resolve_capabilities(target="2.2.5", detected="2.9.0")
    assert any("differs" in rec.message for rec in caplog.records)


def test_no_target_no_detected_warns(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        resolve_capabilities(target=None, detected=None)
    assert any("defaulting to Airflow 2.2.5" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# override_api_version
# ---------------------------------------------------------------------------


def test_override_auto_keeps_resolved() -> None:
    caps = resolve_capabilities(target="2.4.0")
    assert override_api_version(caps, "auto").api_version == "v2"


def test_override_to_v1() -> None:
    caps = resolve_capabilities(target="2.9.0")
    overridden = override_api_version(caps, "v1")
    assert overridden.api_version == "v1"
    assert overridden.set_task_state_method == "PATCH"
    assert overridden.set_task_state_suffix == ""
    # logical_date use flag changes too
    assert overridden.uses_logical_date is False


def test_override_to_v2() -> None:
    caps = resolve_capabilities(target="2.2.5")
    overridden = override_api_version(caps, "v2")
    assert overridden.api_version == "v2"
    assert overridden.set_task_state_method == "POST"
    assert overridden.set_task_state_suffix == "/state"


def test_override_to_same_no_op() -> None:
    caps = resolve_capabilities(target="2.4.0")
    assert override_api_version(caps, "v2").api_version == "v2"


# ---------------------------------------------------------------------------
# build_capabilities (with mocked HTTP)
# ---------------------------------------------------------------------------


import httpx
import pytest


@pytest.mark.asyncio
async def test_build_capabilities_auto_detects_v2() -> None:
    from airflow_mcp.client import build_capabilities
    from airflow_mcp.config import Settings

    settings = Settings(
        base_url="http://airflow.example.com",
        username="u",
        password="p",
        target_version="auto",
    )

    def v2_handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("/api/v2/version"):
            return httpx.Response(200, json={"version": "2.9.0"})
        return httpx.Response(404)

    probe = httpx.AsyncClient(
        base_url="http://airflow.example.com/",
        auth=httpx.BasicAuth("u", "p"),
        transport=httpx.MockTransport(v2_handler),
    )
    try:
        caps = await build_capabilities(settings, http_client=probe)
        assert caps.target_version == "2.9.0"
        assert caps.api_version == "v2"
    finally:
        await probe.aclose()


@pytest.mark.asyncio
async def test_build_capabilities_falls_back_to_v1() -> None:
    from airflow_mcp.client import build_capabilities
    from airflow_mcp.config import Settings

    settings = Settings(
        base_url="http://airflow.example.com",
        username="u",
        password="p",
        target_version="auto",
    )

    def v1_only(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("/api/v1/version"):
            return httpx.Response(200, json={"version": "2.3.0"})
        return httpx.Response(404)

    probe = httpx.AsyncClient(
        base_url="http://airflow.example.com/",
        auth=httpx.BasicAuth("u", "p"),
        transport=httpx.MockTransport(v1_only),
    )
    try:
        caps = await build_capabilities(settings, http_client=probe)
        # v1 served, falls back to v1 path
        assert caps.target_version == "2.3.0"
        assert caps.api_version == "v1"
    finally:
        await probe.aclose()
