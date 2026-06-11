"""MCP server entrypoint for Apache Airflow 2.2.x and 3.x.

Run via the installed console script:

    airflow-mcp                # stdio transport (default)
    airflow-mcp --transport sse  # SSE transport (HTTP)

Environment variables (with the ``AIRFLOW_`` prefix):

    AIRFLOW_BASE_URL         (default: http://localhost:8080)
    AIRFLOW_TARGET_VERSION   (default: "auto" — probed from the server)
    AIRFLOW_API_VERSION      (default: "auto" — resolved from TARGET_VERSION)
    AIRFLOW_TOKEN            (optional, wins over basic auth)
    AIRFLOW_USERNAME         (required unless AIRFLOW_TOKEN is set)
    AIRFLOW_PASSWORD         (required unless AIRFLOW_TOKEN is set)
    AIRFLOW_READ_ONLY        (default: true)
    AIRFLOW_ENABLED_TOOLS    (optional CSV allowlist, overrides READ_ONLY)
    AIRFLOW_TIMEOUT          (seconds, default 30)
    AIRFLOW_VERIFY_SSL       (default true)
    AIRFLOW_MAX_RETRIES      (default 3)
    AIRFLOW_MCP_TRANSPORT    (default: stdio)
    AIRFLOW_MCP_LOG_LEVEL    (default: INFO)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import FastMCP
from pydantic import ValidationError

from .client import AirflowClient, build_capabilities
from .config import Settings
from .errors import AirflowConfigError
from .tools import register_all
from .versioning import Capabilities

log = logging.getLogger("airflow_mcp")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[dict[str, object]]:
    """Open the shared :class:`AirflowClient` for the lifetime of the server.

    Resolves the target Airflow version (auto-detect if needed) and stashes
    both the client and the resolved :class:`Capabilities` on the
    lifespan context for tools to read.
    """
    settings = Settings()
    log.info(
        "starting airflow-mcp (base_url=%s, auth=%s, target_version=%s)",
        settings.base_url,
        settings.auth_mode,
        settings.target_version,
    )
    caps: Capabilities = await build_capabilities(settings)
    client = AirflowClient(settings, caps)
    await client.start()
    try:
        yield {"airflow": client, "settings": settings, "capabilities": caps}
    finally:
        await client.aclose()
        log.info("airflow-mcp stopped")


# ---------------------------------------------------------------------------
# Server factory
# ---------------------------------------------------------------------------


def build_server(
    *,
    read_only: bool | None = None,
    enabled_tools: list[str] | None = None,
    target_version: str | None = None,
) -> FastMCP:
    """Build a configured :class:`FastMCP` instance with all tools registered.

    Args:
        read_only: If set, override ``Settings.read_only``.
        enabled_tools: If set, override ``Settings.enabled_tools``.
        target_version: If set, override ``Settings.target_version``.
    """
    try:
        settings = Settings()
    except ValidationError as exc:
        raise AirflowConfigError(_format_settings_errors(exc)) from exc
    if read_only is not None:
        settings = settings.model_copy(update={"read_only": read_only})
    if enabled_tools is not None:
        settings = settings.model_copy(update={"enabled_tools": enabled_tools})
    if target_version is not None:
        settings = settings.model_copy(update={"target_version": target_version})

    mcp = FastMCP(
        name="airflow",
        instructions=_instructions(settings),
        lifespan=app_lifespan,
    )
    register_all(mcp, settings)
    return mcp


def _instructions(settings: Settings) -> str:
    base = (
        "MCP server for Apache Airflow 2.2.x / 2.3.x / 2.4+ / 3.x stable REST API. "
        "Read-only tools: list/get DAGs, list/get DAG runs, list/get tasks, "
        "list/get task instances, get task logs (truncated by default), "
        "list/get variables (list returns keys only), list/get pools, "
        "get_health, get_version, get_capabilities."
    )
    if not settings.read_only:
        base += (
            " Mutating tools ENABLED: pause_dag, unpause_dag, trigger_dag_run, "
            "delete_dag_run, clear_dag_run, set_task_instance_state, "
            "set_variable, delete_variable."
        )
    else:
        base += " Mutating tools are DISABLED (read-only mode)."
    return base


def _format_settings_errors(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", ()))
        parts.append(f"{loc}: {err.get('msg')}")
    return "invalid Airflow MCP configuration — " + "; ".join(parts)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="airflow-mcp",
        description="MCP server for Apache Airflow 2.x and 3.x",
    )
    parser.add_argument(
        "--transport",
        choices=("stdio", "sse"),
        default=os.environ.get("AIRFLOW_MCP_TRANSPORT", "stdio"),
        help="Transport to use. Default: stdio.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("AIRFLOW_MCP_LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG/INFO/WARNING/ERROR). Default: INFO.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        server = build_server()
    except AirflowConfigError as exc:
        log.error("configuration error: %s", exc)
        sys.exit(2)

    transport = args.transport
    log.info("running with transport=%s", transport)
    server.run(transport=transport)


if __name__ == "__main__":
    main()
