"""System tools (read-only): health, version, capabilities."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from ..config import Settings
from ..versioning import Capabilities
from ._helpers import get_capabilities, get_client, to_json, tool_errors


def register(
    mcp: object,
    settings: Settings,
    *,
    include_mutating: bool = True,
    allowlist: set[str] | None = None,
) -> None:
    """Attach system tools. None of these mutate state."""

    def want(name: str) -> bool:
        return allowlist is None or name in allowlist

    if want("get_health"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_health(ctx: Context) -> str:
            """Probe ``/health`` on the webserver (compact).

            Returns per-component health — useful as a first call when
            something looks off.
            """
            client = get_client(ctx)
            return to_json(await client.get_health())

    if want("get_version"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_version(ctx: Context) -> str:
            """Return the Airflow version reported by the webserver (compact).

            Calls the appropriate ``/api/{v1,v2}/version`` based on the
            resolved target version.
            """
            client = get_client(ctx)
            data = await client.get_version()
            return to_json(
                {
                    "version": data.get("version") or data.get("airflow_version"),
                    "git_version": data.get("git_version"),
                }
            )

    if want("get_capabilities"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_capabilities(ctx: Context) -> str:
            """Report the resolved Airflow target version + capability flags.

            Useful for the model to know which API path is in use, whether
            ``logical_date`` is the canonical run date field, and which
            mutating tool paths are available.
            """
            caps: Capabilities = get_capabilities(ctx)
            return to_json(
                {
                    "target_version": caps.target_version,
                    "parsed": list(caps.parsed),
                    "api_version": caps.api_version,
                    "uses_logical_date": caps.uses_logical_date,
                    "uses_execution_date": caps.uses_execution_date,
                    "set_task_state_method": caps.set_task_state_method,
                    "supports_dry_run_clear": caps.supports_dry_run_clear,
                }
            )
