"""DAG-level tools. Mutating: ``pause_dag`` / ``unpause_dag``."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from ..config import Settings
from ._helpers import get_client, to_json, tool_errors


def register(
    mcp: object,
    settings: Settings,
    *,
    include_mutating: bool,
    allowlist: set[str] | None = None,
) -> None:
    """Attach DAG tools to a FastMCP instance.

    Args:
        mcp: FastMCP server.
        settings: Server settings (controls page sizes).
        include_mutating: If false, hide ``pause_dag`` / ``unpause_dag``.
        allowlist: When set, register only the listed tool names.
    """

    def want(name: str) -> bool:
        return allowlist is None or name in allowlist

    if want("list_dags"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def list_dags(
            ctx: Context,
            limit: int = 0,
            offset: int = 0,
            only_active: bool = True,
            dag_id_pattern: str | None = None,
            tags: list[str] | None = None,
        ) -> str:
            """List DAGs (compact). ``limit=0`` → server default of 20.

            Args:
                limit: Max DAGs to return (0 → default 20, hard cap 100).
                offset: Pagination offset.
                only_active: Hide paused DAGs when true.
                dag_id_pattern: SQL-LIKE filter, e.g. ``%ingest_%``.
                tags: AND-filter by tag list.
            """
            if limit <= 0:
                limit = settings.list_page_default
            limit = max(1, min(limit, settings.list_page_max))
            offset = max(0, offset)
            client = get_client(ctx)
            data = await client.list_dags(
                limit=limit,
                offset=offset,
                only_active=only_active,
                dag_id_pattern=dag_id_pattern,
                tags=tags,
            )
            dags = [
                {
                    "dag_id": d.get("dag_id"),
                    "is_paused": d.get("is_paused"),
                    "is_active": d.get("is_active"),
                    "schedule": (d.get("schedule_interval") or {}).get("value")
                    if isinstance(d.get("schedule_interval"), dict)
                    else d.get("schedule_interval"),
                    "owners": d.get("owners"),
                    "tags": [t.get("name") for t in d.get("tags", [])] or None,
                    "description": (d.get("description") or "").strip() or None,
                }
                for d in data.get("dags", [])
            ]
            return to_json({"count": len(dags), "dags": dags})

    if want("get_dag"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_dag(ctx: Context, dag_id: str) -> str:
            """Get a single DAG (compact). Use ``list_dags`` for an index first."""
            client = get_client(ctx)
            return to_json(await client.get_dag(dag_id))

    if include_mutating:
        if want("pause_dag"):

            @mcp.tool()  # type: ignore[union-attr]
            @tool_errors
            async def pause_dag(ctx: Context, dag_id: str) -> str:
                """Pause a DAG (mutating — hidden in read-only mode)."""
                client = get_client(ctx)
                return to_json(await client.patch_dag(dag_id, {"is_paused": True}))

        if want("unpause_dag"):

            @mcp.tool()  # type: ignore[union-attr]
            @tool_errors
            async def unpause_dag(ctx: Context, dag_id: str) -> str:
                """Resume a DAG (mutating — hidden in read-only mode)."""
                client = get_client(ctx)
                return to_json(await client.patch_dag(dag_id, {"is_paused": False}))
