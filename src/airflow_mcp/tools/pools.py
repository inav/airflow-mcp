"""Pool tools (read-only)."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from ..config import Settings
from ._helpers import get_client, to_json, tool_errors


def register(
    mcp: object,
    settings: Settings,
    *,
    include_mutating: bool = True,  # unused, kept for signature parity
    allowlist: set[str] | None = None,
) -> None:
    """Attach pool tools. There are no mutating pool tools right now."""

    def want(name: str) -> bool:
        return allowlist is None or name in allowlist

    if want("list_pools"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def list_pools(ctx: Context, limit: int = 0, offset: int = 0) -> str:
            """List Airflow pools (compact)."""
            if limit <= 0:
                limit = settings.list_page_default
            limit = max(1, min(limit, settings.list_page_max))
            offset = max(0, offset)
            client = get_client(ctx)
            data = await client.list_pools(limit=limit, offset=offset)
            pools = [
                {
                    "name": p.get("name"),
                    "slots": p.get("slots"),
                    "occupied_slots": p.get("occupied_slots"),
                    "running_slots": p.get("running_slots"),
                    "queued_slots": p.get("queued_slots"),
                    "open_slots": p.get("open_slots"),
                }
                for p in data.get("pools", [])
            ]
            return to_json({"count": len(pools), "pools": pools})

    if want("get_pool"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_pool(ctx: Context, pool_name: str) -> str:
            """Get a single pool by name (compact)."""
            client = get_client(ctx)
            return to_json(await client.get_pool(pool_name))
