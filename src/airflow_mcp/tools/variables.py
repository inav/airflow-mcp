"""Variable tools. Read-only: list (keys only), get. Mutating: set, delete."""

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
    """Attach variable tools to a FastMCP instance."""

    def want(name: str) -> bool:
        return allowlist is None or name in allowlist

    if want("list_variables"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def list_variables(ctx: Context, limit: int = 0, offset: int = 0) -> str:
            """List variable **keys** only — values are never echoed in bulk."""
            if limit <= 0:
                limit = settings.list_page_default
            limit = max(1, min(limit, settings.list_page_max))
            offset = max(0, offset)
            client = get_client(ctx)
            data = await client.list_variables(limit=limit, offset=offset)
            return to_json(
                {
                    "count": len(data.get("variables", [])),
                    "variables": [v.get("key") for v in data.get("variables", []) if v.get("key")],
                }
            )

    if want("get_variable"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_variable(ctx: Context, key: str) -> str:
            """Get a single variable's value (intentional — opt-in to read secrets)."""
            client = get_client(ctx)
            data = await client.get_variable(key)
            return to_json(
                {
                    "key": data.get("key"),
                    "value": data.get("value"),
                    "description": data.get("description") or None,
                }
            )

    if include_mutating:
        if want("set_variable"):

            @mcp.tool()  # type: ignore[union-attr]
            @tool_errors
            async def set_variable(
                ctx: Context, key: str, value: str, description: str = ""
            ) -> str:
                """Create or update a variable (mutating — hidden in read-only mode)."""
                client = get_client(ctx)
                result = await client.set_variable(key, value, description=description)
                return to_json({"key": result.get("key", key), "updated": True})

        if want("delete_variable"):

            @mcp.tool()  # type: ignore[union-attr]
            @tool_errors
            async def delete_variable(ctx: Context, key: str) -> str:
                """Delete a variable (mutating — hidden in read-only mode)."""
                client = get_client(ctx)
                await client.delete_variable(key)
                return to_json({"key": key, "deleted": True})
