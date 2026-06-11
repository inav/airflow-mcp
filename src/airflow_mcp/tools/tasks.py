"""Task + task-instance tools. Mutating: ``set_task_instance_state``."""

from __future__ import annotations

from typing import Literal

from mcp.server.fastmcp import Context

from ..config import Settings
from ._helpers import get_client, to_json, tool_errors

NewState = Literal["success", "failed", "skipped", "up_for_retry"]


def register(
    mcp: object,
    settings: Settings,
    *,
    include_mutating: bool,
    allowlist: set[str] | None = None,
) -> None:
    """Attach task-related tools to a FastMCP instance."""

    def want(name: str) -> bool:
        return allowlist is None or name in allowlist

    if want("list_tasks"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def list_tasks(ctx: Context, dag_id: str) -> str:
            """List tasks defined in a DAG (compact: id, operator, deps)."""
            client = get_client(ctx)
            data = await client.list_tasks(dag_id)
            tasks = [
                {
                    "task_id": t.get("task_id"),
                    "operator": (t.get("class_ref") or {}).get("class_name"),
                    "downstream": t.get("downstream_task_ids") or None,
                    "upstream": t.get("upstream_task_ids") or None,
                }
                for t in data.get("tasks", [])
            ]
            return to_json({"dag_id": dag_id, "count": len(tasks), "tasks": tasks})

    if want("get_task"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_task(ctx: Context, dag_id: str, task_id: str) -> str:
            """Get a single task definition (compact)."""
            client = get_client(ctx)
            data = await client.get_task(dag_id, task_id)
            # Drop the heavy doc fields — usually huge and rarely useful on
            # first look; the user can ask to inspect them explicitly.
            for k in ("doc_md", "doc_md_rst", "doc_yaml"):
                if k in data:
                    data[k] = None
            return to_json(data)

    if want("list_task_instances"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def list_task_instances(
            ctx: Context,
            dag_id: str,
            dag_run_id: str,
            state: list[str] | None = None,
            limit: int = 0,
        ) -> str:
            """List task instances for a DAG run (compact).

            Args:
                dag_id: The DAG identifier.
                dag_run_id: The run id.
                state: Filter by state(s), e.g. ``["failed"]``.
                limit: 0 → default 20.
            """
            if limit <= 0:
                limit = settings.list_page_default
            limit = max(1, min(limit, settings.list_page_max))
            client = get_client(ctx)
            data = await client.list_task_instances(dag_id, dag_run_id, state=state)
            items = data.get("task_instances", [])[:limit]
            tis = [
                {
                    "task_id": ti.get("task_id"),
                    "state": ti.get("state"),
                    "try_number": ti.get("try_number"),
                    "start_date": ti.get("start_date"),
                    "end_date": ti.get("end_date"),
                    "duration": ti.get("duration"),
                    "operator": ti.get("operator") or None,
                }
                for ti in items
            ]
            return to_json(
                {
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "count": len(tis),
                    "task_instances": tis,
                }
            )

    if want("get_task_instance"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_task_instance(
            ctx: Context, dag_id: str, dag_run_id: str, task_id: str
        ) -> str:
            """Get a single task instance (compact)."""
            client = get_client(ctx)
            return to_json(await client.get_task_instance(dag_id, dag_run_id, task_id))

    if want("get_task_logs"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_task_logs(
            ctx: Context,
            dag_id: str,
            dag_run_id: str,
            task_id: str,
            try_number: int = 1,
            full_content: bool = False,
            max_bytes: int | None = None,
            max_lines: int | None = None,
        ) -> str:
            """Fetch task logs with aggressive token-saving defaults.

            Default: returns the **last** ``max_lines`` lines up to
            ``max_bytes`` bytes (defaults from settings, 200 lines / 4 KB).
            Set ``full_content=True`` to disable truncation, or pass larger
            ``max_bytes`` / ``max_lines`` for more context.

            Note: logs are plain text, not JSON.
            """
            max_bytes = max_bytes if max_bytes is not None else settings.log_max_bytes
            max_lines = max_lines if max_lines is not None else settings.log_max_lines
            client = get_client(ctx)
            text = await client.get_task_logs(
                dag_id, dag_run_id, task_id, try_number=try_number, full_content=full_content
            )
            if not full_content:
                text = _truncate(text, max_bytes=max_bytes, max_lines=max_lines)
            return text

    if include_mutating and want("set_task_instance_state"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def set_task_instance_state(
            ctx: Context,
            dag_id: str,
            dag_run_id: str,
            task_id: str,
            new_state: NewState,
        ) -> str:
            """Force a task instance into a state (mutating — hidden in read-only)."""
            client = get_client(ctx)
            return to_json(
                await client.set_task_instance_state(dag_id, dag_run_id, task_id, new_state)
            )


def _truncate(text: str, *, max_bytes: int, max_lines: int) -> str:
    """Keep the last ``max_lines`` lines, then cap at ``max_bytes`` bytes.

    Returns text plus a trailing hint when truncation happens.
    """
    truncated = False
    if max_lines and max_lines > 0:
        lines = text.splitlines()
        if len(lines) > max_lines:
            text = "\n".join(lines[-max_lines:])
            truncated = True
    if max_bytes and max_bytes > 0 and len(text.encode("utf-8")) > max_bytes:
        # Keep the tail — that's where the error usually is.
        text = text.encode("utf-8")[-max_bytes:].decode("utf-8", errors="ignore")
        text = "...[truncated head]...\n" + text
        truncated = True
    if truncated:
        text += "\n... [truncated — call with full_content=True for the full log]"
    return text
