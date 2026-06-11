"""DAG-run tools. Read-only: list, get. Mutating: trigger, delete, clear."""

from __future__ import annotations

from typing import Any

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
    """Attach DAG-run tools to a FastMCP instance."""

    def want(name: str) -> bool:
        return allowlist is None or name in allowlist

    if want("list_dag_runs"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def list_dag_runs(
            ctx: Context,
            dag_id: str,
            limit: int = 0,
            offset: int = 0,
            state: list[str] | None = None,
            execution_date_gte: str | None = None,
            execution_date_lte: str | None = None,
            logical_date_gte: str | None = None,
            logical_date_lte: str | None = None,
            order_by: str | None = None,
        ) -> str:
            """List runs of a DAG (compact).

            Args:
                dag_id: The DAG identifier.
                limit: Max runs to return (0 → default 20, cap 100).
                offset: Pagination offset.
                state: Filter by state, e.g. ``["failed","running"]``.
                execution_date_gte: ISO-8601 lower bound (v1 only; ignored on 2.4+).
                execution_date_lte: ISO-8601 upper bound (v1 only; ignored on 2.4+).
                logical_date_gte: ISO-8601 lower bound (v2 / 2.4+; ignored on v1).
                logical_date_lte: ISO-8601 upper bound (v2 / 2.4+; ignored on v1).
                order_by: Sort key; prefix with ``-`` for descending.
                    Defaults to the canonical date field for the target version.
            """
            if limit <= 0:
                limit = settings.list_page_default
            limit = max(1, min(limit, settings.list_page_max))
            offset = max(0, offset)
            client = get_client(ctx)
            data = await client.list_dag_runs(
                dag_id,
                limit=limit,
                offset=offset,
                state=state,
                execution_date_gte=execution_date_gte,
                execution_date_lte=execution_date_lte,
                logical_date_gte=logical_date_gte,
                logical_date_lte=logical_date_lte,
                order_by=order_by
                or (
                    "-logical_date" if client.capabilities.uses_logical_date else "-execution_date"
                ),
            )
            runs = [
                {
                    "dag_run_id": r.get("dag_run_id"),
                    "state": r.get("state"),
                    "execution_date": r.get("execution_date"),
                    "start_date": r.get("start_date"),
                    "end_date": r.get("end_date"),
                    "run_type": r.get("run_type"),
                    "note": r.get("note"),
                    "external_trigger": r.get("external_trigger"),
                }
                for r in data.get("dag_runs", [])
            ]
            return to_json({"dag_id": dag_id, "count": len(runs), "dag_runs": runs})

    if want("get_dag_run"):

        @mcp.tool()  # type: ignore[union-attr]
        @tool_errors
        async def get_dag_run(ctx: Context, dag_id: str, dag_run_id: str) -> str:
            """Get a single DAG run (compact)."""
            client = get_client(ctx)
            return to_json(await client.get_dag_run(dag_id, dag_run_id))

    if include_mutating:
        if want("trigger_dag_run"):

            @mcp.tool()  # type: ignore[union-attr]
            @tool_errors
            async def trigger_dag_run(
                ctx: Context,
                dag_id: str,
                conf: dict[str, Any] | None = None,
                execution_date: str | None = None,
                note: str | None = None,
            ) -> str:
                """Trigger a manual run (mutating — hidden in read-only mode).

                Args:
                    dag_id: The DAG identifier.
                    conf: Optional ``dag_run.conf`` payload.
                    execution_date: ISO-8601 execution date (defaults to now).
                    note: Human-readable note attached to the run.
                """
                client = get_client(ctx)
                payload: dict[str, Any] = {}
                if conf is not None:
                    payload["conf"] = conf
                if execution_date is not None:
                    payload["execution_date"] = execution_date
                if note is not None:
                    payload["note"] = note
                return to_json(await client.trigger_dag_run(dag_id, payload))

        if want("delete_dag_run"):

            @mcp.tool()  # type: ignore[union-attr]
            @tool_errors
            async def delete_dag_run(ctx: Context, dag_id: str, dag_run_id: str) -> str:
                """Delete a DAG run (destructive, mutating — hidden in read-only mode)."""
                client = get_client(ctx)
                await client.delete_dag_run(dag_id, dag_run_id)
                return to_json({"dag_id": dag_id, "dag_run_id": dag_run_id, "deleted": True})

        if want("clear_dag_run"):

            @mcp.tool()  # type: ignore[union-attr]
            @tool_errors
            async def clear_dag_run(
                ctx: Context,
                dag_id: str,
                dag_run_id: str,
                dry_run: bool = True,
                reset_dag_run: bool = True,
                only_failed: bool = False,
            ) -> str:
                """Clear (re-schedule) task instances (mutating).

                Args:
                    dag_id: The DAG identifier.
                    dag_run_id: The run id.
                    dry_run: Report what *would* clear without changing state
                        (default true — must be set false to actually clear).
                    reset_dag_run: Reset the run state to ``running``.
                    only_failed: Only clear failed / up_for_retry tasks.
                """
                client = get_client(ctx)
                result = await client.clear_dag_run(
                    dag_id,
                    dag_run_id,
                    {
                        "dry_run": dry_run,
                        "reset_dag_run": reset_dag_run,
                        "only_failed": only_failed,
                    },
                )
                if isinstance(result, list):
                    return to_json({"dry_run": dry_run, "would_clear": result})
                return to_json(result)
