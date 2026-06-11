"""Tool registration helpers.

Each submodule exposes a :func:`register` that wires its tools onto a
FastMCP server. The server entrypoint iterates over them in order,
filtering mutating tools out when ``read_only`` is true.
"""

from __future__ import annotations

from ..config import Settings
from . import dags, dag_runs, pools, system, tasks, variables

__all__ = ["dags", "dag_runs", "pools", "system", "tasks", "variables"]


def register_all(mcp: object, settings: Settings) -> None:
    """Register every tool submodule against a FastMCP instance.

    Semantics:

    * ``enabled_tools`` set → register **exactly** those tools. An explicit
      allowlist is the source of truth; ``read_only`` is ignored. (If the
      allowlist contains a mutating tool, you asked for it; you get it.)
    * ``enabled_tools`` unset + ``read_only=True`` → register only
      read-only tools.
    * ``enabled_tools`` unset + ``read_only=False`` → register everything.
    """
    allowlist = set(settings.enabled_tools) if settings.enabled_tools else None
    # When an allowlist is set, it overrides read-only: the user named the
    # tools explicitly.
    include_mut = not settings.read_only if allowlist is None else True
    kwargs = {"include_mutating": include_mut, "allowlist": allowlist}

    system.register(mcp, settings, **kwargs)
    dags.register(mcp, settings, **kwargs)
    dag_runs.register(mcp, settings, **kwargs)
    tasks.register(mcp, settings, **kwargs)
    variables.register(mcp, settings, **kwargs)
    pools.register(mcp, settings, **kwargs)
