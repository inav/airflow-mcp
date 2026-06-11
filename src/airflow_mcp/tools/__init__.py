"""Tool registration helpers.

Each submodule exposes a :func:`register` that wires its tools onto a
FastMCP server. The server entrypoint iterates over them in order,
filtering mutating tools out when ``read_only`` is true.
"""

from __future__ import annotations

from typing import Protocol

from ..config import Settings
from . import dag_runs, dags, pools, system, tasks, variables

__all__ = ["dag_runs", "dags", "pools", "system", "tasks", "variables"]


class _ToolRegister(Protocol):
    """Protocol for per-module ``register`` functions.

    Every tool submodule exposes a function of this shape — the project
    keeps them duck-typed on purpose to avoid a heavy import-time
    dependency on the FastMCP types in this file.
    """

    def __call__(
        self,
        mcp: object,
        settings: Settings,
        *,
        include_mutating: bool,
        allowlist: set[str] | None,
    ) -> None: ...


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

    # Iterate through typed callables so the **kwargs forwarding below
    # stays statically checked even though the FastMCP `mcp` arg is `object`.
    modules: tuple[_ToolRegister, ...] = (
        system.register,
        dags.register,
        dag_runs.register,
        tasks.register,
        variables.register,
        pools.register,
    )
    for register in modules:
        register(mcp, settings, include_mutating=include_mut, allowlist=allowlist)
