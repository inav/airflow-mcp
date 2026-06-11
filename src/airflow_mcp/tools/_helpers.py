"""Shared helpers for tool modules.

Token-saving is the priority here:

* :func:`to_compact` strips ``None`` / empty-string / empty-list /
  empty-dict values recursively and emits compact JSON. This typically
  shaves 30-60% off Airflow response payloads, which are full of
  ``null``/empty fields.
* :func:`to_json` is the public formatter used by every tool — it picks
  compact or pretty based on payload size.
* :func:`tool_errors` converts :class:`AirflowAPIError` into a single-line
  string the model can act on.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from ..client import AirflowClient
from ..errors import AirflowAPIError

T = TypeVar("T")

# Threshold: below this, use pretty JSON (easier for the model to skim);
# above, switch to compact (still valid JSON, just denser on the wire).
_PRETTY_THRESHOLD = 4_000  # bytes


def _strip_empties(value: Any) -> Any:
    """Recursively drop ``None`` / empty containers / empty strings."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            v2 = _strip_empties(v)
            if v2 is None:
                continue
            if isinstance(v2, (str, list, dict)) and len(v2) == 0:
                continue
            out[k] = v2
        return out
    if isinstance(value, list):
        return [
            _strip_empties(v) for v in value
            if v is not None and (not isinstance(v, str) or v != "")
        ]
    return value


def to_compact(payload: Any) -> str:
    """Compact JSON with all empty / null fields stripped."""
    cleaned = _strip_empties(payload)
    return json.dumps(cleaned, separators=(",", ":"), default=str)


def to_json(payload: Any) -> str:
    """Choose between pretty and compact based on size."""
    cleaned = _strip_empties(payload)
    text = json.dumps(cleaned, default=str)
    if len(text) <= _PRETTY_THRESHOLD:
        return json.dumps(cleaned, indent=2, sort_keys=True, default=str)
    return text  # already compact-ish (no indent)


def get_client(ctx: Any) -> AirflowClient:
    """Pull the shared :class:`AirflowClient` out of the MCP request context."""
    try:
        return ctx.request_context.lifespan_context["airflow"]  # type: ignore[no-any-return]
    except (AttributeError, KeyError) as exc:
        raise RuntimeError(
            "Airflow client is not initialised — did the server start?"
        ) from exc


def get_settings(ctx: Any):  # type: ignore[no-untyped-def]
    """Pull the shared :class:`Settings` out of the MCP request context."""
    try:
        return ctx.request_context.lifespan_context["settings"]  # type: ignore[no-any-return]
    except (AttributeError, KeyError) as exc:
        raise RuntimeError(
            "Settings are not initialised — did the server start?"
        ) from exc


def get_capabilities(ctx: Any):  # type: ignore[no-untyped-def]
    """Pull the resolved :class:`Capabilities` from the MCP request context."""
    try:
        return ctx.request_context.lifespan_context["capabilities"]  # type: ignore[no-any-return]
    except (AttributeError, KeyError) as exc:
        raise RuntimeError(
            "Capabilities are not initialised — did the server start?"
        ) from exc


def tool_errors(
    fn: Callable[..., Awaitable[str]],
) -> Callable[..., Awaitable[str]]:
    """Convert :class:`AirflowAPIError` into a clean user message."""

    @wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> str:
        try:
            return await fn(*args, **kwargs)
        except AirflowAPIError as exc:
            return f"Error: {exc.to_user_message()}"

    return wrapper
