"""Typed exception hierarchy for the Airflow MCP server.

The MCP layer catches these and returns a structured error back to the
client so the model sees something useful, not a raw stack trace.
"""

from __future__ import annotations


class AirflowMCPError(RuntimeError):
    """Base error for the Airflow MCP server."""


class AirflowConfigError(AirflowMCPError):
    """Configuration / startup error (bad URL, missing auth, ...)."""


class AirflowAPIError(AirflowMCPError):
    """Airflow returned a non-2xx response."""

    def __init__(
        self, message: str, *, status: int, endpoint: str, body: str | None = None
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.endpoint = endpoint
        self.body = body

    def to_user_message(self) -> str:
        """Compact, single-line message safe to surface to the model."""
        body = (self.body or "").strip()
        if len(body) > 400:
            body = body[:397] + "..."
        return f"[HTTP {self.status}] {self.endpoint}: {self.message}" + (
            f" — {body}" if body else ""
        )


class AirflowNotFoundError(AirflowAPIError):
    """Convenience subclass for 404s — usually means a bad DAG id / run id."""


class AirflowAuthError(AirflowAPIError):
    """401/403 — credentials are wrong or lack permission."""
