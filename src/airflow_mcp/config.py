"""Configuration for the Airflow MCP server.

Settings are loaded from environment variables (or a ``.env`` file).
The key knobs that control *behaviour* are:

* ``target_version`` (default ``"auto"``) — the Airflow version this
  MCP server is talking to. Used to pick the right REST API path
  (``/api/v1`` vs ``/api/v2``) and to apply version-specific response
  shims (e.g. ``execution_date`` ↔ ``logical_date``). Set this to
  ``"2.2.5"``, ``"2.9.0"``, ``"3.0.0"`` etc. Or leave as ``"auto"``
  and the server will call ``/api/v2/version`` (falling back to
  ``/api/v1/version``) at startup.

* ``api_version`` (default ``"auto"``) — override the API path. Normally
  inferred from ``target_version``; set explicitly to ``"v1"`` or ``"v2"``
  if you need a specific one (e.g. your Airflow 2.7 deployment has a
  broken ``/api/v2`` proxy and you want to stay on v1).

* ``read_only`` (default ``true``) — only register read-only tools
  (``list_*``, ``get_*``, ``get_task_logs``, ``get_health``,
  ``get_version``). Mutating tools are hidden. Set
  ``AIRFLOW_READ_ONLY=false`` to expose them.

* ``enabled_tools`` — optional CSV allowlist of tool names. Overrides
  ``read_only`` when set.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Re-export the literal so callers can type-hint against it.
ApiVersionChoice = Literal["v1", "v2", "auto"]


class Settings(BaseSettings):
    """Runtime configuration for the MCP server."""

    model_config = SettingsConfigDict(
        env_prefix="AIRFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Connection ---------------------------------------------------------
    base_url: str = Field(
        default="http://localhost:8080",
        description="Base URL of the Airflow webserver (no trailing slash).",
    )
    api_version: ApiVersionChoice = Field(
        default="auto",
        description=("REST API path: 'v1', 'v2', or 'auto' (resolved from target_version)."),
    )
    target_version: str = Field(
        default="auto",
        description=(
            "Airflow version this server is talking to, e.g. '2.2.5', '2.9.0', "
            "'3.0.0'. Use 'auto' to call /api/v{1,2}/version at startup."
        ),
    )
    timeout: float = Field(default=30.0, gt=0, description="HTTP request timeout (s).")
    verify_ssl: bool = Field(default=True, description="Verify TLS certificates.")

    # --- Auth ----------------------------------------------------------------
    token: str | None = Field(default=None, description="Bearer token (if using token auth).")
    username: str | None = Field(default=None, description="Basic auth username.")
    password: str | None = Field(default=None, description="Basic auth password.")

    # --- Behaviour -----------------------------------------------------------
    user_agent: str = Field(default="airflow-mcp/0.2.0", description="HTTP User-Agent.")
    max_retries: int = Field(default=3, ge=0, description="Retries on transient HTTP errors.")
    retry_backoff: float = Field(default=0.5, gt=0, description="Exponential backoff base (s).")

    # --- Safety --------------------------------------------------------------
    read_only: bool = Field(
        default=True,
        description=(
            "If true, hide mutating tools (trigger/set/delete/pause/clear). "
            "Read-only by default — flip to false to expose write operations."
        ),
    )
    enabled_tools: list[str] | None = Field(
        default=None,
        description=(
            "Optional allowlist of tool names. When set, overrides read_only "
            "and only the listed tools are registered. Comma-separated in env."
        ),
    )

    # --- Token economy -------------------------------------------------------
    list_page_default: int = Field(default=20, ge=1, le=100, description="Default list page size.")
    list_page_max: int = Field(
        default=100, ge=1, le=1000, description="Hard cap on list page size."
    )
    log_max_bytes: int = Field(
        default=4096, ge=256, description="Soft cap on log bytes returned per call."
    )
    log_max_lines: int = Field(default=200, ge=10, description="Max log lines returned per call.")

    @field_validator("enabled_tools", mode="before")
    @classmethod
    def _split_csv(cls, v: object) -> object:
        if isinstance(v, str):
            v = [s.strip() for s in v.split(",") if s.strip()]
        return v

    @model_validator(mode="after")
    def _check_auth(self) -> Settings:
        if self.token is None and (self.username is None or self.password is None):
            raise ValueError(
                "Authentication is required. Set AIRFLOW_TOKEN, or both "
                "AIRFLOW_USERNAME and AIRFLOW_PASSWORD."
            )
        return self

    @property
    def auth_mode(self) -> Literal["token", "basic"]:
        return "token" if self.token is not None else "basic"
