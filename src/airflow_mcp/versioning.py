"""Airflow version compatibility layer.

Airflow's stable REST API has evolved across the 2.x line and the 3.0
release. This module is the single place where we encode the differences:

* Which API path prefix to use (``/api/v1`` vs ``/api/v2``).
* Whether ``execution_date`` or ``logical_date`` is the canonical
  run-identifier field.
* Endpoint shape quirks (``set_task_instance_state`` moved from
  PATCH on the TI to POST on a ``/state`` sub-resource in v2).

The output of :func:`resolve_capabilities` is a :class:`Capabilities`
frozen dataclass that the client and tools read from. No version checks
are scattered around the codebase — they all funnel through here.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Literal

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Version matrix
# ---------------------------------------------------------------------------

#: Lowest minor version that ships a stable v2 endpoint (2.4.0).
_V2_MIN: tuple[int, int, int] = (2, 4, 0)

#: First version that REMOVED v1 entirely (3.0.0).
_V1_REMOVED: tuple[int, int, int] = (3, 0, 0)

#: Pre-release dev versions we accept (e.g. ``2.10.0.dev0``). Anything
#: that looks like a 4th version segment is rejected — ``2.2.5.6`` is not
#: a valid Airflow version.
_VERSION_RE = re.compile(
    r"^v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?:\.(?P<suffix>[\-+A-Za-z][\-+A-Za-z0-9.]*))?$"
)


def parse_version(raw: str | None) -> tuple[int, int, int] | None:
    """Parse a version string like ``2.2.5`` or ``v3.0.0.dev0`` into a tuple.

    Returns ``None`` for unparseable input.
    """
    if not raw:
        return None
    m = _VERSION_RE.match(raw.strip())
    if not m:
        return None
    return int(m["major"]), int(m["minor"]), int(m["patch"])


def _cmp(a: tuple[int, int, int], b: tuple[int, int, int]) -> int:
    """3-way compare; returns -1/0/1."""
    return (a > b) - (a < b)


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Capabilities:
    """Resolved per-target-version capabilities.

    The client and tools read this; nothing else should care about
    Airflow version numbers.
    """

    target_version: str  # raw string the user / server reported
    parsed: tuple[int, int, int]  # (major, minor, patch)
    api_version: Literal["v1", "v2"]

    # Field naming for DAG runs:
    uses_logical_date: bool  # 2.4+
    uses_execution_date: bool  # < 3.0 (kept as alias in 2.4-2.x)

    # set_task_instance_state endpoint shape:
    set_task_state_method: Literal["PATCH", "POST"]
    set_task_state_suffix: str  # "" for v1, "/state" for v2

    # Convenience flags for tool-level guards:
    has_v2_only_fields: bool  # e.g. ``note``, ``run_type`` in run payload
    supports_dry_run_clear: bool  # older 2.0 didn't support dry_run in clear


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def resolve_capabilities(
    *,
    target: str | None,
    detected: str | None = None,
) -> Capabilities:
    """Pick the API path and quirk set for a given target version.

    Args:
        target: User-configured target version (e.g. ``"2.2.5"``) or
            ``"auto"`` / ``None`` to use the detected version.
        detected: Version reported by the Airflow webserver
            (``/api/{v}/version``).

    Precedence:

    1. If ``target`` is explicit and parseable, it wins. We still log a
       warning when the target diverges from ``detected``.
    2. If ``target`` is ``"auto"`` / ``None``, fall back to ``detected``.
    3. If both are missing/unparseable, default to a safe legacy profile
       (2.2.5 / v1) and log a warning.
    """
    target_norm = (target or "").strip().lower()
    use_explicit = target_norm not in ("", "auto", "latest")

    chosen: tuple[int, int, int] | None
    raw: str
    if use_explicit:
        chosen = parse_version(target)
        if chosen is None:
            log.warning(
                "could not parse AIRFLOW_TARGET_VERSION=%r, falling back to auto",
                target,
            )
            use_explicit = False
        else:
            raw = target.strip()

    if not use_explicit:
        chosen = parse_version(detected) if detected else None
        raw = (detected or "2.2.5").strip()

    if chosen is None:
        log.warning(
            "no target version resolved (target=%r, detected=%r) — "
            "defaulting to Airflow 2.2.5 / v1 for safety",
            target,
            detected,
        )
        chosen = (2, 2, 5)
        raw = "2.2.5"

    # Warn on divergence.
    detected_parsed = parse_version(detected) if detected else None
    if detected_parsed and detected_parsed != chosen:
        log.warning(
            "configured target version %s differs from server-reported %s — "
            "honouring configured target; behaviour may not match server",
            ".".join(map(str, chosen)),
            ".".join(map(str, detected_parsed)),
        )

    api_version: Literal["v1", "v2"]
    if _cmp(chosen, _V1_REMOVED) >= 0:
        api_version = "v2"
    elif _cmp(chosen, _V2_MIN) >= 0:
        # 2.4 - 2.x supports both; default to v2 (the recommended one)
        # but the user can override via the explicit api_version setting.
        api_version = "v2"
    else:
        api_version = "v1"

    return Capabilities(
        target_version=raw,
        parsed=chosen,
        api_version=api_version,
        uses_logical_date=_cmp(chosen, _V2_MIN) >= 0,
        uses_execution_date=_cmp(chosen, _V1_REMOVED) < 0,
        set_task_state_method="POST" if api_version == "v2" else "PATCH",
        set_task_state_suffix="/state" if api_version == "v2" else "",
        has_v2_only_fields=api_version == "v2",
        supports_dry_run_clear=chosen >= (2, 2, 0),
    )


def override_api_version(
    caps: Capabilities, requested: Literal["v1", "v2", "auto"]
) -> Capabilities:
    """Override the api_version part of capabilities.

    ``"auto"`` keeps whatever the resolver picked. ``"v1"`` / ``"v2"``
    force that path even when the target version would prefer the other.
    """
    if requested == "auto":
        return caps
    if requested == caps.api_version:
        return caps
    log.info(
        "overriding resolved api_version=%s -> %s (target %s)",
        caps.api_version,
        requested,
        caps.target_version,
    )
    return Capabilities(
        target_version=caps.target_version,
        parsed=caps.parsed,
        api_version=requested,
        # If we're on v2, the field is logical_date; v1 uses execution_date.
        uses_logical_date=requested == "v2",
        uses_execution_date=requested == "v1",
        set_task_state_method="POST" if requested == "v2" else "PATCH",
        set_task_state_suffix="/state" if requested == "v2" else "",
        has_v2_only_fields=requested == "v2",
        supports_dry_run_clear=caps.supports_dry_run_clear,
    )
