# airflow-mcp

[![CI](https://github.com/inav/airflow-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/inav/airflow-mcp/actions/workflows/ci.yml)
[![Release](https://github.com/inav/airflow-mcp/actions/workflows/release.yml/badge.svg)](https://github.com/inav/airflow-mcp/actions/workflows/release.yml)

A Model Context Protocol (MCP) server for **Apache Airflow 2.x and 3.x**,
built on the [official MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).

It exposes ~24 tools that an LLM agent (Claude Desktop, Cursor, Cline, ...)
can call to inspect DAGs, trigger runs, fetch task logs, manage variables,
read pools, and check cluster health. Talks both `/api/v1` (legacy) and
`/api/v2` (stable on 2.4+), and picks the right one for your target
version automatically.

## Features

- **Multi-version** — targets Airflow 2.2.x through 3.x. Set
  `AIRFLOW_TARGET_VERSION=2.2.5` / `2.9.0` / `3.0.0` etc., or leave
  as `auto` to probe at startup. The MCP picks the correct API path
  (`/api/v1` vs `/api/v2`) and applies the right response shims
  (`execution_date` ↔ `logical_date`, `set_task_instance_state`
  endpoint shape, etc.).
- **Read-only by default** — mutating tools (`trigger_*`, `set_*`,
  `delete_*`, `pause_*`, `clear_*`) are hidden unless you opt in.
  Set `AIRFLOW_READ_ONLY=false` to expose them, or `AIRFLOW_ENABLED_TOOLS`
  for a custom allowlist.
- **Token-optimised** — compact JSON output, null/empty fields stripped,
  small default page sizes (20), and aggressive log truncation
  (default 4 KB / 200 lines, tail only). The model never has to wade
  through Airflow's verbose response payloads.
- **Comprehensive** — DAGs, runs, task instances, variables, pools, system.
- **Async-first** — built on `httpx.AsyncClient` with a single shared
  connection pool across the MCP lifetime.
- **Resilient** — exponential-backoff retries on 408/425/429/5xx for
  idempotent methods; typed error hierarchy (`AirflowNotFoundError`,
  `AirflowAuthError`, `AirflowAPIError`).
- **Safe by default** — `list_variables` returns keys only, never values;
  `clear_dag_run` defaults to `dry_run=True`.
- **Both auth modes** — auto-detect: `AIRFLOW_TOKEN` if set, else basic auth.

## Install

The project uses [uv](https://docs.astral.sh/uv/) for everything — no
manual `venv` juggling, no system-level `pip install` needed. If you don't
have it yet:

```bash
# macOS / Linux / WSL
curl -LsSf https://astral.sh/uv/install.sh | sh
# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Then, from the repo root:

```bash
# Resolves and pins every dependency into uv.lock, then installs the
# project (including the [dev] extras) into a project-local .venv.
uv sync --all-extras --dev

# Run the MCP server from the venv without ever activating it.
uv run airflow-mcp
```

To install the published package into an existing project:

```bash
uv add airflow-mcp
# or, classic pip in a venv you manage yourself:
pip install airflow-mcp
```

Tested with Python 3.10 / 3.11 / 3.12.

## Configuration

All settings come from environment variables (or a `.env` file at the
working directory):

| Variable              | Required | Default                  | Notes                                      |
|-----------------------|----------|--------------------------|--------------------------------------------|
| `AIRFLOW_BASE_URL`    | no       | `http://localhost:8080`  | Airflow **webserver** base URL             |
| `AIRFLOW_TARGET_VERSION` | no    | `auto`                   | Target Airflow version, e.g. `2.2.5`, `2.9.0`, `3.0.0`. `auto` probes the server. |
| `AIRFLOW_API_VERSION` | no       | `auto`                   | Force `v1` / `v2` regardless of target.    |
| `AIRFLOW_TOKEN`       | one of   | —                        | Bearer token (wins over basic auth)        |
| `AIRFLOW_USERNAME`    | one of   | —                        | Required if `AIRFLOW_TOKEN` is unset       |
| `AIRFLOW_PASSWORD`    | one of   | —                        | Required if `AIRFLOW_TOKEN` is unset       |
| `AIRFLOW_READ_ONLY`   | no       | `true`                   | Hide mutating tools when true              |
| `AIRFLOW_ENABLED_TOOLS` | no     | —                        | CSV allowlist; overrides `AIRFLOW_READ_ONLY` |
| `AIRFLOW_LIST_PAGE_DEFAULT` | no | `20`                    | Default list page size                     |
| `AIRFLOW_LIST_PAGE_MAX` | no    | `100`                    | Hard cap on list page size                 |
| `AIRFLOW_LOG_MAX_BYTES` | no    | `4096`                   | Soft cap on log bytes per call             |
| `AIRFLOW_LOG_MAX_LINES` | no    | `200`                    | Max log lines per call (tail only)         |
| `AIRFLOW_TIMEOUT`     | no       | `30`                     | Per-request HTTP timeout (s)               |
| `AIRFLOW_VERIFY_SSL`  | no       | `true`                   | Set `false` for self-signed certs          |
| `AIRFLOW_MAX_RETRIES` | no       | `3`                      | Retries on transient HTTP errors           |
| `AIRFLOW_MCP_LOG_LEVEL` | no     | `INFO`                   | `DEBUG` / `INFO` / `WARNING` / `ERROR`     |
| `AIRFLOW_MCP_TRANSPORT` | no     | `stdio`                  | `stdio` or `sse`                           |

## Supported Airflow versions

| Airflow   | API path        | Date field              | `set_task_state` endpoint           |
|-----------|-----------------|-------------------------|-------------------------------------|
| 2.2.x     | `/api/v1`       | `execution_date`        | `PATCH /taskInstances/{id}`         |
| 2.3.x     | `/api/v1`       | `execution_date`        | `PATCH /taskInstances/{id}`         |
| 2.4.x     | `/api/v2` (default; v1 still works) | `logical_date` (+ `execution_date` alias) | `POST /taskInstances/{id}/state`    |
| 2.5.x – 2.9.x | `/api/v2`   | `logical_date` (+ alias)| `POST /taskInstances/{id}/state`    |
| 2.10.x    | `/api/v2`       | `logical_date` (+ alias)| `POST /taskInstances/{id}/state`    |
| 3.0.x     | `/api/v2` (v1 removed) | `logical_date` only | `POST /taskInstances/{id}/state`    |

The MCP normalises responses for you — `get_dag_run` always returns both
`logical_date` and `execution_date` keys (the one Airflow didn't include
is filled in from the other). `trigger_dag_run` accepts either key in
the payload and renames it for the target version.

Set the target explicitly for best behaviour:

```bash
export AIRFLOW_TARGET_VERSION=2.2.5   # pick the right API path
export AIRFLOW_TARGET_VERSION=2.9.0
export AIRFLOW_TARGET_VERSION=3.0.0
export AIRFLOW_TARGET_VERSION=auto    # probe at startup
```

Or pin the API path while keeping the version's quirks:

```bash
export AIRFLOW_TARGET_VERSION=2.7.0
export AIRFLOW_API_VERSION=v1         # use v1 even though v2 is the default
```

Use the `get_capabilities` tool at runtime to see what was resolved:

```
get_capabilities
→ {"target_version":"2.9.0","api_version":"v2",
   "uses_logical_date":true,"set_task_state_method":"POST",...}
```

### Read-only mode

By default only the following tools are registered (15 total):

`get_health`, `get_version`, `list_dags`, `get_dag`, `list_dag_runs`,
`get_dag_run`, `list_tasks`, `get_task`, `list_task_instances`,
`get_task_instance`, `get_task_logs`, `list_variables`, `get_variable`,
`list_pools`, `get_pool`.

To expose mutating tools (`pause_dag`, `unpause_dag`, `trigger_dag_run`,
`delete_dag_run`, `clear_dag_run`, `set_task_instance_state`,
`set_variable`, `delete_variable`) set `AIRFLOW_READ_ONLY=false`.

For a custom allowlist, set `AIRFLOW_ENABLED_TOOLS` to a comma-separated
list of tool names. The allowlist **overrides** `AIRFLOW_READ_ONLY` — if
you name a mutating tool there, you get it.

```bash
# Just the safe basics
export AIRFLOW_ENABLED_TOOLS=list_dags,get_dag,list_dag_runs,get_health

# Explicit opt-in to a single mutating tool (still read-only otherwise)
export AIRFLOW_ENABLED_TOOLS=list_dags,get_dag,trigger_dag_run

# Full access
export AIRFLOW_READ_ONLY=false
```

### Example: basic auth against a local Airflow

```bash
export AIRFLOW_BASE_URL=http://localhost:8080
export AIRFLOW_USERNAME=airflow
export AIRFLOW_PASSWORD=airflow
```

### Example: API token (e.g. MWAA, Astronomer)

```bash
export AIRFLOW_BASE_URL=https://my-env.astronomer.run
export AIRFLOW_TOKEN=xxxxxxxxxxxxxxxx
```

## Run

```bash
# stdio (default — for Claude Desktop, Cursor, Cline, ...)
airflow-mcp

# SSE transport (HTTP)
airflow-mcp --transport sse

# debug logging
airflow-mcp --log-level DEBUG
```

## Wire it into Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "airflow": {
      "command": "airflow-mcp",
      "env": {
        "AIRFLOW_BASE_URL": "http://localhost:8080",
        "AIRFLOW_USERNAME": "airflow",
        "AIRFLOW_PASSWORD": "airflow"
      }
    }
  }
}
```

## Tools

| Tool                       | Mode        | What it does                                                          |
|----------------------------|-------------|------------------------------------------------------------------------|
| `get_health`               | read-only   | Probe `/health` on the webserver                                       |
| `get_version`              | read-only   | Airflow version (handy to confirm 2.x vs 3.x)                         |
| `get_capabilities`         | read-only   | Report the resolved target version + API path + capability flags      |
| `list_dags`                | read-only   | List DAGs (filter by active / pattern / tags)                         |
| `get_dag`                  | read-only   | Get full DAG definition                                                |
| `pause_dag` / `unpause_dag`| **mutating**| Toggle scheduling                                                      |
| `list_dag_runs`            | read-only   | List runs of a DAG (filter by state, date range — `logical_*` or `execution_*`) |
| `get_dag_run`              | read-only   | Get a single run                                                       |
| `trigger_dag_run`          | **mutating**| Trigger a manual run, with optional `conf` and `note`                 |
| `delete_dag_run`           | **mutating**| Delete a run (destructive)                                             |
| `clear_dag_run`            | **mutating**| Clear task instances (`dry_run=True` by default)                      |
| `list_tasks`               | read-only   | List tasks in a DAG                                                    |
| `get_task`                 | read-only   | Get a single task definition                                           |
| `list_task_instances`      | read-only   | List TIs of a run (filter by state)                                    |
| `get_task_instance`        | read-only   | Get one TI                                                             |
| `get_task_logs`            | read-only   | Fetch logs (truncated by default; `full_content=True` for the lot)    |
| `set_task_instance_state`  | **mutating**| Force a TI to `success` / `failed` / `skipped` / `up_for_retry`        |
| `list_variables`           | read-only   | List variable **keys only** (safe)                                    |
| `get_variable`             | read-only   | Read a single variable's value                                         |
| `set_variable`             | **mutating**| Create or update a variable                                            |
| `delete_variable`          | **mutating**| Delete a variable                                                      |
| `list_pools`               | read-only   | List worker pools                                                      |
| `get_pool`                 | read-only   | Get one pool                                                           |

## Airflow version notes

- **2.2.x – 2.3.x**: only `/api/v1` is available. `execution_date` is the
  canonical run date. `set_task_instance_state` is a `PATCH` on the task
  instance. Both work natively; no shims needed.
- **2.4.0+**: `/api/v2` is the recommended path; v1 still works. Both
  `logical_date` and `execution_date` are present in v2 responses (the
  former is canonical, the latter is an alias kept for back-compat).
  `set_task_instance_state` moved to `POST /taskInstances/{id}/state`.
- **3.0.x**: `/api/v1` is removed. `logical_date` is the only date field.
- The experimental API under `/api/experimental` is **not** used here.
- The webserver must be configured with an auth backend that accepts
  basic auth *or* a static token (e.g. `airflow.api.auth.backend.basic_auth`
  for 2.x, or FAB auth for 3.x). The MCP server doesn't configure
  Airflow — it just speaks the protocol.

## Development

```bash
# Bring up the dev environment (uses uv.lock, no surprises).
uv sync --all-extras --dev

# Run the test suite (62 tests, ~12s).
uv run pytest

# Lint + format (ruff bundles both).
uv run ruff check src/ tests/
uv run ruff format src/ tests/

# Type-check (mypy strict-ish, see pyproject.toml for the per-code disables).
uv run mypy src/airflow_mcp

# All-in-one:
uv run ruff check src/ tests/ && \
  uv run ruff format --check src/ tests/ && \
  uv run mypy src/airflow_mcp && \
  uv run pytest
```

## Versioning & releases

The package version is **derived from git tags** by
[`hatch-vcs`](https://github.com/jdillard/hatch-vcs) — there is no version
string to bump in `pyproject.toml` or `__init__.py`. Just cut a tag:

```bash
# Pick one of:
git tag v0.2.0
git tag v0.2.1
git tag v0.3.0

git push origin --tags
```

Pushing a `vX.Y.Z` tag triggers the
[release workflow](.github/workflows/release.yml), which:

1. Builds the sdist and wheel.
2. Publishes to PyPI via
   [trusted publishing](https://docs.pypi.org/trusted-publishers/) (OIDC,
   no API token to manage).
3. Creates a GitHub Release with auto-generated notes.

Versions in between tags (commits on `main`) automatically get a
`X.Y.Z.devN+gHASH.dDATE` suffix, so every dev install is uniquely
identifiable.

### One-time PyPI setup

Before the first release, configure trusted publishing at
<https://pypi.org/manage/account/publishing/> with:

- **Owner / Project**: `inav/airflow-mcp`
- **Workflow file**: `release.yml`
- **Environment name**: `pypi`

Then create the `pypi` environment in your repo's Settings → Environments
so the release job can request it.

## Project layout

```
airflow-mcp/
├── pyproject.toml
├── conftest.py
├── src/airflow_mcp/
│   ├── __init__.py
│   ├── config.py         # pydantic-settings, env-driven
│   ├── errors.py         # typed exception hierarchy
│   ├── versioning.py     # version matrix + capability resolver
│   ├── client.py         # async httpx wrapper, retries, version-aware
│   ├── server.py         # FastMCP entrypoint + CLI
│   └── tools/
│       ├── _helpers.py   # JSON formatting, ctx helpers, error decorator
│       ├── system.py     # health / version / capabilities
│       ├── dags.py
│       ├── dag_runs.py
│       ├── tasks.py
│       ├── variables.py
│       └── pools.py
└── tests/
    ├── test_smoke.py     # config + wiring + read-only filter
    ├── test_client.py    # mocked HTTPX transport (v1 + v2)
    └── test_versioning.py # version resolution + auto-detect
```

## License

Apache-2.0
