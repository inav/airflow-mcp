# airflow-mcp

[![CI](https://github.com/inav/airflow-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/inav/airflow-mcp/actions/workflows/ci.yml)
[![Release](https://github.com/inav/airflow-mcp/actions/workflows/release.yml/badge.svg)](https://github.com/inav/airflow-mcp/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Type-checked: mypy](https://img.shields.io/badge/type--checked-mypy-blue)](https://mypy.readthedocs.io)

A [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for
**Apache Airflow 2.x and 3.x**, built on the
[official MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk).

Talk to your Airflow from Claude Desktop, Cursor, Cline, or any MCP-aware
agent — inspect DAGs, trigger runs, fetch task logs, manage variables, read
pools, check cluster health. About two dozen tools, safe by default, sized
for LLM context windows.

---

## What you get

- **Multi-version** — Airflow 2.2 through 3.x. Point it at any version
  (`AIRFLOW_TARGET_VERSION=2.2.5`, `2.9.0`, `3.0.0`, …) or leave as
  `auto` to probe the server. The MCP picks the right API path
  (`/api/v1` vs `/api/v2`) and applies the right response shims
  (`execution_date` ↔ `logical_date`, `set_task_instance_state` endpoint
  shape, etc.).
- **Read-only by default** — mutating tools (`trigger_*`, `set_*`,
  `delete_*`, `pause_*`, `clear_*`) are hidden until you opt in with
  `AIRFLOW_READ_ONLY=false` or a custom `AIRFLOW_ENABLED_TOOLS` allowlist.
- **Token-conscious** — compact JSON, null/empty fields stripped, small
  default page sizes (20), aggressive log truncation (4 KB / 200 lines,
  tail only). Models don't have to wade through Airflow's verbose
  payloads.
- **Resilient** — exponential backoff on 408/425/429/5xx for idempotent
  methods; typed error hierarchy (`AirflowNotFoundError`,
  `AirflowAuthError`, `AirflowAPIError`).
- **Safe by default** — `list_variables` returns keys only;
  `clear_dag_run` defaults to `dry_run=True`.
- **Both auth modes** — bearer token if `AIRFLOW_TOKEN` is set, otherwise
  basic auth.

---

## Install

### 1. Install `uv` (once per machine)

`uv` is a tiny Rust binary that handles Python versions, virtual envs,
and dependency resolution in one tool. It's the only thing you install
globally — everything else lives inside the project's `.venv`.

```bash
# macOS / Linux / WSL
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

That's it. No `python -m venv`, no `pip install`, no `PATH` fiddling.

### 2. Open the project in VS Code

> This is the path you want. VS Code handles everything — the venv,
> the dependencies, the test runner, the linter, the type checker. You
> never type `uv` at the command line.

**Install the official Astral `uv` VS Code extension** (Astral
[maintains it](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff);
the same one that ships the linter and formatter). It teaches VS Code
to discover the project's `.venv` and the right Python interpreter
automatically.

Then:

1. **Clone the repo** (or download the source tarball from the
   [v0.2.0 release](https://github.com/inav/airflow-mcp/releases/tag/v0.2.0)).
2. **Open the folder** in VS Code
   (`File → Open Folder…`).
3. When the prompt *"We noticed a new virtual environment was created. Do
   you want to use it?"* appears, click **Yes**. (Or: open the
   Command Palette, `Python: Select Interpreter`, pick the
   `.venv/bin/python` it just made.)
4. Wait for the status bar to read `Python 3.12 ('.venv': venv)` —
   the extension will have already run the equivalent of
   `uv sync` for you in the background.
5. **Done.** Open `src/airflow_mcp/server.py`, hit ▶️ next to `main`,
   and the MCP server starts on stdio.

You'll also get:

- ✅ **Inline errors** from `ruff` (red squigglies, quick fixes via
  ⌘.)
- ✅ **Format on save** (also from `ruff`)
- ✅ **Type hints** from `mypy` (install the
  [Mypy extension](https://marketplace.visualstudio.com/items?itemName=ms-python.mypy-vscode)
  for inline type errors)
- ✅ **Test runner** in the Test Explorer (the Python extension finds
  `pytest` automatically)

If you ever need a terminal in the venv, VS Code's integrated terminal
activates it for you — no `source .venv/bin/activate` needed.

### 3. From the command line (optional)

If you don't use VS Code, or just want a quick sanity check from a
shell, the CLI is the same `uv` commands:

```bash
# Inside the cloned repo
uv sync --all-extras --dev        # creates .venv and installs everything
uv run pytest                     # run the test suite
uv run ruff check src/ tests/     # lint
uv run ruff format src/ tests/    # format
uv run mypy src/airflow_mcp       # type-check
uv run airflow-mcp                # run the server
```

`uv run …` is the equivalent of activating the venv and running the
command — it works from any directory inside the project tree.

### 4. Use it as a library

```bash
# From a git tag (works without PyPI)
uv add "airflow-mcp @ git+https://github.com/inav/airflow-mcp@v0.2.0"
# or, once published to PyPI
uv add airflow-mcp
# or, classic pip
pip install airflow-mcp
```

---

## Wire it into an MCP client

The server speaks stdio by default. Each client has a slightly different
config spot.

### Claude Desktop

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

Restart Claude Desktop — the tools appear under the 🔨 icon.

### Cursor

`Cursor → Settings → Cursor Settings → MCP → Add new global MCP server`.
Same JSON shape as above.

### Cline / Continue / other VS Code MCP clients

Each one has its own MCP config — usually a JSON file in the extension's
data directory. Point it at the `airflow-mcp` command with the env vars
you need.

> **Heads up:** the MCP client invokes `airflow-mcp` directly, so that
> command must be on `PATH`. If you only have `uv` and not the
> installed package, use `command: "uv"` and
> `args: ["run", "--with", "airflow-mcp @ git+https://github.com/inav/airflow-mcp@v0.2.0", "airflow-mcp"]`
> instead.

---

## Configuration

Every setting is an environment variable, also readable from a `.env`
file in the working directory. `cp .env.example .env` to start.

| Variable                    | Required | Default                 | Notes                                                                |
|-----------------------------|----------|-------------------------|----------------------------------------------------------------------|
| `AIRFLOW_BASE_URL`          | no       | `http://localhost:8080` | Airflow **webserver** base URL                                       |
| `AIRFLOW_TARGET_VERSION`    | no       | `auto`                  | Target Airflow version, e.g. `2.2.5`, `2.9.0`, `3.0.0`. `auto` probes at startup. |
| `AIRFLOW_API_VERSION`       | no       | `auto`                  | Force `v1` / `v2` regardless of target.                             |
| `AIRFLOW_TOKEN`             | one of   | —                       | Bearer token (wins over basic auth)                                 |
| `AIRFLOW_USERNAME`          | one of   | —                       | Required if `AIRFLOW_TOKEN` is unset                                |
| `AIRFLOW_PASSWORD`          | one of   | —                       | Required if `AIRFLOW_TOKEN` is unset                                |
| `AIRFLOW_READ_ONLY`         | no       | `true`                  | Hide mutating tools when true                                       |
| `AIRFLOW_ENABLED_TOOLS`     | no       | —                       | CSV allowlist; overrides `AIRFLOW_READ_ONLY`                        |
| `AIRFLOW_LIST_PAGE_DEFAULT` | no       | `20`                    | Default list page size                                              |
| `AIRFLOW_LIST_PAGE_MAX`     | no       | `100`                   | Hard cap on list page size                                          |
| `AIRFLOW_LOG_MAX_BYTES`     | no       | `4096`                  | Soft cap on log bytes per call                                      |
| `AIRFLOW_LOG_MAX_LINES`     | no       | `200`                   | Max log lines per call (tail only)                                  |
| `AIRFLOW_TIMEOUT`           | no       | `30`                    | Per-request HTTP timeout (seconds)                                  |
| `AIRFLOW_VERIFY_SSL`        | no       | `true`                  | Set `false` for self-signed certs                                   |
| `AIRFLOW_MAX_RETRIES`       | no       | `3`                     | Retries on transient HTTP errors                                    |
| `AIRFLOW_MCP_LOG_LEVEL`     | no       | `INFO`                  | `DEBUG` / `INFO` / `WARNING` / `ERROR`                              |
| `AIRFLOW_MCP_TRANSPORT`     | no       | `stdio`                 | `stdio` or `sse`                                                    |

### Read-only mode

By default only these 15 tools are registered:

`get_health`, `get_version`, `get_capabilities`, `list_dags`, `get_dag`,
`list_dag_runs`, `get_dag_run`, `list_tasks`, `get_task`,
`list_task_instances`, `get_task_instance`, `get_task_logs`,
`list_variables`, `get_variable`, `list_pools`, `get_pool`.

To expose mutating tools (`pause_dag`, `unpause_dag`, `trigger_dag_run`,
`delete_dag_run`, `clear_dag_run`, `set_task_instance_state`,
`set_variable`, `delete_variable`) set `AIRFLOW_READ_ONLY=false`.

For a custom allowlist, set `AIRFLOW_ENABLED_TOOLS` to a comma-separated
list. The allowlist **overrides** `AIRFLOW_READ_ONLY` — name a mutating
tool there and you get it.

```bash
# Just the safe basics
export AIRFLOW_ENABLED_TOOLS=list_dags,get_dag,list_dag_runs,get_health

# Mix: read-only + one mutating tool (allowlist overrides read-only)
export AIRFLOW_ENABLED_TOOLS=list_dags,get_dag,trigger_dag_run

# Full access
export AIRFLOW_READ_ONLY=false
```

---

## Supported Airflow versions

| Airflow     | API path                          | Date field                          | `set_task_state` endpoint        |
|-------------|-----------------------------------|-------------------------------------|----------------------------------|
| 2.2.x       | `/api/v1`                         | `execution_date`                    | `PATCH /taskInstances/{id}`      |
| 2.3.x       | `/api/v1`                         | `execution_date`                    | `PATCH /taskInstances/{id}`      |
| 2.4.x       | `/api/v2` (v1 still works)        | `logical_date` (+ alias)            | `POST /taskInstances/{id}/state` |
| 2.5 – 2.9.x | `/api/v2`                         | `logical_date` (+ alias)            | `POST /taskInstances/{id}/state` |
| 2.10.x      | `/api/v2`                         | `logical_date` (+ alias)            | `POST /taskInstances/{id}/state` |
| 3.0.x       | `/api/v2` (v1 removed)            | `logical_date` only                 | `POST /taskInstances/{id}/state` |

The MCP normalises responses — `get_dag_run` always returns both
`logical_date` and `execution_date` keys (the one Airflow didn't include
is filled in from the other). `trigger_dag_run` accepts either key in
the payload and renames it for the target version.

Use the `get_capabilities` tool at runtime to see what was resolved:

```
get_capabilities
→ {"target_version":"2.9.0","api_version":"v2",
   "uses_logical_date":true,"set_task_state_method":"POST",...}
```

---

## Tools

| Tool                          | Mode         | What it does                                                            |
|-------------------------------|--------------|-------------------------------------------------------------------------|
| `get_health`                  | read-only    | Probe `/health` on the webserver                                        |
| `get_version`                 | read-only    | Airflow version (handy to confirm 2.x vs 3.x)                           |
| `get_capabilities`            | read-only    | Resolved target version + API path + capability flags                   |
| `list_dags`                   | read-only    | List DAGs (filter by active / pattern / tags)                           |
| `get_dag`                     | read-only    | Full DAG definition                                                     |
| `pause_dag` / `unpause_dag`   | **mutating** | Toggle scheduling                                                       |
| `list_dag_runs`               | read-only    | List runs of a DAG (`logical_*` or `execution_*` date filters)          |
| `get_dag_run`                 | read-only    | One run                                                                |
| `trigger_dag_run`             | **mutating** | Manual run, with `conf` and `note`                                      |
| `delete_dag_run`              | **mutating** | Delete a run (destructive)                                              |
| `clear_dag_run`               | **mutating** | Clear task instances (`dry_run=True` by default)                        |
| `list_tasks`                  | read-only    | Tasks in a DAG                                                          |
| `get_task`                    | read-only    | One task definition                                                     |
| `list_task_instances`         | read-only    | TIs of a run (filter by state)                                          |
| `get_task_instance`           | read-only    | One TI                                                                 |
| `get_task_logs`               | read-only    | Logs (truncated by default; `full_content=True` for everything)         |
| `set_task_instance_state`     | **mutating** | Force a TI to `success` / `failed` / `skipped` / `up_for_retry`         |
| `list_variables`              | read-only    | Variable **keys only** (safe)                                           |
| `get_variable`                | read-only    | One variable's value                                                    |
| `set_variable`                | **mutating** | Create or update a variable                                             |
| `delete_variable`             | **mutating** | Delete a variable                                                       |
| `list_pools`                  | read-only    | Worker pools                                                            |
| `get_pool`                    | read-only    | One pool                                                                |

---

## How it works

```
┌────────────────┐    stdio / sse     ┌──────────────────┐    httpx     ┌────────────┐
│  MCP client    │ ◀───────────────▶ │   airflow-mcp    │ ◀──────────▶ │  Airflow   │
│  (Claude,      │   JSON-RPC        │   (this server)  │  /api/v1 or  │  webserver │
│   Cursor, …)   │                   │                  │   /api/v2    │            │
└────────────────┘                   └──────────────────┘              └────────────┘
                                            │
                                            ├── versioning.py    version matrix → API path + shims
                                            ├── client.py        async httpx, retries, version-aware
                                            ├── config.py        pydantic-settings, env-driven
                                            ├── errors.py        typed exception hierarchy
                                            └── tools/           one module per Airflow resource
```

- **`server.py`** — FastMCP entrypoint; loads `Settings`, resolves
  capabilities, then calls `register_all` to wire up the tool modules.
- **`client.py`** — async `httpx.AsyncClient` wrapper, one connection
  pool for the server's lifetime. Retries 408/425/429/5xx with
  exponential backoff for idempotent methods.
- **`versioning.py`** — the core "magic". Given a target version
  (`2.2.5`, `2.9.0`, `3.0.0`, …) or `auto`, it picks `/api/v1` vs
  `/api/v2` and a set of capability flags (`uses_logical_date`,
  `set_task_state_method`, …) that the rest of the code branches on.
- **`tools/`** — one module per Airflow resource
  (`dags`, `dag_runs`, `tasks`, `variables`, `pools`, `system`).
  Each `register(mcp, settings, *, include_mutating, allowlist)`
  adds its tools to the FastMCP server and filters by mode.

---

## Development

### In VS Code (recommended)

The flow is just "open the folder". The `uv` extension handles sync,
the Python Test Explorer finds `pytest`, the `ruff` extension lints and
formats on save, and the `mypy` extension shows type errors inline.

### In a terminal

```bash
uv sync --all-extras --dev
uv run pytest                  # 62 tests, ~12s
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run mypy src/airflow_mcp
```

The all-in-one quality gate (what CI runs):

```bash
uv run ruff check src/ tests/ && \
  uv run ruff format --check src/ tests/ && \
  uv run mypy src/airflow_mcp && \
  uv run pytest
```

### Project layout

```
airflow-mcp/
├── pyproject.toml          # hatch-vcs, ruff, mypy, pytest, coverage config
├── conftest.py             # pytest bootstrap; make src/ importable
├── .python-version         # pins Python 3.12 for uv and the test matrix
├── uv.lock                 # full dependency graph (committed)
├── .env.example            # every config var, with sensible defaults
├── src/airflow_mcp/
│   ├── __init__.py         # version comes from hatch-vcs at build time
│   ├── config.py           # pydantic-settings, env-driven
│   ├── errors.py           # typed exception hierarchy
│   ├── versioning.py       # version matrix + capability resolver
│   ├── client.py           # async httpx wrapper, retries, version-aware
│   ├── server.py           # FastMCP entrypoint + CLI
│   └── tools/
│       ├── _helpers.py     # JSON formatting, ctx helpers, error decorator
│       ├── system.py       # health / version / capabilities
│       ├── dags.py
│       ├── dag_runs.py
│       ├── tasks.py
│       ├── variables.py
│       └── pools.py
└── tests/
    ├── test_smoke.py       # config + wiring + read-only filter
    ├── test_client.py      # mocked httpx transport (v1 + v2)
    └── test_versioning.py  # version resolution + auto-detect
```

---

## Versioning & releases

The package version is **derived from git tags** by
[`hatch-vcs`](https://github.com/jdillard/hatch-vcs) — there is no
version string to bump in `pyproject.toml` or `__init__.py`. The tag
*is* the version.

```bash
git tag v0.2.0          # pick one
git tag v0.2.1
git tag v0.3.0

git push origin --tags  # fires the release workflow
```

Pushing a `vX.Y.Z` tag triggers [`.github/workflows/release.yml`](.github/workflows/release.yml):

1. Build the sdist and wheel.
2. Publish to PyPI via
   [trusted publishing](https://docs.pypi.org/trusted-publishers/) (OIDC,
   no API token to manage).
3. Create a GitHub Release with auto-generated notes.

Versions in between tags (commits on `main`) automatically get a
`X.Y.Z.devN+gHASH.dDATE` suffix, so every dev install is uniquely
identifiable.

### One-time PyPI setup

Before the first release, configure trusted publishing at
<https://pypi.org/manage/account/publishing/> with:

- **Owner / Project**: `inav/airflow-mcp`
- **Workflow file**: `release.yml`
- **Environment name**: `pypi`

Then create the `pypi` environment in
**Settings → Environments** so the release job can request it.

### Semver policy

Until `v1.0.0`, the minor line (`v0.2.x` → `v0.3.0`) is also where
breaking changes go, with a CHANGELOG note. After `v1.0.0`:

- **Patch** (`vX.Y.Z` → `vX.Y.Z+1`) — bug fixes only, no public-API change
- **Minor** (`vX.Y` → `vX.(Y+1)`) — additive features, no breaking change
- **Major** (`vX` → `v(X+1)`) — breaking changes to MCP tool signatures,
  settings, or environment variables

---

## Troubleshooting

**"Python was not found" / VS Code status bar shows the system Python, not `.venv`.**

The `uv` extension didn't sync yet. Run
`Python: Reset Workspace Trust → Reload` in the Command Palette, or
`> uv: Sync` if the extension exposes it. Or, from a terminal:
`uv sync`.

**`uv run` says "No project found" / `.venv` doesn't exist.**

You're not in the repo root. `cd` into the folder that contains
`pyproject.toml`.

**Tools appear in the client but every call returns an auth error.**

The MCP client launches the server with whatever env vars are in the
config (or no env vars at all). Put the full env block in the MCP
client config, not in a `.env` file the client can't see.

**`AIRFLOW_TARGET_VERSION=auto` resolves to the wrong version.**

Either set it explicitly, or upgrade your webserver to a version where
`/api/v2/version` is accessible to the auth user (some 2.4 / 2.5
releases lock that endpoint behind admin).

**CI is red on `Lint (ruff)`.**

Run `uv run ruff check src/ tests/` locally and fix what it shows.
The CI lint job runs against the synced `.venv`, so make sure
`uv sync` works for you first.

**CI says `Unrecognized named-value: 'runner.temp'`.**

The workflow file references `runner.temp` at the top level — that's
not valid in GitHub Actions (that context is step-scoped). Edit
`.github/workflows/ci.yml` and move any `runner.*` references into a
step's `env:` block.

---

## License

[Apache-2.0](LICENSE)
