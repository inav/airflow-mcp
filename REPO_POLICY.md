# Repository policy

This is a personal project (single-owner). The rules below exist to make
the project self-enforce the things the owner would otherwise have to
remember to do by hand.

## Branch protection on `main`

Configured via `scripts/enable-branch-protection.sh`. Verifiable at
<https://github.com/inav/airflow-mcp/settings/branches>.

| Rule                              | Why                                                            |
|-----------------------------------|----------------------------------------------------------------|
| Require linear history            | Keeps `git log --graph` readable; no surprise merge commits.    |
| Block force-pushes                | Tags and `uv.lock` history are load-bearing — never rewrite.    |
| Block branch deletion             | `main` is the only branch that matters; it must not vanish.     |
| Require status checks (strict)    | Even an admin push must pass lint / typecheck / test.           |
| Require conversation resolution   | A self-review by the author (you) is fine; the rule is about   |
|                                   | not leaving unresolved review threads behind on a merged PR.   |
| Enforce admins                    | Owners can't bypass either. Re-enable to override in a hurry.  |
| `prevent_self_review: false` on `pypi` env | A solo owner is the only reviewer. If `prevent_self_review` were on, releases would deadlock (you can't approve your own deploy). Keep it off; the rule buys nothing for a one-person team and breaks the flow. |

The required status checks are pinned to the exact `name:` values in
[`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- `Lint (ruff)`
- `Type-check (mypy)`
- `Test (Python 3.10)` / `3.11` / `3.12`

`strict: true` means "the most recent commit on the PR is the one being
tested" — no stale green checks from a previous push can satisfy the rule.

## Release flow

1. Cut a tag locally: `git tag vX.Y.Z && git push origin --tags`.
2. The `release` workflow runs:
   - builds sdist + wheel,
   - **pauses** at the `pypi` environment (manual approval by the owner),
   - publishes to PyPI via trusted publishing (OIDC, no API token),
   - creates a GitHub Release with auto-generated notes.
3. The `pypi` environment is configured to **only** allow deployments from
   the `release.yml` workflow — no other workflow can accidentally
   publish.

If you ever need to skip the approval gate for a hotfix, do it from
`https://github.com/inav/airflow-mcp/settings/environments` and re-enable
it before the next release.

## Versioning

The package version is derived from git tags by
[`hatch-vcs`](https://github.com/jdillard/hatch-vcs). There is no
version string to bump anywhere in the source. The tag *is* the
version.

- `git tag v0.2.0` → published as `0.2.0`.
- A commit on `main` between tags → reported as `X.Y.Z.devN+gHASH.dDATE`
  in editable installs.

We follow [Semantic Versioning](https://semver.org/):

- **Patch** (`v0.2.0` → `v0.2.1`) — bug fixes only, no public-API change.
- **Minor** (`v0.2.0` → `v0.3.0`) — additive features, no breaking change.
- **Major** (`v0.2.0` → `v1.0.0`) — breaking changes to MCP tool
  signatures, settings, or environment variables. Until `v1.0.0` the
  minor line is also where breaking changes go, with a CHANGELOG note.

## Commit conventions

Lightweight — not enforced by a linter, but kept consistent for
`release-please` / auto-generated release notes to read well:

- `feat:` new MCP tool or user-visible capability
- `fix:` bug fix
- `docs:` README / docstring / comment-only changes
- `refactor:` internal restructuring, no behaviour change
- `test:` test-only changes
- `ci:` workflow / Dependabot / branch-protection changes
- `chore:` dependency bumps, build, repo hygiene

## Local checklist before pushing a tag

```bash
uv sync --all-extras --dev
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run mypy src/airflow_mcp
uv run pytest
```

If all five are clean, tag and push.
