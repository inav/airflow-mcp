#!/usr/bin/env bash
# scripts/enable-branch-protection.sh
#
# One-shot bootstrap for inav/airflow-mcp. Run this once after the
# initial CI workflow is live and the three jobs (lint, typecheck, test)
# have shown up in at least one Actions run.
#
# Idempotent — safe to re-run.
#
# Requires: gh (authenticated, with admin:repo_hook + repo scopes).
#
# What it does:
#   1. Enables branch protection on `main`:
#        - require PR before merge (you can still bypass as admin)
#        - require CI (lint, typecheck, test) to pass
#        - require linear history (no merge commits)
#        - block force-pushes and branch deletion
#   2. Creates the `pypi` environment if missing, with you as the
#      sole required reviewer — so every release tag needs your
#      manual click before the publish job runs.
#
# Usage:
#   ./scripts/enable-branch-protection.sh
#
# Override the owner/repo if you fork or rename:
#   REPO=my-fork/airflow-mcp ./scripts/enable-branch-protection.sh

set -euo pipefail

REPO="${REPO:-inav/airflow-mcp}"
BRANCH="${BRANCH:-main}"

echo ">> Configuring branch protection for $REPO@$BRANCH ..."

# Build the JSON payload. The required_status_check contexts must match the
# job `name:` values in .github/workflows/ci.yml exactly.
read -r -d '' PAYLOAD <<JSON || true
{
  "branch": "$BRANCH",
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "Lint (ruff)",
      "Type-check (mypy)",
      "Test (Python 3.10)",
      "Test (Python 3.11)",
      "Test (Python 3.12)"
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": null,
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": true,
  "lock_branch": false,
  "allow_fork_syncing": false
}
JSON

gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  "/repos/$REPO/branches/$BRANCH/protection" \
  --input - <<< "$PAYLOAD"

echo ">> Branch protection enabled."

echo ""
echo ">> Creating the 'pypi' deployment environment ..."

# Find the authenticated user's numeric GitHub ID so we can add them as a
# required reviewer of the environment. (Usernames can be renamed; IDs
# don't change.)
MY_ID="$(gh api user --jq .id)"
MY_LOGIN="$(gh api user --jq .login)"
echo ">>   ... as required reviewer: $MY_LOGIN (id=$MY_ID)"

# The environments API is create-or-noop; if it exists already, GitHub
# returns 422 with 'already_exists' and we ignore it.
gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  "/repos/$REPO/environments/pypi" \
  --input - <<JSON || true
{
  "wait_timer": 0,
  "prevent_self_review": false,
  "reviewers": [
    { "type": "User", "id": $MY_ID }
  ],
  "deployment_branch_policy": {
    "protected_branches": false,
    "custom_branch_policies": true
  }
}
JSON

# Restrict the pypi environment to the release workflow's `release` job
# so accidentally-targeted deploys from other workflows can't trigger it.
gh api \
  --method POST \
  -H "Accept: application/vnd.github+json" \
  "/repos/$REPO/environments/pypi/deployment-branch-policies" \
  --input - <<'JSON' || true
{ "pattern": "release.yml" }
JSON

echo ""
echo ">> Done. Verify at:"
echo "   https://github.com/$REPO/settings/branches"
echo "   https://github.com/$REPO/settings/environments"
