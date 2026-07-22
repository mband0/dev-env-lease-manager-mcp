# Lease-Aware Deploy Tool Contract

The normal Agent HQ dev promotion path is the lease-aware deploy capability. An
implementation agent must not pull, reset, copy files into, or otherwise mutate a
shared dev checkout directly.

## Capability Command

Use the MCP tool:

```json
{
  "environment_id": "agent-hq-dev",
  "task_id": "426",
  "actor": "cinder-backend",
  "source_repo_path": "/path/to/agent/worktree",
  "agent_id": "cinder-backend",
  "agent_name": "Cinder",
  "branch": "cinder/task-426",
  "commit": "abc123",
  "services": "both",
  "health_check": true,
  "queue_if_busy": true,
  "database_policy": "preflight_and_apply",
  "callback_url": "http://localhost:3501",
  "callback_api_key": "ahq_mcp_..."
}
```

Equivalent CLI:

```sh
.venv/bin/dev-env-lease --config config/environments.json deploy agent-hq-dev --task-id 426 --actor cinder-backend --source-repo-path /path/to/agent/worktree --branch cinder/task-426 --commit abc123 --queue-if-busy --database-policy preflight_and_apply --callback-url http://localhost:3501 --callback-api-key "$AGENT_HQ_MCP_API_KEY"
```

The deploy path acquires a lease, marks it `deploying`, promotes the exact clean
source `HEAD` into the configured persistent dev checkout, builds the configured
services, runs database migration checks according to `database_policy`, restarts
PM2 services, verifies the served commit, and marks `deployed_for_qa` only after
verification succeeds.

For Agent HQ dev, this deploy behavior is native to the MCP server through
`metadata.deploy_mode = "native"`. The older shell-command path remains
available only when an environment explicitly sets deploy mode to `command`.

`database_policy` controls migration behavior for native API deploys:

- `preflight_and_apply` runs migration status, preflights on a copied database, backs up the live database, applies migrations, and runs integrity checks.
- `preflight_only` runs the copied-database preflight and integrity check without mutating the live database.
- `status_only` reports migration status and database integrity only.
- `none` skips migration handling.

## Busy Or Missing Environment

If the tool returns `environment_busy`, the agent posts blocked/waiting with the
owner task, lease id, branch, and commit from the response. The agent must not
fall back to manual shared-checkout mutation.

If `queue_if_busy` is true, `environment_id` is treated as the preferred target
for that deployment pool. When the preferred environment is busy and another
configured environment has the same selector tags, the lease manager assigns the
deploy to the next available matching environment immediately. If all matching
environments are busy, the tool returns `status: queued` with a queue id,
position, `requested_environment_id`, and empty `assigned_environment_id`. This
is an expected workflow state, not a blocked task state. The queue worker
command/tool `sweep-deploy-queue` / `dev_env_sweep_deploy_queue` deploys the
exact recorded commit on the next available matching environment. New queued
requests for the same task supersede older queued requests so stale commits do
not deploy later.

Queued deployments require a full `commit` SHA. At enqueue time and again
immediately before claim/deploy, `source_repo_path` must be a readable git
worktree whose `git rev-parse HEAD` output exactly equals that SHA. A missing
path, non-git directory, or mismatch is rejected with an actionable structured
error. This prevents a recovery or project-manager workspace from enqueueing a
different agent's task commit.

Every terminal lease release path (success, deploy/QA/production failure,
manual/forced release, and stale cleanup) wakes the next queued deploy that can
run on the released environment. Wakeups are idempotent and reentrant wakeups
are deferred so a failed queued deploy cannot recursively double-claim.
Callers may still run `sweep-deploy-queue` manually for recovery or operator
maintenance, but the expected path is that releasing the active lease promotes
the next queued request automatically.

Queued callbacks use the explicit `callback_url` / `callback_api_key` when
provided. If omitted, the manager falls back to `agent_hq.base_url` for the
callback URL. The callback API key must come from the calling agent's
materialized MCP environment as `AGENT_HQ_MCP_API_KEY`, or from an explicit
`callback_api_key` argument. A shared lease-manager service key is not required.

When callback settings are supplied, the lease manager posts Agent HQ external
task events for `dev_deploy_queued`, `dev_deploying`, `deployed_for_qa`,
`deploy_failed`, `stale_lease_released`, `cancelled`, and `superseded`.
`stale_lease_released` is emitted when MCP preflight cleanup auto-releases a
heartbeat-expired lease, and includes `release_reason`, `prior_lease_status`,
and `prior_deploy_status` for deterministic Agent HQ recovery. Known deploy
failure classes are posted as specific events instead of generic `deploy_failed`: `database_backup_failed`,
`database_migration_failed`, `database_integrity_failed`, `api_boot_failed`,
`api_health_failed`, `ui_health_failed`, `process_restart_failed`, `checkout_failed`,
and `build_failed`.

Every callback attempt is recorded in the lease manager state database without
storing the API key. Operators can inspect delivery with `callback-attempts` or
the `dev_env_callback_attempts` MCP tool.

If no matching environment exists, the agent posts blocked/waiting with
`environment_not_found`. The agent must not invent a new target or deploy to
production.

## Evidence

Successful deploy output includes Agent HQ review evidence with:

- lease id
- environment id
- task id
- agent id and agent name
- branch and commit
- review URL

Post review evidence only after `deployed_for_qa`.

## QA And Release

QA must call `dev_env_validate_qa` with task id and expected commit before
testing. Commit or task mismatch is an environment integrity failure.

QA failure uses `dev_env_mark_qa_failed`. Release uses
`dev_env_deploy_production` with `dry_run = true` first, then `dry_run = false`
after the plan is acceptable. The production deploy tool owns the
`prod_deploying -> done/prod_failed` lease transition, exact production checkout
reset, build, database migration, PM2 restart, health check, and structured
release evidence. The older `dev_env_mark_prod_deploying`,
`dev_env_mark_prod_failed`, and `dev_env_mark_done` tools remain available for
manual recovery, but normal release agents should not stitch those steps
together by hand.
