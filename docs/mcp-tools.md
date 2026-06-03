# MCP Tools

The server runs over MCP stdio:

```sh
.venv/bin/dev-env-lease-mcp --config config/environments.json
```

Every MCP tool call first runs a preflight cleanup. The cleanup releases
heartbeat-expired active leases as `stale_released` and removes stale native
deploy lock artifacts when no live deploy process owns the lock. If cleanup
changes anything, the tool response includes `preflight_cleanup` with the
released leases and removed lock entries.

## Tools

- `dev_env_health`
- `dev_env_status`
- `dev_env_queue_status`
- `dev_env_acquire`
- `dev_env_mark_deploying`
- `dev_env_mark_deployed_for_qa`
- `dev_env_mark_prod_deploying`
- `dev_env_release`
- `dev_env_mark_deploy_failed`
- `dev_env_mark_qa_failed`
- `dev_env_mark_prod_failed`
- `dev_env_mark_done`
- `dev_env_force_release`
- `dev_env_heartbeat`
- `dev_env_sweep_stale`
- `dev_env_sweep_deploy_queue`
- `dev_env_reconcile_stuck_deploys`
- `dev_env_sweep_deploy_locks`
- `dev_env_cancel_queue`
- `dev_env_validate_qa`
- `dev_env_events`
- `dev_env_callback_attempts`
- `dev_env_cleanup_task_branch`
- `dev_env_deploy_worktree`

`dev_env_deploy_worktree` is the normal dev promotion tool. By default it is
**asynchronous**: it authorizes the request, enqueues a durable deploy, and
returns `status: "accepted"` with a `queue_id` immediately, so the deploy cannot
be killed mid-flight by the MCP/OpenClaw tool-call boundary. The actual build,
migration, restart, health check, and lease transition run in the background
`dev-env-deploy-worker` process (see below); the outcome arrives via the Agent HQ
`deployed_for_qa` / `deploy_failed` callback. Poll `dev_env_queue_status` for
progress, or pass `wait_seconds` to block the call (bounded, max 600s) for the
terminal result. Pass `synchronous: true` (or `dry_run: true`) to run the deploy
inline in the calling process instead.

For environments with `metadata.deploy_mode = "native"`, the worker performs the
persistent dev checkout promotion, build, database migration preflight/apply,
PM2 restart, health check, served commit verification, and lease transition.
Environments can still opt into the legacy external command path with
`metadata.deploy_mode = "command"`.

### Background deploy worker and reconciliation

Native deploys take minutes — longer than the tool-call boundary — so execution
is decoupled from the caller. Run the worker under pm2 alongside the dev API/UI:

```
pm2 start dev-env-deploy-worker --name agent-hq-dev-deploy-worker -- \
  --config <path-to>/config/environments.json
```

The worker must have `AGENT_HQ_MCP_API_KEY` (and, if not in the config,
`agent_hq.base_url`) in its environment so it can send callbacks. It claims one
queued request at a time (atomically, guarded by the per-environment active-lease
index), heartbeats progress per phase into `deploy_queue.heartbeat_at`/`phase`,
and writes the terminal status + callback itself.

Every worker tick — and `dev_env_reconcile_stuck_deploys`, and the preflight that
runs before every MCP tool call — reconciles **orphaned** deploys: a row stuck in
`deploying` whose worker process is gone (dead `worker_pid`, or a stale heartbeat
past `deploy_orphan_after_seconds` with no held `deploy.lock`) is failed in place,
its lease released as `deploy_failed`, and a `deploy_failed` callback is sent so
Agent HQ leaves `dev_deploying`. `deploy_orphan_after_seconds` (default 600) and
`deploy_worker_poll_interval_seconds` (default 5) are configurable in
`environments.json`.

If the worker is down, `dev_env_sweep_deploy_queue` still drains the queue
synchronously as a degraded-mode fallback.

`dev_env_deploy_worktree` accepts `database_policy`, defaulting to
`preflight_and_apply`. Native API deploys use it to run migration status checks,
a copied-database preflight, live-database backup, migration apply, and SQLite
integrity checks before restarting the API. Use `preflight_only`, `status_only`,
or `none` for narrower maintenance flows.

Busy environment responses include owner task, agent, branch, commit, lease id,
environment, and next action guidance. Agents must treat `environment_busy` as a
blocked/waiting state and must not mutate the shared checkout manually.

When `dev_env_deploy_worktree` is called with `queue_if_busy = true`,
`environment_id` is the preferred target for that deployment pool. If the
preferred environment is busy, the manager first tries to deploy on another idle
environment with the same selector tags. If no matching environment is idle, it
creates a durable deploy queue entry instead of returning blocked semantics. The
queued response includes `queue.id`, `queue.position`,
`requested_environment_id`, `assigned_environment_id`, branch, commit, and
source repo path. `dev_env_queue_status` reports queued/deploying requests and
positions, derives `busy_owner` from the current active lease for each
environment, and preserves the original queue-time blocker as
`queued_because_owner` for audit/debugging. `dev_env_cancel_queue` cancels a
request that has not started, and `dev_env_sweep_deploy_queue` deploys the next
queued request for an available matching environment.

Native deploys use an OS-level `deploy.lock` file under the environment
`metadata.state_dir`. The lock records pid, acquired time, environment, lease,
task, actor, branch, and commit, and the OS releases the lock if the deploy
process dies. `dev_env_status` reports `native_deploy_lock` when a deploy lock
or legacy lock directory exists; environments blocked by a live native deploy
report `blocked_by: native_deploy_lock`. Use `dev_env_sweep_deploy_locks` with
an explicit actor and reason to remove stale artifacts only when no live deploy
owns the lock, or with `force` for non-held legacy/unlocked artifacts.

Queued deploys can call Agent HQ's external task event endpoint. Provide
`callback_url` as either the Agent HQ base URL or
`/api/v1/external/task-events`, and `callback_api_key` as an Agent HQ MCP API
key from the calling agent's materialized MCP environment. Callback events are
`dev_deploy_queued`, `dev_deploying`, `deployed_for_qa`, `deploy_failed`,
`cancelled`, and `superseded`. Known deploy failure classes are emitted as
specific callback events when possible: `database_backup_failed`,
`database_migration_failed`, `database_integrity_failed`, `api_boot_failed`,
`api_health_failed`, `ui_health_failed`, `process_restart_failed`,
`checkout_failed`, and `build_failed`.

Each callback attempt is stored durably without the API key. Use
`dev_env_callback_attempts` with `queue_id`, `lease_id`, `task_id`, or
`environment_id` to inspect the attempted endpoint, auth presence, HTTP status,
response body, error, and payload.

## Branch Cleanup

`dev_env_cleanup_task_branch` safely removes released task branches after
production deploy/live verification has already proven the task commit is
retained in deployed history. It is a cleanup tool, not a deploy tool; cleanup
failure must be reported as cleanup failure and must not be treated as a failed
production deploy.

Required inputs:

```json
{
  "repo_path": "/path/to/repo",
  "source_branch": "task-679",
  "source_commit": "abc123...",
  "deployed_commit": "def456...",
  "actor": "release-agent",
  "remote": "origin",
  "dry_run": true
}
```

Safety checks:

- refuses protected branch refs such as `main`, `master`, `origin/main`, and
  `origin/master`
- requires source branch, source commit, deployed commit, actor, and repo path
- requires the source commit to be an ancestor of the deployed/main commit
- requires local and remote branch tips to match the source commit or be proven
  retained in deployed history
- refuses deletion while any active lease or git worktree is using the branch
- re-checks the remote branch tip immediately before `git push origin --delete`
- uses `git branch -d` for local deletion and never force-deletes by default

`dry_run` defaults to `true` and returns `checks`, `planned_actions`,
`local`, `remote_status`, and any `errors` without deleting anything. Real mode
uses `dry_run = false` and returns `performed_actions` with git command results.
Already-missing branches are idempotent success only after the source commit is
proven retained by the deployed commit.

## Example Acquire

```json
{
  "environment_id": "agent-hq-dev",
  "task_id": "426",
  "actor": "cinder-backend",
  "agent_id": "94",
  "agent_name": "Cinder",
  "branch": "cinder/task-426",
  "commit": "abc123"
}
```

## Example Release

The generic release tool accepts a `reason`. The named release tools use the
same payload minus `reason`, and bind the reason to the tool name.

```json
{
  "lease_id": "uuid",
  "actor": "quinn-qa",
  "reason": "qa_failed",
  "message": "QA regression failed."
}
```
