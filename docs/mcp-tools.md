# MCP Tools

The server runs over MCP stdio:

```sh
.venv/bin/dev-env-lease-mcp --config config/environments.json
```

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
- `dev_env_cancel_queue`
- `dev_env_validate_qa`
- `dev_env_events`
- `dev_env_callback_attempts`
- `dev_env_deploy_worktree`

`dev_env_deploy_worktree` is the normal dev promotion tool. For environments
with `metadata.deploy_mode = "native"`, the MCP server itself performs the
persistent dev checkout promotion, build, PM2 restart, health check, served
commit verification, and lease transition. Environments can still opt into the
legacy external command path with `metadata.deploy_mode = "command"`.

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

Queued deploys can call Agent HQ's external task event endpoint. Provide
`callback_url` as either the Agent HQ base URL or
`/api/v1/external/task-events`, and `callback_api_key` as an Agent HQ MCP API
key from the calling agent's materialized MCP environment. Callback events are
`dev_deploy_queued`, `dev_deploying`, `deployed_for_qa`, `deploy_failed`,
`cancelled`, and `superseded`.

Each callback attempt is stored durably without the API key. Use
`dev_env_callback_attempts` with `queue_id`, `lease_id`, `task_id`, or
`environment_id` to inspect the attempted endpoint, auth presence, HTTP status,
response body, error, and payload.

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
