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
  "callback_url": "http://localhost:3501",
  "callback_api_key": "ahq_mcp_..."
}
```

Equivalent CLI:

```sh
.venv/bin/dev-env-lease --config config/environments.json deploy agent-hq-dev --task-id 426 --actor cinder-backend --source-repo-path /path/to/agent/worktree --branch cinder/task-426 --commit abc123 --queue-if-busy --callback-url http://localhost:3501 --callback-api-key "$AGENT_HQ_MCP_API_KEY"
```

The deploy path acquires a lease, marks it `deploying`, promotes the exact clean
source `HEAD` into the configured persistent dev checkout, builds and restarts
the configured services, verifies the served commit, and marks
`deployed_for_qa` only after verification succeeds.

For Agent HQ dev, this deploy behavior is native to the MCP server through
`metadata.deploy_mode = "native"`. The older shell-command path remains
available only when an environment explicitly sets deploy mode to `command`.

## Busy Or Missing Environment

If the tool returns `environment_busy`, the agent posts blocked/waiting with the
owner task, lease id, branch, and commit from the response. The agent must not
fall back to manual shared-checkout mutation.

If `queue_if_busy` is true and the environment is busy, the tool returns
`status: queued` with a queue id and position. This is an expected workflow
state, not a blocked task state. The queue worker command/tool
`sweep-deploy-queue` / `dev_env_sweep_deploy_queue` deploys the exact recorded
commit once the environment is available. New queued requests for the same task
supersede older queued requests so stale commits do not deploy later.

Normal lease release also sweeps the next queued deploy for that environment.
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
`deploy_failed`, `cancelled`, and `superseded`.

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

QA failure uses `dev_env_mark_qa_failed`. Release starts production work with
`dev_env_mark_prod_deploying`, uses `dev_env_mark_prod_failed` on production
failure, and uses `dev_env_mark_done` only after production succeeds and the
Agent HQ task is done.
