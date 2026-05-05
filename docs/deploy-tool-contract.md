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
  "source_repo_path": "/Users/nordini/.openclaw/workspace-cinder/agent-hq",
  "agent_id": "cinder-backend",
  "agent_name": "Cinder",
  "branch": "cinder/task-426",
  "commit": "abc123",
  "services": "both",
  "health_check": true
}
```

Equivalent CLI:

```sh
python3 -B -m dev_env_lease_manager.cli --config /Users/nordini/dev-environment-lease-manager/config/environments.json deploy agent-hq-dev --task-id 426 --actor cinder-backend --source-repo-path /Users/nordini/.openclaw/workspace-cinder/agent-hq --branch cinder/task-426 --commit abc123
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
