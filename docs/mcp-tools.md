# MCP Tools

The server runs over MCP stdio:

```sh
python3 -m dev_env_lease_manager.mcp_server --config config/environments.json
```

## Tools

- `dev_env_health`
- `dev_env_status`
- `dev_env_acquire`
- `dev_env_mark_deploying`
- `dev_env_mark_deployed_for_qa`
- `dev_env_mark_prod_deploying`
- `dev_env_release`
- `dev_env_force_release`
- `dev_env_heartbeat`
- `dev_env_sweep_stale`
- `dev_env_validate_qa`
- `dev_env_events`
- `dev_env_deploy_worktree`

Busy environment responses include owner task, agent, branch, commit, lease id,
environment, and next action guidance. Agents must treat `environment_busy` as a
blocked/waiting state and must not mutate the shared checkout manually.

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

```json
{
  "lease_id": "uuid",
  "actor": "quinn-qa",
  "reason": "qa_failed",
  "message": "QA regression failed."
}
```

