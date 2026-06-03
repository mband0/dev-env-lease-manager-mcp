# Agent HQ MCP Registration

Register the lease manager as an external stdio MCP server in Agent HQ
Capabilities:

```json
{
  "name": "Dev Environment Lease Manager",
  "slug": "dev-environment-lease-manager",
  "description": "External lease authority for shared Agent HQ development environments.",
  "transport": "stdio",
  "command": ".venv/bin/dev-env-lease-mcp",
  "args": [
    "--config",
    "config/environments.json"
  ],
  "cwd": "<lease-manager-checkout>",
  "env": {},
  "enabled": true
}
```

Assign it to implementation, QA, release, and Atlas/operator agents that can
touch or inspect shared Agent HQ development environments.

Implementation agents need `dev_env_deploy_worktree`. QA agents need
`dev_env_validate_qa` and `dev_env_mark_qa_failed`. Release agents need
`dev_env_mark_prod_deploying`, `dev_env_mark_prod_failed`, and
`dev_env_mark_done`; after successful live verification they also need
`dev_env_cleanup_task_branch` for dry-run and real task branch cleanup evidence.
Operators need `dev_env_status`, `dev_env_sweep_stale`, and
`dev_env_force_release`.
