# Agent HQ MCP Registration

Register the lease manager as an external stdio MCP server in Agent HQ
Capabilities:

```json
{
  "name": "Dev Environment Lease Manager",
  "slug": "dev-environment-lease-manager",
  "description": "External lease authority for shared Agent HQ development environments.",
  "transport": "stdio",
  "command": "python3",
  "args": [
    "-B",
    "-m",
    "dev_env_lease_manager.mcp_server",
    "--config",
    "/Users/nordini/dev-environment-lease-manager/config/environments.json"
  ],
  "cwd": "/Users/nordini/dev-environment-lease-manager",
  "env": {},
  "enabled": true
}
```

Assign it to implementation, QA, release, and Atlas/operator agents that can
touch or inspect shared Agent HQ development environments.

Implementation agents need `dev_env_deploy_worktree`. QA agents need
`dev_env_validate_qa` and `dev_env_mark_qa_failed`. Release agents need
`dev_env_mark_prod_deploying`, `dev_env_mark_prod_failed`, and
`dev_env_mark_done`. Operators need `dev_env_status`, `dev_env_sweep_stale`, and
`dev_env_force_release`.
