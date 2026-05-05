# Dev Environment Lease Manager

External MCP/capability service that owns shared dev environment leases.

This repo is intentionally separate from Agent HQ core. It protects shared dev
deployments from concurrent overwrites while Agent HQ remains the task and
evidence system.

The MCP server also owns the normal Agent HQ dev promotion path. Its
`dev_env_deploy_worktree` tool can natively promote a clean source checkout into
the persistent dev checkout, build/restart the configured PM2 services, verify
health and served commit, then keep the lease locked for QA.

## Quick Start

```sh
python3 -m unittest discover -s tests
python3 -m dev_env_lease_manager.cli --config config/environments.json health
python3 -m dev_env_lease_manager.mcp_server --config config/environments.json
```

## Operator CLI

```sh
python3 -m dev_env_lease_manager.cli status
python3 -m dev_env_lease_manager.cli acquire agent-hq-dev --task-id 426 --actor cinder --branch task-426 --commit abc123
python3 -m dev_env_lease_manager.cli mark-deploying --lease-id <lease>
python3 -m dev_env_lease_manager.cli mark-deployed-for-qa --lease-id <lease>
python3 -m dev_env_lease_manager.cli release --lease-id <lease> --reason qa_failed --actor quinn
python3 -m dev_env_lease_manager.cli force-release --environment-id agent-hq-dev --actor operator:nordini --reason stale
```

See [docs/lease-contract.md](docs/lease-contract.md) for the contract, state
machine, and MCP payloads. See
[docs/deploy-tool-contract.md](docs/deploy-tool-contract.md) for agent deploy
rules and [docs/agent-hq-registration.md](docs/agent-hq-registration.md) for
Agent HQ MCP registration.
