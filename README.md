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
python3.11 -m venv .venv
.venv/bin/python -m pip install -e .
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m dev_env_lease_manager.cli --config config/environments.json health
.venv/bin/dev-env-lease-mcp --config config/environments.json
```

## Operator CLI

```sh
.venv/bin/dev-env-lease status
.venv/bin/dev-env-lease acquire agent-hq-dev --task-id 426 --actor cinder --branch task-426 --commit abc123
.venv/bin/dev-env-lease mark-deploying --lease-id <lease>
.venv/bin/dev-env-lease mark-deployed-for-qa --lease-id <lease>
.venv/bin/dev-env-lease release --lease-id <lease> --reason qa_failed --actor quinn
.venv/bin/dev-env-lease force-release --environment-id agent-hq-dev --actor operator:admin --reason stale
```

See [docs/lease-contract.md](docs/lease-contract.md) for the contract, state
machine, and MCP payloads. See
[docs/deploy-tool-contract.md](docs/deploy-tool-contract.md) for agent deploy
rules and [docs/agent-hq-registration.md](docs/agent-hq-registration.md) for
Agent HQ MCP registration.
