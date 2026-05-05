# Dev Environment Lease Manager

External MCP/capability service that owns shared dev environment leases.

This repo is intentionally separate from Agent HQ core. It protects shared dev
deployments from concurrent overwrites while Agent HQ remains the task and
evidence system.

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

See [docs/lease-contract.md](docs/lease-contract.md) for the contract, state machine, and MCP payloads.

