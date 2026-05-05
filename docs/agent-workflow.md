# Lease-Aware Agent Workflow

Implementation agents:

1. Commit the intended task worktree.
2. Call `dev_env_deploy_worktree` or the lease-aware deploy wrapper.
3. If the environment is busy, post blocked/waiting with the lease id and owner details.
4. Do not manually pull, reset, copy, or mutate the shared dev checkout.
5. Post review evidence only after the lease reaches `deployed_for_qa`.

QA agents:

1. Read the task review evidence.
2. Call `dev_env_validate_qa` with task id and expected commit.
3. Refuse QA if task or commit does not match the active lease.
4. On QA failure, call `dev_env_mark_qa_failed`.
5. On QA pass, keep the lease active for release unless the workflow says QA is terminal.

Release agents:

1. Call `dev_env_mark_prod_deploying` before production deployment.
2. If production deployment fails, call `dev_env_mark_prod_failed`.
3. After production succeeds and the Agent HQ task reaches done, call `dev_env_mark_done`.

Operators:

1. Use `dev_env_status --events` or `dev_env_sweep_stale` to inspect stuck environments.
2. Use force release only with an explicit actor and reason.
3. Treat force release as an audit event, not as normal workflow.
