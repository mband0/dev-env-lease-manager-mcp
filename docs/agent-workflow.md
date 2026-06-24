# Lease-Aware Agent Workflow

Implementation agents:

1. Commit the intended task worktree.
2. Call `dev_env_deploy_worktree` or the lease-aware deploy wrapper.
3. If the environment is busy, post blocked/waiting with the lease id and owner details.
4. Do not manually pull, reset, copy, or mutate the shared dev checkout.
5. Post review evidence only after the lease reaches `deployed_for_qa`.
6. If a prior review lease for the same task is still active after QA failure,
   redeploy through the lease-aware path. The previous same-task review lease is
   recorded as `superseded`; do not force-release it manually unless the
   lease-aware deploy reports a real blocker.

QA agents:

1. Read the task review evidence.
2. Call `dev_env_validate_qa` with task id and expected commit.
3. Refuse QA if task or commit does not match the active lease.
4. On QA failure, call `dev_env_mark_qa_failed`.
5. On QA pass, keep the lease active for release unless the workflow says QA is terminal.

Release agents:

1. Call `dev_env_deploy_production` with `dry_run = true` and inspect the plan.
2. Call `dev_env_deploy_production` with `dry_run = false` to run the exact-commit production deploy.
3. Use the returned `production_evidence` for release notes and branch cleanup.

Operators:

1. Use `dev_env_status --events` or `dev_env_sweep_stale` to inspect stuck environments.
2. Use force release only with an explicit actor and reason.
3. Treat force release as an audit event, not as normal workflow.
