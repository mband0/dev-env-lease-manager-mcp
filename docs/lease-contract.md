# Dev Environment Lease Manager Contract

The Dev Environment Lease Manager is the source of truth for shared development environment ownership.

Agent HQ remains the task, status, and evidence system. Agent HQ does not decide which task owns a shared dev checkout, and Agent HQ status must not be used as a lock. Agents and deployment tools must ask the lease manager before mutating a shared environment.

## Records

### Environment

An environment is a shared deployment target that can be owned by at most one active lease.

```json
{
  "id": "agent-hq-dev",
  "label": "Agent HQ Dev",
  "base_url": "http://127.0.0.1:3510",
  "health_url": "http://127.0.0.1:3511/health",
  "repo_path": "/Users/nordini/agent-hq-dev",
  "deploy_command": "/Users/nordini/agent-hq/scripts/capability-tools/deploy_dev_worktree.sh",
  "served_commit_command": "git -C /Users/nordini/agent-hq-dev rev-parse HEAD",
  "stale_after_seconds": 7200,
  "tags": ["agent-hq", "dev"],
  "metadata": {}
}
```

### Lease

A lease is the audited ownership record for one task's use of one environment.

```json
{
  "id": "uuid",
  "environment_id": "agent-hq-dev",
  "task_id": "426",
  "agent_id": "94",
  "agent_name": "Cinder",
  "branch": "cinder/task-426",
  "commit_sha": "abc123",
  "status": "deployed_for_qa",
  "acquired_at": "2026-05-05T18:00:00Z",
  "deploying_at": "2026-05-05T18:01:00Z",
  "deployed_at": "2026-05-05T18:05:00Z",
  "heartbeat_at": "2026-05-05T18:05:00Z",
  "released_at": null,
  "release_reason": null
}
```

### Lease Event

Every transition writes an immutable event.

```json
{
  "lease_id": "uuid",
  "environment_id": "agent-hq-dev",
  "task_id": "426",
  "actor": "cinder-backend",
  "event_type": "mark_deployed_for_qa",
  "from_status": "deploying",
  "to_status": "deployed_for_qa",
  "release_reason": null,
  "message": null,
  "created_at": "2026-05-05T18:05:00Z"
}
```

## State Machine

```text
free
  acquire
    -> acquired

acquired
  mark_deploying
    -> deploying
  release(cancelled)
    -> cancelled
  force_release
    -> force_released

deploying
  mark_deployed_for_qa
    -> deployed_for_qa
  release(deploy_failed)
    -> deploy_failed
  release(cancelled)
    -> cancelled
  force_release
    -> force_released

deployed_for_qa
  mark_prod_deploying
    -> prod_deploying
  release(qa_failed)
    -> qa_failed
  release(cancelled)
    -> cancelled
  force_release
    -> force_released

prod_deploying
  release(prod_failed)
    -> prod_failed
  release(done)
    -> done
  release(cancelled)
    -> cancelled
  force_release
    -> force_released

stale
  release(stale_released)
    -> stale_released
  force_release
    -> force_released
```

Terminal statuses are `released`, `deploy_failed`, `qa_failed`, `prod_failed`, `done`, `cancelled`, `stale_released`, and `force_released`.

## Release Reasons

Allowed normal release reasons:

- `deploy_failed`
- `qa_failed`
- `prod_failed`
- `done`
- `cancelled`
- `stale_released`
- `manual_release`

Admin force release requires:

- explicit `actor`
- explicit `reason`
- `lease_id` or `environment_id`

Force release records `event_type=force_release` and `status=force_released`.

## Busy Environment Response

When a second task attempts to acquire a locked environment, the lease manager returns a structured blocked response.

```json
{
  "ok": false,
  "status": "blocked",
  "error": "environment_busy",
  "environment": {
    "id": "agent-hq-dev",
    "label": "Agent HQ Dev",
    "base_url": "http://127.0.0.1:3510"
  },
  "lease": {
    "id": "uuid",
    "environment_id": "agent-hq-dev",
    "task_id": "426",
    "agent_id": "94",
    "agent_name": "Cinder",
    "branch": "cinder/task-426",
    "commit_sha": "abc123",
    "status": "deployed_for_qa"
  },
  "owner": {
    "task_id": "426",
    "agent_id": "94",
    "agent_name": "Cinder",
    "branch": "cinder/task-426",
    "commit": "abc123",
    "lease_id": "uuid",
    "status": "deployed_for_qa"
  },
  "next_action": "Do not deploy or mutate the shared dev checkout. Post blocked/waiting with this lease id, or ask an operator to force release if the lease is stale."
}
```

## MCP Tool Payloads

All tools return structured JSON with `ok`.

### `dev_env_acquire`

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

### `dev_env_mark_deploying`

```json
{
  "lease_id": "uuid",
  "actor": "deploy_dev_worktree"
}
```

### `dev_env_mark_deployed_for_qa`

```json
{
  "lease_id": "uuid",
  "actor": "deploy_dev_worktree",
  "served_commit": "abc123"
}
```

### `dev_env_release`

```json
{
  "lease_id": "uuid",
  "actor": "quinn-qa",
  "reason": "qa_failed",
  "message": "Regression failed on dev."
}
```

### `dev_env_force_release`

```json
{
  "environment_id": "agent-hq-dev",
  "actor": "operator:nordini",
  "reason": "stale owner agent terminated"
}
```

## Agent HQ Evidence Contract

The lease manager can produce evidence payloads ready for Agent HQ, but Agent HQ does not become the lock authority.

Review evidence from successful dev deploy:

```json
{
  "review_branch": "cinder/task-426",
  "review_commit": "abc123",
  "review_url": "http://127.0.0.1:3510",
  "summary": "Dev environment lease uuid owns agent-hq-dev for task 426.",
  "lease": {
    "lease_id": "uuid",
    "environment_id": "agent-hq-dev",
    "task_id": "426",
    "agent_id": "94",
    "agent_name": "Cinder",
    "branch": "cinder/task-426",
    "commit": "abc123",
    "status": "deployed_for_qa"
  }
}
```

Release note:

```text
Dev environment lease released
Lease: uuid
Environment: agent-hq-dev
Task: 426
Reason: qa_failed
Status: qa_failed
```

## Integrity Rules

- A shared dev checkout must not be mutated without an active lease.
- QA must verify the active lease task and commit match task evidence before testing.
- Commit mismatch is an environment integrity failure, not a product failure.
- A successful dev deploy keeps the environment locked for QA.
- Deploy failure, QA failure, production failure, cancellation, and done each release with an audited reason.
- Stale detection is visible to operators. The manager does not silently unlock active QA work unless explicit policy or force action is used.

