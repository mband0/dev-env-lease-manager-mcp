from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable

from .config import default_config_path, load_config
from .manager import LeaseManager


def print_json(payload: Any) -> int:
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if isinstance(payload, dict) and payload.get("ok", True) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dev-env-lease")
    parser.add_argument("--config", default=default_config_path(), help="Path to environments.json")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health")
    status = sub.add_parser("status")
    status.add_argument("--environment-id")
    status.add_argument("--events", action="store_true")

    queue_status = sub.add_parser("queue-status")
    queue_status.add_argument("--environment-id")
    queue_status.add_argument("--include-terminal", action="store_true")

    acquire = sub.add_parser("acquire")
    acquire.add_argument("environment_id")
    acquire.add_argument("--task-id", required=True)
    acquire.add_argument("--actor", required=True)
    acquire.add_argument("--agent-id")
    acquire.add_argument("--agent-name")
    acquire.add_argument("--branch")
    acquire.add_argument("--commit")

    for name in ("mark-deploying", "mark-prod-deploying", "heartbeat"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--lease-id", required=True)
        cmd.add_argument("--actor", required=True)

    deployed = sub.add_parser("mark-deployed-for-qa")
    deployed.add_argument("--lease-id", required=True)
    deployed.add_argument("--actor", required=True)
    deployed.add_argument("--served-commit")

    release = sub.add_parser("release")
    release.add_argument("--lease-id", required=True)
    release.add_argument("--actor", required=True)
    release.add_argument("--reason", required=True)
    release.add_argument("--message")

    for name in ("mark-deploy-failed", "mark-qa-failed", "mark-prod-failed", "mark-done"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--lease-id", required=True)
        cmd.add_argument("--actor", required=True)
        cmd.add_argument("--message")

    force = sub.add_parser("force-release")
    force.add_argument("--actor", required=True)
    force.add_argument("--reason", required=True)
    force.add_argument("--lease-id")
    force.add_argument("--environment-id")

    sweep = sub.add_parser("sweep-stale")
    sweep.add_argument("--actor", required=True)

    sweep_queue = sub.add_parser("sweep-deploy-queue")
    sweep_queue.add_argument("--actor", required=True)
    sweep_queue.add_argument("--environment-id")
    sweep_queue.add_argument("--limit", type=int, default=1)
    sweep_queue.add_argument("--dry-run", action="store_true")

    sweep_locks = sub.add_parser("sweep-deploy-locks")
    sweep_locks.add_argument("--actor", required=True)
    sweep_locks.add_argument("--reason", required=True)
    sweep_locks.add_argument("--environment-id")
    sweep_locks.add_argument("--force", action="store_true")

    cancel_queue = sub.add_parser("cancel-queue")
    cancel_queue.add_argument("--queue-id", required=True)
    cancel_queue.add_argument("--actor", required=True)
    cancel_queue.add_argument("--message")

    events = sub.add_parser("events")
    events.add_argument("--lease-id", required=True)

    callback_attempts = sub.add_parser("callback-attempts")
    callback_attempts.add_argument("--queue-id")
    callback_attempts.add_argument("--lease-id")
    callback_attempts.add_argument("--task-id")
    callback_attempts.add_argument("--environment-id")
    callback_attempts.add_argument("--limit", type=int, default=50)

    qa = sub.add_parser("validate-qa")
    qa.add_argument("--task-id", required=True)
    qa.add_argument("--commit", required=True)
    qa.add_argument("--lease-id")
    qa.add_argument("--environment-id")

    prod_deploy = sub.add_parser("deploy-production")
    prod_deploy.add_argument("environment_id")
    prod_deploy.add_argument("--lease-id", required=True)
    prod_deploy.add_argument("--task-id", required=True)
    prod_deploy.add_argument("--actor", required=True)
    prod_deploy.add_argument("--expected-commit", required=True)
    prod_deploy.add_argument("--services", default="both")
    prod_deploy.add_argument("--no-health-check", action="store_true")
    prod_deploy.add_argument("--dry-run", action="store_true", default=True)
    prod_deploy.add_argument("--execute", action="store_true")
    prod_deploy.add_argument("--timeout-seconds", type=int, default=1800)
    prod_deploy.add_argument("--database-policy", default="preflight_and_apply", choices=["none", "status_only", "preflight_only", "preflight_and_apply"])

    deploy = sub.add_parser("deploy")
    deploy.add_argument("environment_id")
    deploy.add_argument("--task-id", required=True)
    deploy.add_argument("--actor", required=True)
    deploy.add_argument("--source-repo-path", required=True)
    deploy.add_argument("--agent-id")
    deploy.add_argument("--agent-name")
    deploy.add_argument("--branch")
    deploy.add_argument("--commit")
    deploy.add_argument("--services", default="both")
    deploy.add_argument("--no-health-check", action="store_true")
    deploy.add_argument("--dry-run", action="store_true")
    deploy.add_argument("--queue-if-busy", action="store_true")
    deploy.add_argument("--priority", type=int, default=0)
    deploy.add_argument("--callback-url")
    deploy.add_argument("--callback-api-key")
    deploy.add_argument("--database-policy", default="preflight_and_apply", choices=["none", "status_only", "preflight_only", "preflight_and_apply"])

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)
    manager = LeaseManager(config)
    try:
        command_map: dict[str, Callable[[], dict[str, Any]]] = {
            "health": lambda: {"ok": True, "data_path": config.data_path, "environment_count": len(config.environments)},
            "status": lambda: manager.status(args.environment_id, args.events),
            "queue-status": lambda: manager.queue_status(args.environment_id, args.include_terminal),
            "acquire": lambda: manager.acquire(
                args.environment_id, args.task_id, args.actor, args.agent_id,
                args.agent_name, args.branch, args.commit,
            ),
            "mark-deploying": lambda: manager.transition(args.lease_id, "mark_deploying", args.actor),
            "mark-deployed-for-qa": lambda: manager.transition(
                args.lease_id,
                "mark_deployed_for_qa",
                args.actor,
                {"served_commit": args.served_commit},
            ),
            "mark-prod-deploying": lambda: manager.transition(args.lease_id, "mark_prod_deploying", args.actor),
            "heartbeat": lambda: manager.heartbeat(args.lease_id, args.actor),
            "release": lambda: manager.release(args.lease_id, args.actor, args.reason, args.message),
            "mark-deploy-failed": lambda: manager.release(args.lease_id, args.actor, "deploy_failed", args.message),
            "mark-qa-failed": lambda: manager.release(args.lease_id, args.actor, "qa_failed", args.message),
            "mark-prod-failed": lambda: manager.release(args.lease_id, args.actor, "prod_failed", args.message),
            "mark-done": lambda: manager.release(args.lease_id, args.actor, "done", args.message),
            "force-release": lambda: manager.force_release(args.actor, args.reason, args.lease_id, args.environment_id),
            "sweep-stale": lambda: manager.sweep_stale(args.actor),
            "sweep-deploy-queue": lambda: manager.sweep_deploy_queue(
                args.actor,
                args.environment_id,
                args.limit,
                args.dry_run,
            ),
            "sweep-deploy-locks": lambda: manager.sweep_deploy_locks(
                args.actor,
                args.reason,
                args.environment_id,
                args.force,
            ),
            "cancel-queue": lambda: manager.cancel_queue_request(args.queue_id, args.actor, args.message),
            "events": lambda: manager.events(args.lease_id),
            "callback-attempts": lambda: manager.callback_attempts(
                args.queue_id,
                args.lease_id,
                args.task_id,
                args.environment_id,
                args.limit,
            ),
            "validate-qa": lambda: manager.validate_qa(args.task_id, args.commit, args.environment_id, args.lease_id),
            "deploy-production": lambda: manager.deploy_production(
                args.environment_id,
                args.lease_id,
                args.task_id,
                args.actor,
                args.expected_commit,
                args.services,
                not args.no_health_check,
                False if args.execute else args.dry_run,
                args.timeout_seconds,
                args.database_policy,
            ),
            "deploy": lambda: manager.lease_aware_deploy(
                args.environment_id,
                args.task_id,
                args.actor,
                args.source_repo_path,
                args.agent_id,
                args.agent_name,
                args.branch,
                args.commit,
                args.services,
                not args.no_health_check,
                args.dry_run,
                queue_if_busy=args.queue_if_busy,
                priority=args.priority,
                callback_url=args.callback_url,
                callback_api_key=args.callback_api_key,
                database_policy=args.database_policy,
            ),
        }
        return print_json(command_map[args.command]())
    finally:
        manager.close()


if __name__ == "__main__":
    sys.exit(main())
