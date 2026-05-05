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

    acquire = sub.add_parser("acquire")
    acquire.add_argument("environment_id")
    acquire.add_argument("--task-id", required=True)
    acquire.add_argument("--actor", required=True)
    acquire.add_argument("--agent-id")
    acquire.add_argument("--agent-name")
    acquire.add_argument("--branch")
    acquire.add_argument("--commit")

    for name in ("mark-deploying", "mark-deployed-for-qa", "mark-prod-deploying", "heartbeat"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--lease-id", required=True)
        cmd.add_argument("--actor", required=True)

    release = sub.add_parser("release")
    release.add_argument("--lease-id", required=True)
    release.add_argument("--actor", required=True)
    release.add_argument("--reason", required=True)
    release.add_argument("--message")

    force = sub.add_parser("force-release")
    force.add_argument("--actor", required=True)
    force.add_argument("--reason", required=True)
    force.add_argument("--lease-id")
    force.add_argument("--environment-id")

    sweep = sub.add_parser("sweep-stale")
    sweep.add_argument("--actor", required=True)

    events = sub.add_parser("events")
    events.add_argument("--lease-id", required=True)

    qa = sub.add_parser("validate-qa")
    qa.add_argument("--task-id", required=True)
    qa.add_argument("--commit", required=True)
    qa.add_argument("--lease-id")
    qa.add_argument("--environment-id")

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
            "acquire": lambda: manager.acquire(
                args.environment_id, args.task_id, args.actor, args.agent_id,
                args.agent_name, args.branch, args.commit,
            ),
            "mark-deploying": lambda: manager.transition(args.lease_id, "mark_deploying", args.actor),
            "mark-deployed-for-qa": lambda: manager.transition(args.lease_id, "mark_deployed_for_qa", args.actor),
            "mark-prod-deploying": lambda: manager.transition(args.lease_id, "mark_prod_deploying", args.actor),
            "heartbeat": lambda: manager.heartbeat(args.lease_id, args.actor),
            "release": lambda: manager.release(args.lease_id, args.actor, args.reason, args.message),
            "force-release": lambda: manager.force_release(args.actor, args.reason, args.lease_id, args.environment_id),
            "sweep-stale": lambda: manager.sweep_stale(args.actor),
            "events": lambda: manager.events(args.lease_id),
            "validate-qa": lambda: manager.validate_qa(args.task_id, args.commit, args.environment_id, args.lease_id),
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
            ),
        }
        return print_json(command_map[args.command]())
    finally:
        manager.close()


if __name__ == "__main__":
    sys.exit(main())

