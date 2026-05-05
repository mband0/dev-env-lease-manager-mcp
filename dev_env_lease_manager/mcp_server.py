from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable, Dict

from .config import default_config_path, load_config
from .manager import LeaseManager


def tool_schema(description: str, properties: Dict[str, Any], required: list[str] | None = None) -> Dict[str, Any]:
    return {
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required or [],
            "additionalProperties": False,
        },
    }


TOOLS: Dict[str, Dict[str, Any]] = {
    "dev_env_health": tool_schema("Return lease manager health.", {}, []),
    "dev_env_status": tool_schema("List environments and active leases.", {
        "environment_id": {"type": "string"},
        "include_events": {"type": "boolean"},
    }),
    "dev_env_acquire": tool_schema("Acquire a dev environment lease.", {
        "environment_id": {"type": "string"},
        "task_id": {"type": "string"},
        "actor": {"type": "string"},
        "agent_id": {"type": "string"},
        "agent_name": {"type": "string"},
        "branch": {"type": "string"},
        "commit": {"type": "string"},
    }, ["environment_id", "task_id", "actor"]),
    "dev_env_mark_deploying": tool_schema("Mark a lease as deploying.", {
        "lease_id": {"type": "string"},
        "actor": {"type": "string"},
    }, ["lease_id", "actor"]),
    "dev_env_mark_deployed_for_qa": tool_schema("Mark a lease as deployed for QA.", {
        "lease_id": {"type": "string"},
        "actor": {"type": "string"},
        "served_commit": {"type": "string"},
    }, ["lease_id", "actor"]),
    "dev_env_mark_prod_deploying": tool_schema("Mark a lease as production deploying.", {
        "lease_id": {"type": "string"},
        "actor": {"type": "string"},
    }, ["lease_id", "actor"]),
    "dev_env_release": tool_schema("Release a lease with an allowed release reason.", {
        "lease_id": {"type": "string"},
        "actor": {"type": "string"},
        "reason": {"type": "string"},
        "message": {"type": "string"},
    }, ["lease_id", "actor", "reason"]),
    "dev_env_force_release": tool_schema("Force release an active lease with explicit actor and reason.", {
        "lease_id": {"type": "string"},
        "environment_id": {"type": "string"},
        "actor": {"type": "string"},
        "reason": {"type": "string"},
    }, ["actor", "reason"]),
    "dev_env_heartbeat": tool_schema("Refresh active lease heartbeat.", {
        "lease_id": {"type": "string"},
        "actor": {"type": "string"},
    }, ["lease_id", "actor"]),
    "dev_env_sweep_stale": tool_schema("Mark active leases stale when their heartbeat is older than policy.", {
        "actor": {"type": "string"},
    }, ["actor"]),
    "dev_env_validate_qa": tool_schema("Validate task and commit match the active lease before QA.", {
        "task_id": {"type": "string"},
        "commit": {"type": "string"},
        "lease_id": {"type": "string"},
        "environment_id": {"type": "string"},
    }, ["task_id", "commit"]),
    "dev_env_events": tool_schema("Return event history for a lease.", {
        "lease_id": {"type": "string"},
    }, ["lease_id"]),
    "dev_env_deploy_worktree": tool_schema("Lease-aware deploy wrapper around the configured deploy command.", {
        "environment_id": {"type": "string"},
        "task_id": {"type": "string"},
        "actor": {"type": "string"},
        "source_repo_path": {"type": "string"},
        "agent_id": {"type": "string"},
        "agent_name": {"type": "string"},
        "branch": {"type": "string"},
        "commit": {"type": "string"},
        "services": {"type": "string"},
        "health_check": {"type": "boolean"},
        "dry_run": {"type": "boolean"},
    }, ["environment_id", "task_id", "actor", "source_repo_path"]),
}


class McpServer:
    def __init__(self, manager: LeaseManager):
        self.manager = manager

    def call_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        handlers: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
            "dev_env_health": lambda a: {"ok": True, "environment_count": len(self.manager.config.environments), "data_path": self.manager.config.data_path},
            "dev_env_status": lambda a: self.manager.status(a.get("environment_id"), bool(a.get("include_events", False))),
            "dev_env_acquire": lambda a: self.manager.acquire(a["environment_id"], a["task_id"], a["actor"], a.get("agent_id"), a.get("agent_name"), a.get("branch"), a.get("commit")),
            "dev_env_mark_deploying": lambda a: self.manager.transition(a["lease_id"], "mark_deploying", a["actor"]),
            "dev_env_mark_deployed_for_qa": lambda a: self.manager.transition(a["lease_id"], "mark_deployed_for_qa", a["actor"], {"served_commit": a.get("served_commit")}),
            "dev_env_mark_prod_deploying": lambda a: self.manager.transition(a["lease_id"], "mark_prod_deploying", a["actor"]),
            "dev_env_release": lambda a: self.manager.release(a["lease_id"], a["actor"], a["reason"], a.get("message")),
            "dev_env_force_release": lambda a: self.manager.force_release(a["actor"], a["reason"], a.get("lease_id"), a.get("environment_id")),
            "dev_env_heartbeat": lambda a: self.manager.heartbeat(a["lease_id"], a["actor"]),
            "dev_env_sweep_stale": lambda a: self.manager.sweep_stale(a["actor"]),
            "dev_env_validate_qa": lambda a: self.manager.validate_qa(a["task_id"], a["commit"], a.get("environment_id"), a.get("lease_id")),
            "dev_env_events": lambda a: self.manager.events(a["lease_id"]),
            "dev_env_deploy_worktree": lambda a: self.manager.lease_aware_deploy(
                a["environment_id"], a["task_id"], a["actor"], a["source_repo_path"],
                a.get("agent_id"), a.get("agent_name"), a.get("branch"), a.get("commit"),
                a.get("services", "both"), bool(a.get("health_check", True)), bool(a.get("dry_run", False)),
            ),
        }
        if name not in handlers:
            return {"ok": False, "error": "unknown_tool", "tool": name}
        try:
            return handlers[name](args or {})
        except KeyError as exc:
            return {"ok": False, "error": "missing_required_argument", "argument": str(exc).strip("'")}

    def handle(self, request: Dict[str, Any]) -> Dict[str, Any] | None:
        method = request.get("method")
        request_id = request.get("id")
        if method == "notifications/initialized":
            return None
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "dev-environment-lease-manager", "version": "0.1.0"},
                },
            }
        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {"name": name, **schema}
                        for name, schema in sorted(TOOLS.items())
                    ]
                },
            }
        if method == "tools/call":
            params = request.get("params") or {}
            result = self.call_tool(params.get("name"), params.get("arguments") or {})
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, sort_keys=True)}],
                    "isError": not bool(result.get("ok")),
                },
            }
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"Unknown method: {method}"},
        }


def read_message() -> Dict[str, Any] | None:
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        name, value = line.decode("ascii").split(":", 1)
        headers[name.lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    if length <= 0:
        return None
    return json.loads(sys.stdin.buffer.read(length).decode("utf-8"))


def write_message(message: Dict[str, Any]) -> None:
    payload = json.dumps(message, separators=(",", ":")).encode("utf-8")
    sys.stdout.buffer.write(f"Content-Length: {len(payload)}\r\n\r\n".encode("ascii") + payload)
    sys.stdout.buffer.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=default_config_path())
    args = parser.parse_args(argv)
    manager = LeaseManager(load_config(args.config))
    server = McpServer(manager)
    try:
        while True:
            request = read_message()
            if request is None:
                return 0
            response = server.handle(request)
            if response is not None:
                write_message(response)
    finally:
        manager.close()


if __name__ == "__main__":
    raise SystemExit(main())

