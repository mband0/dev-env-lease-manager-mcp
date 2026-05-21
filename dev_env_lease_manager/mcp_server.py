from __future__ import annotations

import argparse
import traceback
from typing import Any, Callable, Dict, Optional

from mcp.server.fastmcp import FastMCP

from .config import default_config_path, load_config
from .manager import LeaseManager

DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS = 170


class LeaseManagerMcpTools:
    def __init__(self, manager: LeaseManager):
        self.manager = manager

    def _preflight_cleanup_has_reportable_changes(self, cleanup: Dict[str, Any]) -> bool:
        stale_leases = cleanup.get("stale_leases") or {}
        deploy_locks = cleanup.get("deploy_locks") or {}
        return (
            not bool(cleanup.get("ok", False))
            or bool(stale_leases.get("marked_stale"))
            or bool(stale_leases.get("released"))
            or bool(stale_leases.get("skipped"))
            or bool(deploy_locks.get("removed"))
            or any(bool(entry.get("stale")) for entry in deploy_locks.get("skipped", []))
        )

    def call_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        handlers: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
            "dev_env_health": lambda a: {
                "ok": True,
                "environment_count": len(self.manager.config.environments),
                "data_path": self.manager.config.data_path,
            },
            "dev_env_status": lambda a: self.manager.status(a.get("environment_id"), bool(a.get("include_events", False))),
            "dev_env_queue_status": lambda a: self.manager.queue_status(
                a.get("environment_id"),
                bool(a.get("include_terminal", False)),
            ),
            "dev_env_acquire": lambda a: self.manager.acquire(
                a["environment_id"],
                a["task_id"],
                a["actor"],
                a.get("agent_id"),
                a.get("agent_name"),
                a.get("branch"),
                a.get("commit"),
            ),
            "dev_env_mark_deploying": lambda a: self.manager.transition(a["lease_id"], "mark_deploying", a["actor"]),
            "dev_env_mark_deployed_for_qa": lambda a: self.manager.transition(
                a["lease_id"],
                "mark_deployed_for_qa",
                a["actor"],
                {"served_commit": a.get("served_commit")},
            ),
            "dev_env_mark_prod_deploying": lambda a: self.manager.transition(a["lease_id"], "mark_prod_deploying", a["actor"]),
            "dev_env_release": lambda a: self.manager.release(a["lease_id"], a["actor"], a["reason"], a.get("message")),
            "dev_env_mark_deploy_failed": lambda a: self.manager.release(
                a["lease_id"],
                a["actor"],
                "deploy_failed",
                a.get("message"),
            ),
            "dev_env_mark_qa_failed": lambda a: self.manager.release(a["lease_id"], a["actor"], "qa_failed", a.get("message")),
            "dev_env_mark_prod_failed": lambda a: self.manager.release(a["lease_id"], a["actor"], "prod_failed", a.get("message")),
            "dev_env_mark_done": lambda a: self.manager.release(a["lease_id"], a["actor"], "done", a.get("message")),
            "dev_env_force_release": lambda a: self.manager.force_release(
                a["actor"],
                a["reason"],
                a.get("lease_id"),
                a.get("environment_id"),
            ),
            "dev_env_heartbeat": lambda a: self.manager.heartbeat(a["lease_id"], a["actor"]),
            "dev_env_sweep_stale": lambda a: self.manager.sweep_stale(a["actor"]),
            "dev_env_sweep_deploy_queue": lambda a: self.manager.sweep_deploy_queue(
                a["actor"],
                a.get("environment_id"),
                int(a.get("limit") or 1),
                bool(a.get("dry_run", False)),
                int(a.get("timeout_seconds") or DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS),
            ),
            "dev_env_sweep_deploy_locks": lambda a: self.manager.sweep_deploy_locks(
                a["actor"],
                a["reason"],
                a.get("environment_id"),
                bool(a.get("force", False)),
            ),
            "dev_env_cancel_queue": lambda a: self.manager.cancel_queue_request(
                a["queue_id"],
                a["actor"],
                a.get("message"),
            ),
            "dev_env_validate_qa": lambda a: self.manager.validate_qa(
                a["task_id"],
                a["commit"],
                a.get("environment_id"),
                a.get("lease_id"),
            ),
            "dev_env_events": lambda a: self.manager.events(a["lease_id"]),
            "dev_env_callback_attempts": lambda a: self.manager.callback_attempts(
                a.get("queue_id"),
                a.get("lease_id"),
                a.get("task_id"),
                a.get("environment_id"),
                int(a.get("limit") or 50),
            ),
            "dev_env_deploy_worktree": lambda a: self.manager.lease_aware_deploy(
                a["environment_id"],
                a["task_id"],
                a["actor"],
                a["source_repo_path"],
                a.get("agent_id"),
                a.get("agent_name"),
                a.get("branch"),
                a.get("commit"),
                a.get("services", "both"),
                bool(a.get("health_check", True)),
                bool(a.get("dry_run", False)),
                queue_if_busy=bool(a.get("queue_if_busy", False)),
                priority=int(a.get("priority") or 0),
                callback_url=a.get("callback_url"),
                callback_api_key=a.get("callback_api_key"),
                timeout_seconds=int(a.get("timeout_seconds") or DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS),
                database_policy=a.get("database_policy", "preflight_and_apply"),
            ),
        }
        if name not in handlers:
            return {"ok": False, "error": "unknown_tool", "tool": name}
        try:
            preflight_cleanup = self.manager.mcp_preflight_cleanup()
            result = handlers[name](args or {})
            if self._preflight_cleanup_has_reportable_changes(preflight_cleanup):
                result["preflight_cleanup"] = preflight_cleanup
            return result
        except KeyError as exc:
            return {"ok": False, "error": "missing_required_argument", "argument": str(exc).strip("'")}
        except Exception as exc:
            formatted_traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            return {
                "ok": False,
                "error": "tool_call_failed",
                "tool": name,
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "traceback": formatted_traceback[-4000:],
            }


def create_mcp_server(manager: LeaseManager) -> FastMCP:
    service = LeaseManagerMcpTools(manager)
    mcp = FastMCP("dev-environment-lease-manager")

    @mcp.tool()
    def dev_env_health() -> Dict[str, Any]:
        """Return lease manager health."""
        return service.call_tool("dev_env_health", {})

    @mcp.tool()
    def dev_env_status(environment_id: Optional[str] = None, include_events: bool = False) -> Dict[str, Any]:
        """List environments and active leases."""
        return service.call_tool("dev_env_status", {
            "environment_id": environment_id,
            "include_events": include_events,
        })

    @mcp.tool()
    def dev_env_queue_status(
        environment_id: Optional[str] = None,
        include_terminal: bool = False,
    ) -> Dict[str, Any]:
        """List queued deploy requests and active queue positions."""
        return service.call_tool("dev_env_queue_status", {
            "environment_id": environment_id,
            "include_terminal": include_terminal,
        })

    @mcp.tool()
    def dev_env_acquire(
        environment_id: str,
        task_id: str,
        actor: str,
        agent_id: Optional[str] = None,
        agent_name: Optional[str] = None,
        branch: Optional[str] = None,
        commit: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Acquire a dev environment lease."""
        return service.call_tool("dev_env_acquire", {
            "environment_id": environment_id,
            "task_id": task_id,
            "actor": actor,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "branch": branch,
            "commit": commit,
        })

    @mcp.tool()
    def dev_env_mark_deploying(lease_id: str, actor: str) -> Dict[str, Any]:
        """Mark a lease as deploying."""
        return service.call_tool("dev_env_mark_deploying", {"lease_id": lease_id, "actor": actor})

    @mcp.tool()
    def dev_env_mark_deployed_for_qa(
        lease_id: str,
        actor: str,
        served_commit: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Mark a lease as deployed for QA."""
        return service.call_tool("dev_env_mark_deployed_for_qa", {
            "lease_id": lease_id,
            "actor": actor,
            "served_commit": served_commit,
        })

    @mcp.tool()
    def dev_env_mark_prod_deploying(lease_id: str, actor: str) -> Dict[str, Any]:
        """Mark a lease as production deploying."""
        return service.call_tool("dev_env_mark_prod_deploying", {"lease_id": lease_id, "actor": actor})

    @mcp.tool()
    def dev_env_release(
        lease_id: str,
        actor: str,
        reason: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Release a lease with an allowed release reason."""
        return service.call_tool("dev_env_release", {
            "lease_id": lease_id,
            "actor": actor,
            "reason": reason,
            "message": message,
        })

    @mcp.tool()
    def dev_env_mark_deploy_failed(
        lease_id: str,
        actor: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Release a lease after dev deployment fails."""
        return service.call_tool("dev_env_mark_deploy_failed", {
            "lease_id": lease_id,
            "actor": actor,
            "message": message,
        })

    @mcp.tool()
    def dev_env_mark_qa_failed(
        lease_id: str,
        actor: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Release a lease after QA fails."""
        return service.call_tool("dev_env_mark_qa_failed", {
            "lease_id": lease_id,
            "actor": actor,
            "message": message,
        })

    @mcp.tool()
    def dev_env_mark_prod_failed(
        lease_id: str,
        actor: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Release a lease after production deployment fails."""
        return service.call_tool("dev_env_mark_prod_failed", {
            "lease_id": lease_id,
            "actor": actor,
            "message": message,
        })

    @mcp.tool()
    def dev_env_mark_done(
        lease_id: str,
        actor: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Release a lease after production succeeds and task is done."""
        return service.call_tool("dev_env_mark_done", {
            "lease_id": lease_id,
            "actor": actor,
            "message": message,
        })

    @mcp.tool()
    def dev_env_force_release(
        actor: str,
        reason: str,
        lease_id: Optional[str] = None,
        environment_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Force release an active lease with explicit actor and reason."""
        return service.call_tool("dev_env_force_release", {
            "lease_id": lease_id,
            "environment_id": environment_id,
            "actor": actor,
            "reason": reason,
        })

    @mcp.tool()
    def dev_env_heartbeat(lease_id: str, actor: str) -> Dict[str, Any]:
        """Refresh active lease heartbeat."""
        return service.call_tool("dev_env_heartbeat", {"lease_id": lease_id, "actor": actor})

    @mcp.tool()
    def dev_env_sweep_stale(actor: str) -> Dict[str, Any]:
        """Mark active leases stale when their heartbeat is older than policy."""
        return service.call_tool("dev_env_sweep_stale", {"actor": actor})

    @mcp.tool()
    def dev_env_sweep_deploy_queue(
        actor: str,
        environment_id: Optional[str] = None,
        limit: int = 1,
        dry_run: bool = False,
        timeout_seconds: int = DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        """Deploy queued requests for available environments."""
        return service.call_tool("dev_env_sweep_deploy_queue", {
            "actor": actor,
            "environment_id": environment_id,
            "limit": limit,
            "dry_run": dry_run,
            "timeout_seconds": timeout_seconds,
        })

    @mcp.tool()
    def dev_env_sweep_deploy_locks(
        actor: str,
        reason: str,
        environment_id: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Clear stale native deploy lock artifacts when no live deploy owns them."""
        return service.call_tool("dev_env_sweep_deploy_locks", {
            "actor": actor,
            "reason": reason,
            "environment_id": environment_id,
            "force": force,
        })

    @mcp.tool()
    def dev_env_cancel_queue(
        queue_id: str,
        actor: str,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Cancel a queued deploy request before it starts deploying."""
        return service.call_tool("dev_env_cancel_queue", {
            "queue_id": queue_id,
            "actor": actor,
            "message": message,
        })

    @mcp.tool()
    def dev_env_validate_qa(
        task_id: str,
        commit: str,
        lease_id: Optional[str] = None,
        environment_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate task and commit match the active lease before QA."""
        return service.call_tool("dev_env_validate_qa", {
            "task_id": task_id,
            "commit": commit,
            "lease_id": lease_id,
            "environment_id": environment_id,
        })

    @mcp.tool()
    def dev_env_events(lease_id: str) -> Dict[str, Any]:
        """Return event history for a lease."""
        return service.call_tool("dev_env_events", {"lease_id": lease_id})

    @mcp.tool()
    def dev_env_callback_attempts(
        queue_id: Optional[str] = None,
        lease_id: Optional[str] = None,
        task_id: Optional[str] = None,
        environment_id: Optional[str] = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Return Agent HQ callback attempt logs."""
        return service.call_tool("dev_env_callback_attempts", {
            "queue_id": queue_id,
            "lease_id": lease_id,
            "task_id": task_id,
            "environment_id": environment_id,
            "limit": limit,
        })

    @mcp.tool()
    def dev_env_deploy_worktree(
        environment_id: str,
        task_id: str,
        actor: str,
        source_repo_path: str,
        agent_id: Optional[str] = None,
        agent_name: Optional[str] = None,
        branch: Optional[str] = None,
        commit: Optional[str] = None,
        services: str = "both",
        health_check: bool = True,
        dry_run: bool = False,
        queue_if_busy: bool = False,
        priority: int = 0,
        callback_url: Optional[str] = None,
        callback_api_key: Optional[str] = None,
        timeout_seconds: int = DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS,
        database_policy: str = "preflight_and_apply",
    ) -> Dict[str, Any]:
        """Lease-aware deploy wrapper around the configured deploy command.

        The MCP default deploy timeout is slightly below OpenClaw's 3-minute
        tool boundary so agents receive a structured deploy error instead of a
        generic MCP request timeout.
        """
        return service.call_tool("dev_env_deploy_worktree", {
            "environment_id": environment_id,
            "task_id": task_id,
            "actor": actor,
            "source_repo_path": source_repo_path,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "branch": branch,
            "commit": commit,
            "services": services,
            "health_check": health_check,
            "dry_run": dry_run,
            "queue_if_busy": queue_if_busy,
            "priority": priority,
            "callback_url": callback_url,
            "callback_api_key": callback_api_key,
            "timeout_seconds": timeout_seconds,
            "database_policy": database_policy,
        })

    return mcp


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=default_config_path())
    args = parser.parse_args(argv)

    manager = LeaseManager(load_config(args.config))
    try:
        create_mcp_server(manager).run(transport="stdio")
    finally:
        manager.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
