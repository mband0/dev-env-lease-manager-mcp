from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.manager import LeaseManager
from dev_env_lease_manager.mcp_server import DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS, LeaseManagerMcpTools, create_mcp_server


class McpServerTests(unittest.TestCase):
    def make_service(self) -> tuple[LeaseManagerMcpTools, LeaseManager, tempfile.TemporaryDirectory[str]]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config_path = Path(tmp.name) / "envs.json"
        config_path.write_text(json.dumps({
            "data_path": str(Path(tmp.name) / "state.sqlite3"),
            "environments": [{"id": "agent-hq-dev", "label": "Agent HQ Dev"}],
        }), encoding="utf-8")
        manager = LeaseManager(load_config(str(config_path)))
        self.addCleanup(manager.close)
        return LeaseManagerMcpTools(manager), manager, tmp

    def test_creates_fastmcp_server(self) -> None:
        _, manager, _ = self.make_service()
        server = create_mcp_server(manager)

        self.assertEqual(server.name, "dev-environment-lease-manager")

    def test_calls_tool_and_returns_structured_json_content(self) -> None:
        service, _, _ = self.make_service()
        payload = service.call_tool("dev_env_acquire", {
            "environment_id": "agent-hq-dev",
            "task_id": "426",
            "actor": "cinder",
            "commit": "abc123",
        })

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["lease"]["task_id"], "426")

    def test_named_release_tools_call_state_machine_reasons(self) -> None:
        service, manager, _ = self.make_service()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        payload = service.call_tool("dev_env_mark_qa_failed", {
            "lease_id": lease["id"],
            "actor": "qa",
            "message": "regression failed",
        })

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "qa_failed")
        self.assertEqual(payload["release_reason"], "qa_failed")

    def test_queue_tools_expose_status_and_cancel(self) -> None:
        service, manager, tmp = self.make_service()
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.acquire("agent-hq-dev", "425", "anchor")
        queued = service.call_tool("dev_env_deploy_worktree", {
            "environment_id": "agent-hq-dev",
            "task_id": "426",
            "actor": "cinder",
            "source_repo_path": str(source),
            "queue_if_busy": True,
        })["queue"]

        status = service.call_tool("dev_env_queue_status", {"environment_id": "agent-hq-dev"})
        self.assertTrue(status["ok"])
        self.assertEqual(status["queue"][0]["id"], queued["id"])
        self.assertEqual(status["queue"][0]["position"], 1)
        self.assertEqual(status["queue"][0]["busy_owner"]["task_id"], "425")
        self.assertEqual(status["queue"][0]["queued_because_owner"]["task_id"], "425")

        cancelled = service.call_tool("dev_env_cancel_queue", {
            "queue_id": queued["id"],
            "actor": "operator",
            "message": "not needed",
        })
        self.assertTrue(cancelled["ok"])
        self.assertEqual(cancelled["status"], "cancelled")

    def test_deploy_tool_uses_mcp_safe_timeout_default(self) -> None:
        service, manager, tmp = self.make_service()
        source = Path(tmp.name) / "source"
        source.mkdir()

        with patch.object(manager, "lease_aware_deploy", return_value={"ok": True}) as deploy:
            payload = service.call_tool("dev_env_deploy_worktree", {
                "environment_id": "agent-hq-dev",
                "task_id": "426",
                "actor": "cinder",
                "source_repo_path": str(source),
            })

        self.assertTrue(payload["ok"])
        self.assertEqual(deploy.call_args.kwargs["timeout_seconds"], DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS)

    def test_tool_exception_returns_detailed_payload(self) -> None:
        service, manager, _ = self.make_service()

        with patch.object(manager, "status", side_effect=RuntimeError("database locked")):
            payload = service.call_tool("dev_env_status", {})

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "tool_call_failed")
        self.assertEqual(payload["tool"], "dev_env_status")
        self.assertEqual(payload["exception_type"], "RuntimeError")
        self.assertEqual(payload["message"], "database locked")
        self.assertIn("RuntimeError: database locked", payload["traceback"])


if __name__ == "__main__":
    unittest.main()
