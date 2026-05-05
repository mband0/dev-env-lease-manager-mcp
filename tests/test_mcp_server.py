from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.manager import LeaseManager
from dev_env_lease_manager.mcp_server import McpServer


class McpServerTests(unittest.TestCase):
    def make_server(self) -> tuple[McpServer, LeaseManager, tempfile.TemporaryDirectory[str]]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config_path = Path(tmp.name) / "envs.json"
        config_path.write_text(json.dumps({
            "data_path": str(Path(tmp.name) / "state.sqlite3"),
            "environments": [{"id": "agent-hq-dev", "label": "Agent HQ Dev"}],
        }), encoding="utf-8")
        manager = LeaseManager(load_config(str(config_path)))
        self.addCleanup(manager.close)
        return McpServer(manager), manager, tmp

    def test_lists_tools(self) -> None:
        server, _, _ = self.make_server()
        response = server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})

        self.assertEqual(response["id"], 1)
        names = {tool["name"] for tool in response["result"]["tools"]}
        self.assertIn("dev_env_acquire", names)
        self.assertIn("dev_env_deploy_worktree", names)
        self.assertIn("dev_env_validate_qa", names)
        self.assertIn("dev_env_mark_qa_failed", names)
        self.assertIn("dev_env_mark_prod_failed", names)
        self.assertIn("dev_env_mark_done", names)

    def test_calls_tool_and_returns_structured_json_content(self) -> None:
        server, _, _ = self.make_server()
        response = server.handle({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "dev_env_acquire",
                "arguments": {
                    "environment_id": "agent-hq-dev",
                    "task_id": "426",
                    "actor": "cinder",
                    "commit": "abc123",
                },
            },
        })

        self.assertFalse(response["result"]["isError"])
        payload = json.loads(response["result"]["content"][0]["text"])
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["lease"]["task_id"], "426")

    def test_named_release_tools_call_state_machine_reasons(self) -> None:
        server, manager, _ = self.make_server()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        response = server.handle({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "dev_env_mark_qa_failed",
                "arguments": {
                    "lease_id": lease["id"],
                    "actor": "qa",
                    "message": "regression failed",
                },
            },
        })

        self.assertFalse(response["result"]["isError"])
        payload = json.loads(response["result"]["content"][0]["text"])
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "qa_failed")
        self.assertEqual(payload["release_reason"], "qa_failed")


if __name__ == "__main__":
    unittest.main()
