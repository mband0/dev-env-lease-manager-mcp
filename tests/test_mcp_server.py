from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
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

    def git(self, repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode != 0:
            self.fail(f"git {' '.join(args)} failed: {completed.stderr}")
        return completed

    def run_git(self, *args: str) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            ["git", *args],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode != 0:
            self.fail(f"git {' '.join(args)} failed: {completed.stderr}")
        return completed

    def commit_file(self, repo: Path, rel_path: str, content: str, message: str) -> str:
        path = repo / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        self.git(repo, "add", rel_path)
        self.git(repo, "commit", "-m", message)
        return self.git(repo, "rev-parse", "HEAD").stdout.strip()

    def make_cleanup_repo(self, tmp: tempfile.TemporaryDirectory[str]) -> tuple[Path, str, str, str]:
        root = Path(tmp.name)
        remote = root / "remote.git"
        repo = root / "repo"
        self.run_git("init", "--bare", str(remote))
        self.run_git("init", str(repo))
        self.git(repo, "config", "user.email", "test@example.com")
        self.git(repo, "config", "user.name", "Test User")
        self.git(repo, "checkout", "-b", "main")
        initial = self.commit_file(repo, "README.md", "initial\n", "initial")
        self.git(repo, "remote", "add", "origin", str(remote))
        self.git(repo, "push", "-u", "origin", "main")
        self.git(repo, "checkout", "-b", "task-679")
        source = self.commit_file(repo, "feature.txt", "feature\n", "task 679 feature")
        self.git(repo, "push", "origin", "task-679")
        self.git(repo, "checkout", "main")
        self.git(repo, "merge", "--ff-only", "task-679")
        deployed = self.git(repo, "rev-parse", "HEAD").stdout.strip()
        return repo, initial, source, deployed

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

    def test_every_tool_call_releases_stale_leases_first(self) -> None:
        service, manager, _ = self.make_service()
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]
        old = (datetime.now(timezone.utc) - timedelta(hours=3)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        manager.conn.execute("UPDATE leases SET heartbeat_at = ? WHERE id = ?", (old, lease["id"]))

        payload = service.call_tool("dev_env_status", {"environment_id": "agent-hq-dev"})

        self.assertTrue(payload["ok"])
        self.assertIn("preflight_cleanup", payload)
        self.assertEqual(payload["preflight_cleanup"]["stale_leases"]["released"][0]["id"], lease["id"])
        self.assertEqual(manager._lease(lease["id"])["status"], "stale_released")
        self.assertIsNone(payload["environments"][0]["active_lease"])
        self.assertTrue(payload["environments"][0]["available"])

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
        self.assertEqual(deploy.call_args.kwargs["database_policy"], "preflight_and_apply")

    def test_production_deploy_tool_uses_dry_run_default(self) -> None:
        service, manager, _ = self.make_service()

        with patch.object(manager, "deploy_production", return_value={"ok": True, "status": "dry_run"}) as deploy:
            payload = service.call_tool("dev_env_deploy_production", {
                "environment_id": "agent-hq-dev",
                "lease_id": "lease-1",
                "task_id": "426",
                "actor": "release",
                "expected_commit": "abc123",
            })

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(deploy.call_args.args[:5], ("agent-hq-dev", "lease-1", "426", "release", "abc123"))
        self.assertTrue(deploy.call_args.args[7])
        self.assertEqual(deploy.call_args.args[8], DEFAULT_MCP_DEPLOY_TIMEOUT_SECONDS)

    def test_failure_class_errors_become_specific_callback_events(self) -> None:
        service, manager, _ = self.make_service()
        migration_error = {"stage": "deploy", "result": {"deploy": {"failure_class": "database_migration_failed", "phase": "db:migrate:preflight"}}}

        self.assertEqual(manager._failure_event_name(migration_error), "database_migration_failed")
        self.assertEqual(manager._failure_phase_from_error(migration_error), "db:migrate:preflight")
        self.assertEqual(manager._failure_event_name({"stage": "deploy", "result": {"deploy": {"error": "api_health_failed"}}}), "api_health_failed")
        self.assertEqual(manager._failure_event_name({"stage": "deploy", "result": {"deploy": {"error": "unknown"}}}), "deploy_failed")

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

    def test_cleanup_task_branch_dry_run_and_real_cleanup(self) -> None:
        service, _, tmp = self.make_service()
        repo, _, source, deployed = self.make_cleanup_repo(tmp)

        dry_run = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "task-679",
            "source_commit": source,
            "deployed_commit": deployed,
            "actor": "release-agent",
            "dry_run": True,
        })

        self.assertTrue(dry_run["ok"], dry_run)
        self.assertEqual(dry_run["status"], "dry_run")
        self.assertEqual(
            {(action["target"], action["action"]) for action in dry_run["planned_actions"]},
            {("local", "delete_branch"), ("remote", "delete_branch")},
        )
        self.assertTrue(dry_run["local"]["exists"])
        self.assertTrue(dry_run["remote_status"]["exists"])
        self.git(repo, "show-ref", "--verify", "refs/heads/task-679")
        self.assertIn(source, self.git(repo, "ls-remote", "--heads", "origin", "task-679").stdout)

        cleaned = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "task-679",
            "source_commit": source,
            "deployed_commit": deployed,
            "actor": "release-agent",
            "dry_run": False,
        })

        self.assertTrue(cleaned["ok"], cleaned)
        self.assertEqual(cleaned["status"], "cleaned")
        self.assertEqual(
            {(action["target"], action["action"]) for action in cleaned["performed_actions"]},
            {("local", "delete_branch"), ("remote", "delete_branch")},
        )
        self.assertNotEqual(
            subprocess.run(
                ["git", "-C", str(repo), "show-ref", "--verify", "refs/heads/task-679"],
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ).returncode,
            0,
        )
        self.assertEqual(self.git(repo, "ls-remote", "--heads", "origin", "task-679").stdout.strip(), "")

        idempotent = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "task-679",
            "source_commit": source,
            "deployed_commit": deployed,
            "actor": "release-agent",
            "dry_run": False,
        })

        self.assertTrue(idempotent["ok"], idempotent)
        self.assertEqual(idempotent["status"], "cleaned")
        self.assertEqual(idempotent["local"]["planned_action"], "already_missing")
        self.assertEqual(idempotent["remote_status"]["planned_action"], "already_missing")

    def test_cleanup_task_branch_refuses_protected_branch(self) -> None:
        service, _, tmp = self.make_service()
        repo, _, source, deployed = self.make_cleanup_repo(tmp)

        payload = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "main",
            "source_commit": source,
            "deployed_commit": deployed,
            "actor": "release-agent",
        })

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "unsafe_branch_cleanup_refused")
        self.assertIn("protected_branch_refused", {error["error"] for error in payload["errors"]})

    def test_cleanup_task_branch_refuses_source_commit_missing_from_deployed_history(self) -> None:
        service, _, tmp = self.make_service()
        repo, initial, source, _ = self.make_cleanup_repo(tmp)

        payload = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "task-679",
            "source_commit": source,
            "deployed_commit": initial,
            "actor": "release-agent",
        })

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "unsafe_branch_cleanup_refused")
        self.assertIn("source_commit_not_retained_in_deployed_history", {error["error"] for error in payload["errors"]})

    def test_cleanup_task_branch_refuses_branch_tip_drift(self) -> None:
        service, _, tmp = self.make_service()
        repo, _, source, deployed = self.make_cleanup_repo(tmp)
        self.git(repo, "checkout", "task-679")
        self.commit_file(repo, "drift.txt", "drift\n", "branch drift")
        self.git(repo, "push", "origin", "task-679")
        self.git(repo, "checkout", "main")

        payload = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "task-679",
            "source_commit": source,
            "deployed_commit": deployed,
            "actor": "release-agent",
        })

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "unsafe_branch_cleanup_refused")
        self.assertIn("local_branch_tip_not_safe", {error["error"] for error in payload["errors"]})

    def test_cleanup_task_branch_refuses_active_lease_using_branch(self) -> None:
        service, manager, tmp = self.make_service()
        repo, _, source, deployed = self.make_cleanup_repo(tmp)
        manager.acquire("agent-hq-dev", "679", "cinder", branch="task-679", commit=source)

        payload = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "task-679",
            "source_commit": source,
            "deployed_commit": deployed,
            "actor": "release-agent",
        })

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "unsafe_branch_cleanup_refused")
        self.assertIn("active_lease_using_branch", {error["error"] for error in payload["errors"]})

    def test_cleanup_task_branch_refuses_worktree_using_branch(self) -> None:
        service, _, tmp = self.make_service()
        repo, _, source, deployed = self.make_cleanup_repo(tmp)
        linked_worktree = Path(tmp.name) / "task-679-worktree"
        self.git(repo, "worktree", "add", str(linked_worktree), "task-679")

        payload = service.call_tool("dev_env_cleanup_task_branch", {
            "repo_path": str(repo),
            "source_branch": "task-679",
            "source_commit": source,
            "deployed_commit": deployed,
            "actor": "release-agent",
        })

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "unsafe_branch_cleanup_refused")
        self.assertIn("worktree_using_branch", {error["error"] for error in payload["errors"]})


if __name__ == "__main__":
    unittest.main()
