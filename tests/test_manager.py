from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import threading
import unittest

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.manager import LeaseManager


class ManagerTestCase(unittest.TestCase):
    def make_manager(self, stale_after_seconds: int = 3600, deploy_command: str | None = None,
                     served_commit_command: str | None = None) -> tuple[LeaseManager, tempfile.TemporaryDirectory[str]]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        payload = {
            "data_path": str(Path(tmp.name) / "state.sqlite3"),
            "environments": [
                {
                    "id": "agent-hq-dev",
                    "label": "Agent HQ Dev",
                    "base_url": "http://127.0.0.1:3510",
                    "repo_path": str(Path(tmp.name) / "dev"),
                    "stale_after_seconds": stale_after_seconds,
                    "deploy_command": deploy_command,
                    "served_commit_command": served_commit_command,
                }
            ],
        }
        config_path = Path(tmp.name) / "envs.json"
        config_path.write_text(json.dumps(payload), encoding="utf-8")
        return LeaseManager(load_config(str(config_path))), tmp


class LeaseManagerTests(ManagerTestCase):
    def test_acquire_blocks_second_owner_with_busy_shape(self) -> None:
        manager, _ = self.make_manager()
        first = manager.acquire("agent-hq-dev", "426", "cinder", "94", "Cinder", "task-426", "abc123")
        second = manager.acquire("agent-hq-dev", "427", "prism", "95", "Prism", "task-427", "def456")

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["error"], "environment_busy")
        self.assertEqual(second["owner"]["task_id"], "426")
        self.assertEqual(second["owner"]["commit"], "abc123")
        self.assertIn("Do not deploy", second["next_action"])

    def test_concurrent_acquire_allows_one_owner(self) -> None:
        manager, tmp = self.make_manager()
        db_path = manager.config.data_path
        manager.close()

        results = []
        lock = threading.Lock()

        def acquire(task_id: str) -> None:
            payload = {
                "data_path": db_path,
                "environments": [{"id": "agent-hq-dev", "label": "Agent HQ Dev"}],
            }
            config_path = Path(tmp.name) / f"{task_id}.json"
            config_path.write_text(json.dumps(payload), encoding="utf-8")
            local = LeaseManager(load_config(str(config_path)))
            try:
                result = local.acquire("agent-hq-dev", task_id, f"actor-{task_id}")
                with lock:
                    results.append(result)
            finally:
                local.close()

        threads = [threading.Thread(target=acquire, args=(str(i),)) for i in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sum(1 for result in results if result["ok"]), 1)
        self.assertEqual(sum(1 for result in results if not result["ok"] and result["error"] == "environment_busy"), 1)

    def test_valid_state_machine_and_event_history(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]

        self.assertEqual(manager.transition(lease["id"], "mark_deploying", "deploy")["status"], "deploying")
        qa = manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")
        self.assertEqual(qa["status"], "deployed_for_qa")
        self.assertEqual(qa["agent_hq_evidence"]["review_commit"], "abc123")
        self.assertEqual(manager.transition(lease["id"], "mark_prod_deploying", "release")["status"], "prod_deploying")
        self.assertEqual(manager.release(lease["id"], "release", "done")["status"], "done")

        events = manager.events(lease["id"])["events"]
        self.assertEqual([event["event_type"] for event in events], [
            "acquire",
            "mark_deploying",
            "mark_deployed_for_qa",
            "mark_prod_deploying",
            "release",
        ])

    def test_invalid_transition_fails_clearly(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]
        result = manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_transition")
        self.assertEqual(result["from_status"], "acquired")

    def test_release_requires_allowed_reason(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]
        result = manager.release(lease["id"], "cinder", "because")

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_release_reason")

    def test_admin_force_release_requires_actor_and_reason(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]

        self.assertFalse(manager.force_release("", "stale", lease_id=lease["id"])["ok"])
        self.assertFalse(manager.force_release("operator", "", lease_id=lease["id"])["ok"])
        released = manager.force_release("operator", "stale owner", lease_id=lease["id"])
        self.assertTrue(released["ok"])
        self.assertEqual(released["status"], "force_released")

    def test_stale_detection_marks_visible_state_without_silent_release(self) -> None:
        manager, _ = self.make_manager(stale_after_seconds=1)
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]
        old = (datetime.now(timezone.utc) - timedelta(seconds=120)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        manager.conn.execute("UPDATE leases SET heartbeat_at = ? WHERE id = ?", (old, lease["id"]))

        status = manager.status()["environments"][0]
        self.assertTrue(status["stale"])
        self.assertIsNone(status["active_lease"]["released_at"])

        swept = manager.sweep_stale("operator")
        self.assertEqual(swept["marked_stale"][0]["status"], "stale")

    def test_validate_qa_detects_commit_mismatch_as_integrity_failure(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        result = manager.validate_qa("426", "def456", lease_id=lease["id"])
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "environment_integrity_failure")
        self.assertIn("not product failure", result["next_action"])

    def test_lease_aware_deploy_dry_run_keeps_environment_locked_for_qa(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()

        result = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            agent_id="94",
            agent_name="Cinder",
            branch="task-426",
            commit="abc123",
            dry_run=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "deployed_for_qa")
        self.assertEqual(manager.status()["environments"][0]["active_lease"]["status"], "deployed_for_qa")

    def test_deploy_failure_releases_environment(self) -> None:
        manager, tmp = self.make_manager(deploy_command="python3 -c 'import sys; sys.exit(3)'")
        source = Path(tmp.name) / "source"
        source.mkdir()

        result = manager.lease_aware_deploy("agent-hq-dev", "426", "cinder", str(source))

        self.assertTrue(result["ok"])
        self.assertEqual(result["release_reason"], "deploy_failed")
        self.assertTrue(manager.status()["environments"][0]["available"])


if __name__ == "__main__":
    unittest.main()

