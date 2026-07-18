from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.manager import LeaseManager


class QueueIntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        config_path = root / "envs.json"
        config_path.write_text(json.dumps({
            "data_path": str(root / "state.sqlite3"),
            "environments": [{"id": "agency-crm-dev", "label": "Agency CRM Dev"}],
        }), encoding="utf-8")
        self.manager = LeaseManager(load_config(str(config_path)))
        self.addCleanup(self.manager.close)

    def make_repo(self) -> tuple[Path, str]:
        repo = Path(self.tmp.name) / "task-worktree"
        repo.mkdir()
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.email", "tests@example.com"], check=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.name", "Tests"], check=True)
        (repo / "tracked.txt").write_text("task commit\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-qm", "task commit"], check=True)
        head = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            check=True, text=True, capture_output=True,
        ).stdout.strip()
        return repo, head

    def test_enqueue_rejects_missing_path(self) -> None:
        result = self.manager.enqueue_deploy_request(
            "agency-crm-dev", "925", "Kepler", str(Path(self.tmp.name) / "missing"), commit="a" * 40,
        )
        self.assertEqual(result["error"], "source_repo_path_not_found")
        self.assertIn("next_action", result)

    def test_enqueue_rejects_head_mismatch_and_accepts_exact_full_sha(self) -> None:
        repo, head = self.make_repo()
        mismatch = self.manager.enqueue_deploy_request(
            "agency-crm-dev", "925", "Harlow", str(repo), commit="b" * 40,
        )
        self.assertEqual(mismatch["error"], "source_commit_mismatch")
        self.assertEqual(mismatch["actual_head_sha"], head)
        self.assertIn("recovery/PM workspace", mismatch["next_action"])

        valid = self.manager.enqueue_deploy_request(
            "agency-crm-dev", "925", "Kepler", str(repo), commit=head,
        )
        self.assertTrue(valid["ok"], valid)
        self.assertEqual(valid["queue"]["commit_sha"], head)

    def test_claim_revalidates_source_and_fails_mutated_worktree(self) -> None:
        repo, head = self.make_repo()
        queued = self.manager.enqueue_deploy_request(
            "agency-crm-dev", "925", "Kepler", str(repo), commit=head,
        )["queue"]
        (repo / "tracked.txt").write_text("new commit\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-qm", "moved head"], check=True)

        result = self.manager.sweep_deploy_queue("queue-worker", environment_id="agency-crm-dev")
        self.assertEqual(result["processed"][0]["result"]["error"], "source_commit_mismatch")
        self.assertEqual(self.manager._queue_row(queued["id"])["status"], "failed")
        self.assertIsNone(self.manager._active_lease("agency-crm-dev"))

    def test_every_terminal_release_variant_wakes_queue(self) -> None:
        cases = [
            ("deploying", "deploy_failed"),
            ("deployed_for_qa", "qa_failed"),
            ("prod_deploying", "prod_failed"),
            ("prod_deploying", "done"),
            ("acquired", "manual_release"),
        ]
        for index, (status, reason) in enumerate(cases):
            with self.subTest(reason=reason):
                lease = self.manager.acquire("agency-crm-dev", f"task-{index}", "agent")["lease"]
                if status in {"deploying", "deployed_for_qa", "prod_deploying"}:
                    self.manager.transition(lease["id"], "mark_deploying", "agent")
                if status in {"deployed_for_qa", "prod_deploying"}:
                    self.manager.transition(lease["id"], "mark_deployed_for_qa", "agent")
                if status == "prod_deploying":
                    self.manager.transition(lease["id"], "mark_prod_deploying", "agent")
                with patch.object(self.manager, "sweep_deploy_queue", return_value={"ok": True, "processed": [], "skipped": []}) as sweep:
                    result = self.manager.release(lease["id"], "agent", reason)
                self.assertTrue(result["queue_sweep"]["ok"])
                sweep.assert_called_once_with("queue-worker", environment_id="agency-crm-dev", limit=1)

    def test_force_and_stale_release_wake_queue(self) -> None:
        lease = self.manager.acquire("agency-crm-dev", "forced", "agent")["lease"]
        with patch.object(self.manager, "sweep_deploy_queue", return_value={"ok": True, "processed": [], "skipped": []}) as sweep:
            result = self.manager.force_release("operator", "recovery", lease_id=lease["id"])
        self.assertTrue(result["queue_sweep"]["ok"])
        sweep.assert_called_once()

        stale = self.manager.acquire("agency-crm-dev", "stale", "agent")["lease"]
        self.manager.conn.execute("UPDATE leases SET status = 'stale' WHERE id = ?", (stale["id"],))
        with patch.object(self.manager, "sweep_deploy_queue", return_value={"ok": True, "processed": [], "skipped": []}) as sweep:
            result = self.manager.release(stale["id"], "sweeper", "stale_released")
        self.assertTrue(result["queue_sweep"]["ok"])
        sweep.assert_called_once()

    def test_reentrant_wake_is_deferred_without_recursive_sweep(self) -> None:
        calls = 0
        def sweep(*args: object, **kwargs: object) -> dict[str, object]:
            nonlocal calls
            calls += 1
            if calls == 1:
                nested = self.manager._wake_deploy_queue("agency-crm-dev")
                self.assertTrue(nested["deferred"])
            return {"ok": True, "processed": [], "skipped": []}

        with patch.object(self.manager, "sweep_deploy_queue", side_effect=sweep):
            result = self.manager._wake_deploy_queue("agency-crm-dev")
        self.assertTrue(result["ok"])
        self.assertEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()
