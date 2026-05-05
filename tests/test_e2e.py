from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.manager import LeaseManager


class LeaseWorkflowE2ETests(unittest.TestCase):
    def make_manager(self, deploy_command: str | None = None,
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
                    "repo_path": str(Path(tmp.name) / "shared-dev"),
                    "deploy_command": deploy_command,
                    "served_commit_command": served_commit_command,
                    "stale_after_seconds": 3600,
                }
            ],
        }
        config_path = Path(tmp.name) / "envs.json"
        config_path.write_text(json.dumps(payload), encoding="utf-8")
        manager = LeaseManager(load_config(str(config_path)))
        self.addCleanup(manager.close)
        return manager, tmp

    def make_source(self, tmp: tempfile.TemporaryDirectory[str]) -> Path:
        source = Path(tmp.name) / "source"
        source.mkdir()
        return source

    def test_competing_dev_deploy_cannot_overwrite_active_qa_lease(self) -> None:
        manager, tmp = self.make_manager()
        source = self.make_source(tmp)

        cinder = manager.lease_aware_deploy(
            "agent-hq-dev",
            "430-a",
            "cinder",
            str(source),
            agent_id="cinder-backend",
            agent_name="Cinder",
            branch="cinder/task-430-a",
            commit="commit-a",
            dry_run=True,
        )
        prism = manager.lease_aware_deploy(
            "agent-hq-dev",
            "430-b",
            "prism",
            str(source),
            agent_id="prism-fullstack",
            agent_name="Prism",
            branch="prism/task-430-b",
            commit="commit-b",
            dry_run=True,
        )

        self.assertTrue(cinder["ok"])
        self.assertEqual(cinder["status"], "deployed_for_qa")
        self.assertEqual(cinder["agent_hq_evidence"]["lease"]["task_id"], "430-a")
        self.assertFalse(prism["ok"])
        self.assertEqual(prism["error"], "environment_busy")
        self.assertEqual(prism["owner"]["task_id"], "430-a")
        self.assertEqual(prism["owner"]["commit"], "commit-a")
        self.assertIn("Do not deploy", prism["next_action"])

    def test_successful_deploy_verifies_served_commit_and_returns_review_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_name:
            served = Path(tmp_name) / "served.txt"
            deploy_script = Path(tmp_name) / "deploy.py"
            deploy_script.write_text(
                "from pathlib import Path\n"
                "import os\n"
                f"Path({str(served)!r}).write_text(os.environ['DEV_LEASE_COMMIT'], encoding='utf-8')\n",
                encoding="utf-8",
            )
            manager, tmp = self.make_manager(
                deploy_command=f"{sys.executable} {deploy_script}",
                served_commit_command=f"{sys.executable} -c \"from pathlib import Path; print(Path({str(served)!r}).read_text())\"",
            )
            source = self.make_source(tmp)

            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "deploy_dev_worktree",
                str(source),
                agent_id="cinder-backend",
                agent_name="Cinder",
                branch="cinder/task-426",
                commit="commit-426",
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "deployed_for_qa")
            self.assertEqual(result["agent_hq_evidence"]["review_commit"], "commit-426")
            self.assertEqual(result["agent_hq_evidence"]["review_url"], "http://127.0.0.1:3510")
            self.assertEqual(result["agent_hq_evidence"]["lease"]["lease_id"], result["lease"]["id"])
            self.assertFalse(manager.status()["environments"][0]["available"])

    def test_deploy_failure_releases_environment_with_audit_reason(self) -> None:
        manager, tmp = self.make_manager(deploy_command=f"{sys.executable} -c \"import sys; sys.exit(7)\"")
        source = self.make_source(tmp)

        result = manager.lease_aware_deploy("agent-hq-dev", "426", "cinder", str(source))

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "deploy_failed")
        self.assertEqual(result["release_reason"], "deploy_failed")
        self.assertTrue(manager.status()["environments"][0]["available"])

    def test_served_commit_mismatch_is_deploy_integrity_failure_and_releases(self) -> None:
        manager, tmp = self.make_manager(
            deploy_command=f"{sys.executable} -c \"print('deployed')\"",
            served_commit_command=f"{sys.executable} -c \"print('other-commit')\"",
        )
        source = self.make_source(tmp)

        result = manager.lease_aware_deploy("agent-hq-dev", "426", "cinder", str(source), commit="expected-commit")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "deploy_failed")
        self.assertEqual(result["release_reason"], "deploy_failed")
        self.assertIn("other-commit", result["agent_hq_note"])
        self.assertTrue(manager.status()["environments"][0]["available"])

    def test_qa_refuses_commit_mismatch_and_qa_failure_releases(self) -> None:
        manager, tmp = self.make_manager()
        source = self.make_source(tmp)
        deployed = manager.lease_aware_deploy("agent-hq-dev", "427", "cinder", str(source), commit="commit-427", dry_run=True)
        lease_id = deployed["lease"]["id"]

        mismatch = manager.validate_qa("427", "wrong-commit", lease_id=lease_id)
        qa_failed = manager.release(lease_id, "quinn-qa", "qa_failed", "regression failed")

        self.assertFalse(mismatch["ok"])
        self.assertEqual(mismatch["error"], "environment_integrity_failure")
        self.assertIn("not product failure", mismatch["next_action"])
        self.assertTrue(qa_failed["ok"])
        self.assertEqual(qa_failed["release_reason"], "qa_failed")
        self.assertIn("Reason: qa_failed", qa_failed["agent_hq_note"])
        self.assertTrue(manager.status()["environments"][0]["available"])

    def test_production_failure_and_done_release_paths_are_audited(self) -> None:
        manager, tmp = self.make_manager()
        source = self.make_source(tmp)

        prod_fail = manager.lease_aware_deploy("agent-hq-dev", "427-fail", "cinder", str(source), commit="commit-fail", dry_run=True)
        manager.transition(prod_fail["lease"]["id"], "mark_prod_deploying", "release")
        failed = manager.release(prod_fail["lease"]["id"], "release", "prod_failed", "prod deploy failed")

        done = manager.lease_aware_deploy("agent-hq-dev", "427-done", "cinder", str(source), commit="commit-done", dry_run=True)
        manager.transition(done["lease"]["id"], "mark_prod_deploying", "release")
        released = manager.release(done["lease"]["id"], "release", "done", "production verified")

        self.assertEqual(failed["release_reason"], "prod_failed")
        self.assertIn("Reason: prod_failed", failed["agent_hq_note"])
        self.assertEqual(released["release_reason"], "done")
        self.assertIn("Reason: done", released["agent_hq_note"])
        self.assertTrue(manager.status()["environments"][0]["available"])


if __name__ == "__main__":
    unittest.main()
