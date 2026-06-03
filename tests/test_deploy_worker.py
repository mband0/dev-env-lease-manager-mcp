from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.db import connect
from dev_env_lease_manager.manager import LeaseManager
from dev_env_lease_manager.worker import DeployWorker

from test_manager import ManagerTestCase


def _old_iso(seconds_ago: int) -> str:
    moment = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    return moment.replace(microsecond=0).isoformat().replace("+00:00", "Z")


class ReconcilerTests(ManagerTestCase):
    def _enqueue_and_claim(self, manager: LeaseManager, source: Path, worker_pid: int,
                           task_id: str = "426"):
        manager.enqueue_deploy_request("agent-hq-dev", task_id, "cinder", str(source),
                                       branch=f"task-{task_id}", commit="queued-commit")
        claim = manager.claim_next_deploy("worker", "worker-1", worker_pid=worker_pid)
        self.assertTrue(claim["ok"])
        self.assertTrue(claim["claimed"])
        return claim

    def test_reconciler_fails_orphan_with_dead_worker_pid(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        claim = self._enqueue_and_claim(manager, source, worker_pid=999999)
        queue_id = claim["queue"]["id"]
        lease_id = claim["lease"]["id"]

        result = manager.reconcile_stuck_deploys("reconciler")

        self.assertTrue(result["ok"])
        self.assertEqual(len(result["reconciled"]), 1)
        self.assertEqual(manager._queue_row(queue_id)["status"], "failed")
        self.assertEqual(manager._lease(lease_id)["status"], "deploy_failed")
        self.assertTrue(manager.status("agent-hq-dev")["environments"][0]["available"])

    def test_reconciler_skips_live_deploy_with_fresh_heartbeat(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        claim = self._enqueue_and_claim(manager, source, worker_pid=None or 1)  # pid 1 is alive
        # claim defaults worker_pid to current process when None; force an alive pid.
        manager.conn.execute("UPDATE deploy_queue SET worker_pid = ? WHERE id = ?", (1, claim["queue"]["id"]))

        result = manager.reconcile_stuck_deploys("reconciler")

        self.assertEqual(result["reconciled"], [])
        self.assertEqual(manager._queue_row(claim["queue"]["id"])["status"], "deploying")

    def test_reconciler_fails_stale_heartbeat_without_held_lock(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        claim = self._enqueue_and_claim(manager, source, worker_pid=1)  # alive pid
        queue_id = claim["queue"]["id"]
        manager.conn.execute(
            "UPDATE deploy_queue SET worker_pid = ?, heartbeat_at = ? WHERE id = ?",
            (1, _old_iso(7200), queue_id),
        )

        # Alive pid but no progress past the grace window and no native deploy lock held.
        result = manager.reconcile_stuck_deploys("reconciler", orphan_after_seconds=600)

        self.assertEqual(len(result["reconciled"]), 1)
        self.assertEqual(manager._queue_row(queue_id)["status"], "failed")

    def test_reconciler_dry_run_does_not_mutate(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        claim = self._enqueue_and_claim(manager, source, worker_pid=999999)

        result = manager.reconcile_stuck_deploys("reconciler", dry_run=True)

        self.assertEqual(len(result["reconciled"]), 1)
        self.assertTrue(result["reconciled"][0]["dry_run"])
        self.assertEqual(manager._queue_row(claim["queue"]["id"])["status"], "deploying")

    def test_reconciler_fires_deploy_failed_callback(self) -> None:
        manager, tmp = self.make_manager(agent_hq={"base_url": "http://agent-hq.local"})
        source = Path(tmp.name) / "source"
        source.mkdir()
        import os as _os
        _os.environ["AGENT_HQ_MCP_API_KEY"] = "test-key"
        self.addCleanup(_os.environ.pop, "AGENT_HQ_MCP_API_KEY", None)
        self._enqueue_and_claim(manager, source, worker_pid=999999)

        events: list[dict] = []
        from unittest.mock import patch

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self):
                return b'{"ok":true}'

        def fake_urlopen(request, timeout=15):
            events.append(json.loads(request.data.decode("utf-8")))
            return Response()

        with patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            manager.reconcile_stuck_deploys("reconciler")

        self.assertEqual([e["event"] for e in events], ["deploy_failed"])
        self.assertEqual(events[0]["error"]["stage"], "deploy_interrupted")


class AtomicClaimTests(ManagerTestCase):
    def test_only_one_worker_claims_a_queued_row(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.enqueue_deploy_request("agent-hq-dev", "426", "cinder", str(source), commit="c1")
        db_path = manager.config.data_path
        manager.close()

        def fresh_manager(tag: str) -> LeaseManager:
            payload = {
                "data_path": db_path,
                "environments": [{"id": "agent-hq-dev", "label": "Agent HQ Dev",
                                  "repo_path": str(Path(tmp.name) / "dev")}],
            }
            config_path = Path(tmp.name) / f"{tag}.json"
            config_path.write_text(json.dumps(payload), encoding="utf-8")
            return LeaseManager(load_config(str(config_path)))

        worker_a = fresh_manager("a")
        worker_b = fresh_manager("b")
        self.addCleanup(worker_a.close)
        self.addCleanup(worker_b.close)

        claim_a = worker_a.claim_next_deploy("a", "worker-a", worker_pid=1)
        claim_b = worker_b.claim_next_deploy("b", "worker-b", worker_pid=1)

        claimed = [c for c in (claim_a, claim_b) if c.get("claimed")]
        self.assertEqual(len(claimed), 1)


class WorkerTickTests(ManagerTestCase):
    def test_worker_run_once_deploys_queued_request(self) -> None:
        manager, tmp = self.make_manager(deploy_command="python3 -c 'import sys; sys.exit(0)'")
        source = Path(tmp.name) / "source"
        source.mkdir()
        enqueue = manager.enqueue_deploy_request("agent-hq-dev", "426", "cinder", str(source),
                                                 branch="task-426", commit="queued-commit")
        queue_id = enqueue["queue"]["id"]

        worker = DeployWorker(manager, worker_id="test-worker")
        summary = worker.run_once()

        self.assertTrue(summary["ok"])
        self.assertTrue(summary["deployed"])
        self.assertEqual(manager._queue_row(queue_id)["status"], "deployed")
        lease = manager.status("agent-hq-dev")["environments"][0]["active_lease"]
        self.assertEqual(lease["task_id"], "426")
        self.assertEqual(lease["status"], "deployed_for_qa")

    def test_worker_run_once_idle_when_queue_empty(self) -> None:
        manager, _ = self.make_manager()
        worker = DeployWorker(manager, worker_id="test-worker")
        summary = worker.run_once()
        self.assertTrue(summary["ok"])
        self.assertFalse(summary["deployed"])
        self.assertEqual(summary["reason"], "queue_empty")

    def test_worker_run_once_reconciles_orphan_before_claiming(self) -> None:
        manager, tmp = self.make_manager(deploy_command="python3 -c 'import sys; sys.exit(0)'")
        source = Path(tmp.name) / "source"
        source.mkdir()
        # Orphan a prior deploy on the only environment.
        manager.enqueue_deploy_request("agent-hq-dev", "425", "anchor", str(source), commit="old")
        claim = manager.claim_next_deploy("worker", "dead-worker", worker_pid=999999)
        orphan_queue_id = claim["queue"]["id"]

        worker = DeployWorker(manager, worker_id="test-worker")
        summary = worker.run_once()

        self.assertTrue(summary["reconciled"]["reconciled"])
        self.assertEqual(manager._queue_row(orphan_queue_id)["status"], "failed")


class HeartbeatTests(ManagerTestCase):
    def test_update_deploy_heartbeat_advances_phase(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.enqueue_deploy_request("agent-hq-dev", "426", "cinder", str(source), commit="c1")
        claim = manager.claim_next_deploy("worker", "worker-1", worker_pid=1)
        queue_id = claim["queue"]["id"]
        manager.conn.execute("UPDATE deploy_queue SET heartbeat_at = ? WHERE id = ?", (_old_iso(60), queue_id))

        manager.update_deploy_heartbeat(queue_id, "api_built")

        row = manager._queue_row(queue_id)
        self.assertEqual(row["phase"], "api_built")
        self.assertGreater(row["heartbeat_at"], _old_iso(60))


class AsyncDeployTests(ManagerTestCase):
    def test_deploy_worktree_async_returns_accepted(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()

        result = manager.deploy_worktree_async("agent-hq-dev", "426", "cinder", str(source),
                                               branch="task-426", commit="c1")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "accepted")
        self.assertTrue(result["queue_id"])
        self.assertEqual(manager._queue_row(result["queue_id"])["status"], "queued")

    def test_wait_seconds_times_out_without_worker(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()

        result = manager.deploy_worktree_async("agent-hq-dev", "426", "cinder", str(source),
                                               commit="c1", wait_seconds=1)

        self.assertEqual(result["status"], "accepted")
        self.assertFalse(result["wait"]["settled"])
        self.assertEqual(result["wait"]["reason"], "still_deploying")

    def test_wait_seconds_settles_after_worker_deploys(self) -> None:
        manager, tmp = self.make_manager(deploy_command="python3 -c 'import sys; sys.exit(0)'")
        source = Path(tmp.name) / "source"
        source.mkdir()
        accepted = manager.deploy_worktree_async("agent-hq-dev", "426", "cinder", str(source), commit="c1")
        # Drain synchronously (stands in for the background worker).
        DeployWorker(manager, worker_id="test-worker").run_once()

        wait = manager._wait_for_queue_terminal(accepted["queue_id"], 1)
        self.assertTrue(wait["settled"])
        self.assertEqual(wait["status"], "deployed")


class SchemaMigrationTests(unittest.TestCase):
    def test_connect_adds_durability_columns_to_legacy_deploy_queue(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = str(Path(tmp.name) / "legacy.sqlite3")
        legacy = sqlite3.connect(db_path)
        # Mirror the original pre-durability deploy_queue schema (without the new columns).
        legacy.execute(
            """
            CREATE TABLE deploy_queue (
              id TEXT PRIMARY KEY,
              environment_id TEXT NOT NULL,
              task_id TEXT NOT NULL,
              actor TEXT NOT NULL,
              agent_id TEXT,
              agent_name TEXT,
              branch TEXT,
              commit_sha TEXT,
              source_repo_path TEXT NOT NULL,
              services TEXT NOT NULL DEFAULT 'both',
              health_check INTEGER NOT NULL DEFAULT 1,
              priority INTEGER NOT NULL DEFAULT 0,
              status TEXT NOT NULL,
              callback_url TEXT,
              callback_api_key TEXT,
              lease_id TEXT,
              requested_at TEXT NOT NULL,
              started_at TEXT,
              completed_at TEXT,
              updated_at TEXT NOT NULL,
              error_json TEXT NOT NULL DEFAULT '{}',
              metadata_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        legacy.execute(
            "INSERT INTO deploy_queue (id, environment_id, task_id, actor, source_repo_path, status, requested_at, updated_at)"
            " VALUES ('q1', 'agent-hq-dev', '426', 'cinder', '/tmp/source', 'queued', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
        )
        legacy.commit()
        legacy.close()

        conn = connect(db_path)
        try:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(deploy_queue)").fetchall()}
            # Existing rows survive the in-place upgrade.
            preserved = conn.execute("SELECT status FROM deploy_queue WHERE id = 'q1'").fetchone()
        finally:
            conn.close()
        for column in ("heartbeat_at", "phase", "worker_pid", "worker_id", "attempts", "claimed_at"):
            self.assertIn(column, columns)
        self.assertEqual(preserved["status"], "queued")


if __name__ == "__main__":
    unittest.main()
