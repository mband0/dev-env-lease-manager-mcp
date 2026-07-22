from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.deploy import NativeDeployError, NativeDeployLock
from dev_env_lease_manager.manager import LeaseManager


class MockHttpResponse:
    def __init__(self, body: dict[str, object] | bytes | str | None = None, status: int = 200):
        self.status = status
        if body is None:
            self._body = b'{"ok":true}'
        elif isinstance(body, bytes):
            self._body = body
        elif isinstance(body, str):
            self._body = body.encode("utf-8")
        else:
            self._body = json.dumps(body).encode("utf-8")

    def __enter__(self) -> "MockHttpResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def is_active_owner_request(request: object) -> bool:
    return getattr(request, "full_url", "").endswith("/active-owner")


def active_owner_response(task_id: str = "426", agent_id: int = 94) -> MockHttpResponse:
    return MockHttpResponse({
        "ok": True,
        "task_id": int(task_id),
        "authenticated_agent_id": agent_id,
        "authenticated_agent_slug": "cinder-backend",
        "active_instance_id": 7001,
        "active_instance_agent_id": agent_id,
        "active_instance_status": "running",
        "is_active_owner": True,
        "reason": "active_instance_owned_by_authenticated_agent",
    })


class ManagerTestCase(unittest.TestCase):
    def make_manager(self, stale_after_seconds: int = 3600, deploy_command: str | None = None,
                     served_commit_command: str | None = None,
                     metadata: dict[str, object] | None = None,
                     agent_hq: dict[str, object] | None = None,
                     environment_tags: list[str] | None = None,
                     extra_environments: list[dict[str, object]] | None = None) -> tuple[LeaseManager, tempfile.TemporaryDirectory[str]]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        environment = {
            "id": "agent-hq-dev",
            "label": "Agent HQ Dev",
            "base_url": "http://127.0.0.1:3510",
            "repo_path": str(Path(tmp.name) / "dev"),
            "stale_after_seconds": stale_after_seconds,
            "deploy_command": deploy_command,
            "served_commit_command": served_commit_command,
            "tags": environment_tags or [],
            "metadata": metadata or {},
        }
        payload = {
            "data_path": str(Path(tmp.name) / "state.sqlite3"),
            "agent_hq": agent_hq or {},
            "environments": [environment, *(extra_environments or [])],
        }
        config_path = Path(tmp.name) / "envs.json"
        config_path.write_text(json.dumps(payload), encoding="utf-8")
        return LeaseManager(load_config(str(config_path))), tmp


class LeaseManagerTests(ManagerTestCase):
    def setUp(self) -> None:
        # Legacy queue tests use placeholder directories and synthetic commit
        # names. Source integrity has dedicated real-git tests below.
        self.source_validation_patch = patch.object(
            LeaseManager,
            "_validate_deploy_source",
            return_value={"ok": True},
        )
        self.source_validation_patch.start()
        self.addCleanup(self.source_validation_patch.stop)

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

    def test_rejects_qa_fixture_task_ids_before_lease_or_queue_creation(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()

        acquire = manager.acquire("agent-hq-dev", "999123", "cinder")
        deploy = manager.lease_aware_deploy("agent-hq-dev", "999123", "cinder", str(source), dry_run=True)
        queued = manager.enqueue_deploy_request("agent-hq-dev", "999123", "cinder", str(source))

        for result in (acquire, deploy, queued):
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "qa_fixture_task_not_deployable")
            self.assertEqual(result["task_id"], "999123")
            self.assertIn("QA fixtures", result["message"])

        lease_count = manager.conn.execute("SELECT COUNT(*) FROM leases").fetchone()[0]
        queue_count = manager.conn.execute("SELECT COUNT(*) FROM deploy_queue").fetchone()[0]
        self.assertEqual(lease_count, 0)
        self.assertEqual(queue_count, 0)

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

    def test_deploy_production_dry_run_does_not_change_lease_state(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        with patch("dev_env_lease_manager.manager.NativeProductionDeployer") as deployer:
            deployer.return_value.deploy.return_value = {"ok": True, "dry_run": True, "planned_actions": []}
            result = manager.deploy_production("agent-hq-dev", lease["id"], "426", "release", "abc123", dry_run=True)

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(manager._lease(lease["id"])["status"], "deployed_for_qa")
        deployer.return_value.deploy.assert_called_once()
        self.assertTrue(deployer.return_value.deploy.call_args.kwargs["dry_run"])

    def test_deploy_production_success_marks_done_with_evidence(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        with patch("dev_env_lease_manager.manager.NativeProductionDeployer") as deployer:
            deployer.return_value.deploy.return_value = {
                "ok": True,
                "dry_run": False,
                "deployed_commit": "abc123",
                "services": ["api", "ui"],
                "health": {"api": {"ok": True}, "ui": {"ok": True}},
                "state_file": "/tmp/state.json",
            }
            result = manager.deploy_production("agent-hq-dev", lease["id"], "426", "release", "abc123", dry_run=False)

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["status"], "done")
        self.assertEqual(result["release_reason"], "done")
        self.assertEqual(result["production_evidence"]["deployed_commit"], "abc123")
        events = manager.events(lease["id"])["events"]
        self.assertEqual([event["event_type"] for event in events][-2:], ["mark_prod_deploying", "release"])

    def test_deploy_production_failure_marks_prod_failed(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        with patch("dev_env_lease_manager.manager.NativeProductionDeployer") as deployer:
            deployer.return_value.deploy.side_effect = NativeDeployError(
                "api_health_failed",
                {"failure_class": "api_health_failed", "health": {"api": {"ok": False}}},
            )
            result = manager.deploy_production("agent-hq-dev", lease["id"], "426", "release", "abc123", dry_run=False)

        self.assertTrue(result["ok"], result)
        self.assertEqual(result["status"], "prod_failed")
        self.assertEqual(result["release_reason"], "prod_failed")
        self.assertEqual(result["production_deploy"]["failure_class"], "api_health_failed")

    def test_deploy_production_rejects_mismatched_commit_before_prod_state(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder", commit="abc123")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        result = manager.deploy_production("agent-hq-dev", lease["id"], "426", "release", "def456", dry_run=False)

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "environment_integrity_failure")
        self.assertEqual(manager._lease(lease["id"])["status"], "deployed_for_qa")

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

    def test_release_reasons_are_bound_to_state_machine(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]

        result = manager.release(lease["id"], "release", "done")

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_release_transition")
        self.assertEqual(result["from_status"], "acquired")
        self.assertEqual(result["release_reason"], "done")
        self.assertEqual(result["allowed_from"], ["prod_deploying"])

    def test_qa_failed_release_unlocks_after_qa_state(self) -> None:
        manager, _ = self.make_manager()
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]
        manager.transition(lease["id"], "mark_deploying", "deploy")
        manager.transition(lease["id"], "mark_deployed_for_qa", "deploy")

        result = manager.release(lease["id"], "qa", "qa_failed", "regression failed")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "qa_failed")
        self.assertTrue(manager.status()["environments"][0]["available"])

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

    def test_mcp_preflight_cleanup_releases_stale_leases_and_removes_stale_locks(self) -> None:
        state_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(state_dir, ignore_errors=True))
        manager, _ = self.make_manager(
            stale_after_seconds=1,
            metadata={"state_dir": str(state_dir), "deploy_lock_stale_after_seconds": 1},
        )
        lease = manager.acquire("agent-hq-dev", "426", "cinder")["lease"]
        old = (datetime.now(timezone.utc) - timedelta(seconds=120)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        manager.conn.execute("UPDATE leases SET heartbeat_at = ? WHERE id = ?", (old, lease["id"]))
        legacy_lock = state_dir / "lock"
        legacy_lock.mkdir()
        old_timestamp = datetime.now(timezone.utc).timestamp() - 120
        os.utime(legacy_lock, (old_timestamp, old_timestamp))

        cleanup = manager.mcp_preflight_cleanup()

        self.assertTrue(cleanup["ok"])
        self.assertEqual(cleanup["stale_leases"]["released"][0]["status"], "stale_released")
        self.assertEqual(cleanup["deploy_locks"]["removed"][0]["kind"], "legacy_dir")
        self.assertFalse(legacy_lock.exists())
        self.assertTrue(manager.status("agent-hq-dev")["environments"][0]["available"])

    def test_mcp_preflight_cleanup_emits_stale_release_callback_and_fails_deploying_queue(self) -> None:
        manager, tmp = self.make_manager(stale_after_seconds=1)
        source = Path(tmp.name) / "source"
        source.mkdir()
        posted_payloads: list[dict[str, object]] = []

        def capture_callback(request: object, timeout: int = 0) -> MockHttpResponse:
            del timeout
            data = getattr(request, "data", b"{}")
            posted_payloads.append(json.loads(data.decode("utf-8")))
            return MockHttpResponse({"ok": True, "received": True})

        with patch("urllib.request.urlopen", side_effect=capture_callback):
            queued = manager.enqueue_deploy_request(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="cinder/task-426",
                commit="abc123",
                callback_url="http://agent-hq.local",
                callback_api_key="secret",
            )
            self.assertTrue(queued["ok"], queued)

            queue_id = queued["queue"]["id"]
            lease = manager.acquire(
                "agent-hq-dev",
                "426",
                "queue-worker",
                branch="cinder/task-426",
                commit="abc123",
                metadata={"source_repo_path": str(source), "queue_id": queue_id},
            )["lease"]
            manager._set_queue_status(queue_id, "deploying", lease_id=lease["id"])
            manager.transition(lease["id"], "mark_deploying", "queue-worker")
            old = (datetime.now(timezone.utc) - timedelta(seconds=120)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            manager.conn.execute("UPDATE leases SET heartbeat_at = ? WHERE id = ?", (old, lease["id"]))

            cleanup = manager.mcp_preflight_cleanup()

        self.assertTrue(cleanup["ok"])
        released = cleanup["stale_leases"]["released"][0]
        self.assertEqual(released["status"], "stale_released")
        self.assertEqual(released["release_reason"], "stale_released")
        self.assertTrue(released["callback"]["ok"])

        stale_payload = posted_payloads[-1]
        self.assertEqual(stale_payload["event"], "stale_lease_released")
        self.assertEqual(stale_payload["task_id"], "426")
        self.assertEqual(stale_payload["queue_id"], queue_id)
        self.assertEqual(stale_payload["lease_id"], lease["id"])
        self.assertEqual(stale_payload["environment_id"], "agent-hq-dev")
        self.assertEqual(stale_payload["branch"], "cinder/task-426")
        self.assertEqual(stale_payload["commit_sha"], "abc123")
        self.assertEqual(stale_payload["release_reason"], "stale_released")
        self.assertEqual(stale_payload["prior_lease_status"], "deploying")
        self.assertEqual(stale_payload["prior_deploy_status"], "deploying")
        self.assertIn("stale lease released", str(stale_payload["message"]))

        queue = manager._queue_row(queue_id)
        self.assertEqual(queue["status"], "failed")
        self.assertEqual(queue["lease_id"], lease["id"])

        attempts = manager.callback_attempts(queue_id=queue_id)["callback_attempts"]
        self.assertEqual(attempts[0]["event"], "stale_lease_released")
        self.assertTrue(attempts[0]["ok"])

    def test_status_reports_native_deploy_lock_and_sweep_removes_stale_legacy_lock(self) -> None:
        state_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(state_dir, ignore_errors=True))
        manager, _ = self.make_manager(
            stale_after_seconds=1,
            metadata={"state_dir": str(state_dir), "deploy_lock_stale_after_seconds": 1},
        )
        legacy_lock = state_dir / "lock"
        legacy_lock.mkdir()
        old = datetime.now(timezone.utc).timestamp() - 120
        os.utime(legacy_lock, (old, old))

        status = manager.status("agent-hq-dev")["environments"][0]

        self.assertFalse(status["available"])
        self.assertEqual(status["blocked_by"], "native_deploy_lock")
        self.assertTrue(status["native_deploy_lock"]["blocks_deploy"])
        self.assertTrue(status["native_deploy_lock"]["stale"])

        swept = manager.sweep_deploy_locks("operator", "stale deploy lock", "agent-hq-dev")

        self.assertTrue(swept["ok"])
        self.assertEqual(len(swept["removed"]), 1)
        self.assertFalse(legacy_lock.exists())
        self.assertTrue(manager.status("agent-hq-dev")["environments"][0]["available"])

    def test_status_reports_active_native_deploy_lock_and_sweep_skips_live_lock(self) -> None:
        state_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(state_dir, ignore_errors=True))
        manager, _ = self.make_manager(metadata={"state_dir": str(state_dir)})
        env = manager.status("agent-hq-dev")["environments"][0]["environment"]

        with NativeDeployLock(str(state_dir), {"environment_id": "agent-hq-dev", "task_id": "426"}, env):
            status = manager.status("agent-hq-dev")["environments"][0]
            self.assertFalse(status["available"])
            self.assertEqual(status["blocked_by"], "native_deploy_lock")
            self.assertTrue(status["native_deploy_lock"]["blocks_deploy"])

            swept = manager.sweep_deploy_locks("operator", "stale deploy lock", "agent-hq-dev", force=True)
            self.assertEqual(len(swept["removed"]), 0)
            self.assertEqual(swept["skipped"][0]["reason"], "lock_is_currently_held")

    def test_native_deploy_lock_refuses_concurrent_holder_with_metadata(self) -> None:
        state_dir = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(state_dir, ignore_errors=True))
        manager, _ = self.make_manager(metadata={"state_dir": str(state_dir)})
        env = manager.status("agent-hq-dev")["environments"][0]["environment"]

        with NativeDeployLock(str(state_dir), {"environment_id": "agent-hq-dev", "task_id": "426"}, env):
            with self.assertRaises(NativeDeployError) as caught:
                with NativeDeployLock(str(state_dir), {"environment_id": "agent-hq-dev", "task_id": "427"}, env):
                    pass

        payload = caught.exception.payload()
        self.assertEqual(payload["error"], "deploy lock already held")
        self.assertTrue(payload["deploy_lock"]["blocks_deploy"])
        self.assertEqual(payload["deploy_lock"]["entries"][0]["metadata"]["task_id"], "426")

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

    def test_lease_aware_deploy_can_queue_when_environment_is_busy(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]

        result = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            agent_id="94",
            agent_name="Cinder",
            branch="task-426",
            commit="queued-commit",
            dry_run=True,
            queue_if_busy=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queue"]["position"], 1)
        status = manager.status()["environments"][0]
        self.assertEqual(status["active_lease"]["id"], active["id"])
        self.assertEqual(status["queue_depth"], 1)
        self.assertEqual(status["queue"][0]["commit_sha"], "queued-commit")

    def test_lease_aware_deploy_uses_next_available_matching_environment(self) -> None:
        manager, tmp = self.make_manager(
            environment_tags=["agent-hq", "dev", "shared-checkout"],
            extra_environments=[{
                "id": "agent-hq-dev-2",
                "label": "Agent HQ Dev 2",
                "base_url": "http://127.0.0.1:3520",
                "stale_after_seconds": 3600,
                "tags": ["agent-hq", "dev", "shared-checkout"],
            }],
        )
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]

        result = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="deploy-commit",
            dry_run=True,
            queue_if_busy=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "deployed_for_qa")
        self.assertEqual(result["requested_environment_id"], "agent-hq-dev")
        self.assertEqual(result["assigned_environment_id"], "agent-hq-dev-2")
        self.assertEqual(result["lease"]["environment_id"], "agent-hq-dev-2")
        by_env = {item["environment"]["id"]: item for item in manager.status()["environments"]}
        self.assertEqual(by_env["agent-hq-dev"]["active_lease"]["id"], active["id"])
        self.assertEqual(by_env["agent-hq-dev-2"]["active_lease"]["task_id"], "426")
        self.assertEqual(by_env["agent-hq-dev-2"]["active_lease"]["commit_sha"], "deploy-commit")

    def test_sweep_deploy_queue_assigns_queued_request_to_matching_available_environment(self) -> None:
        manager, tmp = self.make_manager(
            environment_tags=["agent-hq", "dev", "shared-checkout"],
            extra_environments=[{
                "id": "agent-hq-dev-2",
                "label": "Agent HQ Dev 2",
                "base_url": "http://127.0.0.1:3520",
                "stale_after_seconds": 3600,
                "tags": ["agent-hq", "dev", "shared-checkout"],
            }],
        )
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.acquire("agent-hq-dev", "425", "anchor", commit="active")
        active_dev_2 = manager.acquire("agent-hq-dev-2", "426", "prism", commit="active-2")["lease"]
        queued = manager.lease_aware_deploy(
            "agent-hq-dev",
            "427",
            "cinder",
            str(source),
            branch="task-427",
            commit="queued-commit",
            dry_run=True,
            queue_if_busy=True,
        )["queue"]
        manager.force_release("operator", "free alternate", lease_id=active_dev_2["id"], sweep_queue_after_release=False)

        result = manager.sweep_deploy_queue("queue-worker", environment_id="agent-hq-dev-2", dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(len(result["processed"]), 1)
        processed = result["processed"][0]
        self.assertEqual(processed["queue"]["id"], queued["id"])
        self.assertEqual(processed["queue"]["requested_environment_id"], "agent-hq-dev")
        self.assertEqual(processed["queue"]["assigned_environment_id"], "agent-hq-dev-2")
        self.assertEqual(processed["queue"]["environment_id"], "agent-hq-dev-2")
        lease = manager.status("agent-hq-dev-2")["environments"][0]["active_lease"]
        self.assertEqual(lease["task_id"], "427")
        self.assertEqual(lease["commit_sha"], "queued-commit")

    def test_queue_status_reports_current_busy_owner_from_active_lease(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", agent_name="Anchor", commit="active")["lease"]
        queued = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="queued-commit",
            queue_if_busy=True,
        )["queue"]

        entry = manager.queue_status("agent-hq-dev")["queue"][0]

        self.assertEqual(entry["id"], queued["id"])
        self.assertEqual(entry["busy_owner"]["task_id"], active["task_id"])
        self.assertEqual(entry["busy_owner"]["lease_id"], active["id"])
        self.assertEqual(entry["queued_because_owner"]["task_id"], active["task_id"])
        self.assertEqual(entry["metadata"]["busy_owner"]["task_id"], active["task_id"])
        self.assertEqual(entry["metadata"]["queued_because_owner"]["task_id"], active["task_id"])

    def test_queue_status_clears_busy_owner_after_release_but_keeps_queue_reason(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        queued = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="queued-commit",
            queue_if_busy=True,
        )["queue"]

        manager.force_release("operator", "free queue", lease_id=active["id"], sweep_queue_after_release=False)
        entry = manager.queue_status("agent-hq-dev")["queue"][0]

        self.assertEqual(entry["id"], queued["id"])
        self.assertIsNone(entry["busy_owner"])
        self.assertEqual(entry["queued_because_owner"]["task_id"], active["task_id"])
        self.assertNotIn("busy_owner", entry["metadata"])
        self.assertEqual(entry["metadata"]["queued_because_owner"]["task_id"], active["task_id"])

    def test_queue_status_maps_legacy_busy_owner_snapshot_to_queue_reason(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        queued = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="queued-commit",
            queue_if_busy=True,
        )["queue"]

        legacy_owner = {
            "task_id": active["task_id"],
            "agent_id": active["agent_id"],
            "agent_name": active["agent_name"],
            "branch": active["branch"],
            "commit": active["commit_sha"],
            "lease_id": active["id"],
            "status": active["status"],
        }
        manager.conn.execute(
            "UPDATE deploy_queue SET metadata_json = ? WHERE id = ?",
            (json.dumps({"busy_owner": legacy_owner}), queued["id"]),
        )

        manager.force_release("operator", "free queue", lease_id=active["id"], sweep_queue_after_release=False)
        entry = manager.queue_status("agent-hq-dev")["queue"][0]

        self.assertIsNone(entry["busy_owner"])
        self.assertEqual(entry["queued_because_owner"]["task_id"], active["task_id"])
        self.assertNotIn("busy_owner", entry["metadata"])
        self.assertEqual(entry["metadata"]["queued_because_owner"]["task_id"], active["task_id"])

    def test_sweep_deploy_queue_deploys_exact_queued_commit_after_release(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        queued = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="queued-commit",
            dry_run=True,
            queue_if_busy=True,
        )["queue"]
        manager.force_release("operator", "free queue", lease_id=active["id"], sweep_queue_after_release=False)

        result = manager.sweep_deploy_queue("queue-worker", dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(len(result["processed"]), 1)
        processed = result["processed"][0]
        self.assertEqual(processed["queue"]["id"], queued["id"])
        self.assertEqual(processed["queue"]["status"], "deployed")
        self.assertEqual(processed["result"]["status"], "deployed_for_qa")
        lease = manager.status()["environments"][0]["active_lease"]
        self.assertEqual(lease["task_id"], "426")
        self.assertEqual(lease["commit_sha"], "queued-commit")

    def test_sweep_deploy_queue_fails_preexisting_qa_fixture_queue(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        now = "2026-01-01T00:00:00Z"
        manager.conn.execute(
            """
            INSERT INTO deploy_queue (
              id, environment_id, task_id, actor, source_repo_path, status,
              requested_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
            """,
            ("fixture-queue", "agent-hq-dev", "999456", "cinder", str(source), now, now),
        )

        result = manager.sweep_deploy_queue("queue-worker", dry_run=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["processed"][0]["queue"]["status"], "failed")
        self.assertEqual(result["processed"][0]["result"]["error"], "qa_fixture_task_not_deployable")
        self.assertEqual(result["processed"][0]["result"]["task_id"], "999456")
        self.assertIsNone(manager.status()["environments"][0]["active_lease"])

    def test_release_sweeps_next_queued_request(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        queued = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="queued-commit",
            queue_if_busy=True,
        )["queue"]

        with patch("dev_env_lease_manager.manager.NativeDevDeployer") as deployer_class:
            deployer_class.return_value.deploy.return_value = {
                "ok": True,
                "mode": "native",
                "source_sha": "queued-commit",
            }
            result = manager.release(active["id"], "anchor", "manual_release")

        self.assertTrue(result["ok"])
        self.assertEqual(result["queue_sweep"]["processed"][0]["queue"]["id"], queued["id"])
        self.assertEqual(result["queue_sweep"]["processed"][0]["queue"]["status"], "deployed")
        lease = manager.status()["environments"][0]["active_lease"]
        self.assertEqual(lease["task_id"], "426")
        self.assertEqual(lease["commit_sha"], "queued-commit")

    def test_force_release_sweeps_next_queued_request(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        queued = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="queued-commit",
            queue_if_busy=True,
        )["queue"]

        with patch("dev_env_lease_manager.manager.NativeDevDeployer") as deployer_class:
            deployer_class.return_value.deploy.return_value = {
                "ok": True,
                "mode": "native",
                "source_sha": "queued-commit",
            }
            result = manager.force_release("operator", "free queue", lease_id=active["id"])

        self.assertTrue(result["ok"])
        self.assertEqual(result["queue_sweep"]["processed"][0]["queue"]["id"], queued["id"])
        self.assertEqual(manager.status()["environments"][0]["active_lease"]["task_id"], "426")

    def test_queue_callbacks_include_agent_hq_event_payloads(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        captured_events: list[dict[str, object]] = []
        captured_headers: list[dict[str, str]] = []

        class Response:
            status = 200

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"ok":true}'

        def fake_urlopen(request, timeout=15):
            if is_active_owner_request(request):
                return active_owner_response()
            captured_events.append(json.loads(request.data.decode("utf-8")))
            captured_headers.append({key.lower(): value for key, value in request.header_items()})
            return Response()

        with patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            queued = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit="queued-commit",
                dry_run=True,
                queue_if_busy=True,
                callback_url="http://agent-hq.local",
                callback_api_key="test-key",
            )["queue"]
            manager.force_release("operator", "free queue", lease_id=active["id"], sweep_queue_after_release=False)
            manager.sweep_deploy_queue("queue-worker", dry_run=True)

        self.assertEqual([event["event"] for event in captured_events], [
            "dev_deploy_queued",
            "dev_deploying",
            "deployed_for_qa",
        ])
        self.assertTrue(all(event["source"] == "dev_environment_lease_manager" for event in captured_events))
        self.assertTrue(all(event["task_id"] == "426" for event in captured_events))
        self.assertTrue(all(event["queue_id"] == queued["id"] for event in captured_events))
        self.assertTrue(all(event["environment_id"] == "agent-hq-dev" for event in captured_events))
        self.assertEqual(captured_events[-1]["commit_sha"], "queued-commit")
        self.assertEqual(captured_events[-1]["review_url"], "http://127.0.0.1:3510")
        self.assertEqual(captured_headers[0]["x-api-key"], "test-key")

        attempts = list(reversed(manager.callback_attempts(queue_id=queued["id"], limit=10)["callback_attempts"]))
        self.assertEqual([attempt["event"] for attempt in attempts], [
            "dev_deploy_queued",
            "dev_deploying",
            "deployed_for_qa",
        ])
        self.assertTrue(all(attempt["ok"] for attempt in attempts))
        self.assertTrue(all(attempt["auth_present"] for attempt in attempts))
        self.assertEqual(attempts[-1]["http_status"], 200)
        self.assertEqual(attempts[-1]["payload"]["event"], "deployed_for_qa")
        self.assertNotIn("callback_api_key", attempts[-1])
        self.assertNotIn("test-key", json.dumps(attempts))

    def test_queue_callback_uses_served_commit_when_queued_commit_missing(self) -> None:
        manager, tmp = self.make_manager(
            deploy_command="true",
            served_commit_command="printf resolved-commit",
        )
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        captured_events: list[dict[str, object]] = []

        class Response:
            status = 200

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"ok":true}'

        def fake_urlopen(request, timeout=15):
            if is_active_owner_request(request):
                return active_owner_response()
            captured_events.append(json.loads(request.data.decode("utf-8")))
            return Response()

        with patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            queued = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit=None,
                health_check=False,
                queue_if_busy=True,
                callback_url="http://agent-hq.local",
                callback_api_key="test-key",
            )["queue"]
            manager.force_release("operator", "free queue", lease_id=active["id"], sweep_queue_after_release=False)
            manager.sweep_deploy_queue("queue-worker", dry_run=False)

        self.assertEqual(captured_events[-1]["event"], "deployed_for_qa")
        self.assertEqual(captured_events[-1]["commit_sha"], "resolved-commit")
        active_lease = manager.status(environment_id="agent-hq-dev")["environments"][0]["active_lease"]
        self.assertEqual(active_lease["commit_sha"], "resolved-commit")
        attempts = manager.callback_attempts(queue_id=queued["id"], limit=10)["callback_attempts"]
        deployed_attempt = next(attempt for attempt in attempts if attempt["event"] == "deployed_for_qa")
        self.assertEqual(deployed_attempt["payload"]["commit_sha"], "resolved-commit")

    def test_callback_attempt_logs_http_failure(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.acquire("agent-hq-dev", "425", "anchor", commit="active")

        class Response:
            status = 503

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"ok":false,"error":"down"}'

        def fake_urlopen(request, timeout=15):
            if is_active_owner_request(request):
                return active_owner_response()
            return Response()

        with patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit="queued-commit",
                queue_if_busy=True,
                callback_url="http://agent-hq.local",
                callback_api_key="test-key",
            )

        self.assertTrue(result["ok"])
        attempt = manager.callback_attempts(queue_id=result["queue"]["id"])["callback_attempts"][0]
        self.assertFalse(attempt["ok"])
        self.assertEqual(attempt["outcome"], "http_failure")
        self.assertEqual(attempt["http_status"], 503)
        self.assertEqual(attempt["endpoint"], "http://agent-hq.local/api/v1/external/task-events")
        self.assertEqual(attempt["response_body"], '{"ok":false,"error":"down"}')

    def test_queue_callbacks_default_to_agent_hq_config_and_agent_env_key(self) -> None:
        manager, tmp = self.make_manager(agent_hq={
            "base_url": "http://agent-hq.local",
        })
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.acquire("agent-hq-dev", "425", "anchor", commit="active")
        captured_events: list[dict[str, object]] = []
        captured_urls: list[str] = []
        captured_headers: list[dict[str, str]] = []

        class Response:
            status = 200

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"ok":true}'

        def fake_urlopen(request, timeout=15):
            if is_active_owner_request(request):
                return active_owner_response()
            captured_events.append(json.loads(request.data.decode("utf-8")))
            captured_urls.append(request.full_url)
            captured_headers.append({key.lower(): value for key, value in request.header_items()})
            return Response()

        with patch.dict("os.environ", {"AGENT_HQ_MCP_API_KEY": "agent-key"}), \
             patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit="queued-commit",
                queue_if_busy=True,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["queue"]["callback_url"], "http://agent-hq.local")
        self.assertEqual(captured_events[0]["event"], "dev_deploy_queued")
        self.assertEqual(captured_urls[0], "http://agent-hq.local/api/v1/external/task-events")
        self.assertEqual(captured_headers[0]["x-api-key"], "agent-key")

    def test_direct_deploy_callbacks_include_agent_hq_event_payloads(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        captured_events: list[dict[str, object]] = []

        class Response:
            status = 200

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"ok":true}'

        def fake_urlopen(request, timeout=15):
            if is_active_owner_request(request):
                return active_owner_response()
            captured_events.append(json.loads(request.data.decode("utf-8")))
            return Response()

        with patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit="direct-commit",
                dry_run=True,
                callback_url="http://agent-hq.local",
                callback_api_key="test-key",
            )

        self.assertTrue(result["ok"])
        self.assertEqual([event["event"] for event in captured_events], ["dev_deploying", "deployed_for_qa"])
        self.assertTrue(all(event["source"] == "dev_environment_lease_manager" for event in captured_events))
        self.assertTrue(all(event["task_id"] == "426" for event in captured_events))
        self.assertTrue(all(event["queue_id"] == result["lease"]["id"] for event in captured_events))
        self.assertTrue(all(event["lease_id"] == result["lease"]["id"] for event in captured_events))
        self.assertTrue(all(event["environment_id"] == "agent-hq-dev" for event in captured_events))
        self.assertTrue(all(event["assigned_environment_id"] == "agent-hq-dev" for event in captured_events))
        self.assertTrue(all(event["requested_environment_id"] == "agent-hq-dev" for event in captured_events))
        attempts = list(reversed(manager.callback_attempts(lease_id=result["lease"]["id"], limit=10)["callback_attempts"]))
        self.assertEqual([attempt["event"] for attempt in attempts], ["dev_deploying", "deployed_for_qa"])

    def test_direct_deploy_failure_callback_includes_error_summary(self) -> None:
        manager, tmp = self.make_manager(deploy_command="python3 -c 'import sys; sys.exit(3)'")
        source = Path(tmp.name) / "source"
        source.mkdir()
        captured_events: list[dict[str, object]] = []

        class Response:
            status = 200

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"ok":true}'

        def fake_urlopen(request, timeout=15):
            if is_active_owner_request(request):
                return active_owner_response()
            captured_events.append(json.loads(request.data.decode("utf-8")))
            return Response()

        with patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                callback_url="http://agent-hq.local",
                callback_api_key="test-key",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["release_reason"], "deploy_failed")
        self.assertEqual([event["event"] for event in captured_events], ["dev_deploying", "deploy_failed"])
        self.assertIn("failed:", captured_events[-1]["message"])
        self.assertEqual(captured_events[-1]["error"]["stage"], "deploy")
        self.assertEqual(captured_events[-1]["error"]["result"]["deploy"]["returncode"], 3)
        self.assertEqual(result["callbacks"][-1]["payload"]["error"]["result"]["deploy"]["returncode"], 3)
        self.assertNotIn("callbacks", result["callbacks"][-1]["payload"]["error"]["result"])
        json.dumps(result, sort_keys=True)

    def test_queue_requires_agent_api_key_when_callbacks_are_configured(self) -> None:
        manager, tmp = self.make_manager(agent_hq={
            "base_url": "http://agent-hq.local",
        })
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.acquire("agent-hq-dev", "425", "anchor", commit="active")

        with patch.dict("os.environ", {}, clear=True):
            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit="queued-commit",
                queue_if_busy=True,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "callback_api_key_required")

    def test_deploy_rejects_when_active_owner_authorization_fails(self) -> None:
        manager, tmp = self.make_manager(agent_hq={
            "base_url": "http://agent-hq.local",
        })
        source = Path(tmp.name) / "source"
        source.mkdir()

        def fake_urlopen(request, timeout=15):
            self.assertTrue(is_active_owner_request(request))
            return MockHttpResponse({
                "ok": True,
                "task_id": 398,
                "authenticated_agent_id": 94,
                "authenticated_agent_slug": "cinder-backend",
                "active_instance_id": None,
                "is_active_owner": False,
                "reason": "task_has_no_active_instance",
            })

        with patch.dict("os.environ", {"AGENT_HQ_MCP_API_KEY": "agent-key"}), \
             patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "398",
                "cinder",
                str(source),
                branch="task-398",
                commit="wrong-commit",
                dry_run=True,
                queue_if_busy=True,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "deploy_authorization_failed")
        self.assertEqual(result["reason"], "task_has_no_active_instance")
        self.assertTrue(manager.status()["environments"][0]["available"])
        self.assertEqual(manager.queue_status("agent-hq-dev")["queue"], [])

    def test_deploy_records_active_owner_authorization_metadata(self) -> None:
        manager, tmp = self.make_manager(agent_hq={
            "base_url": "http://agent-hq.local",
        })
        source = Path(tmp.name) / "source"
        source.mkdir()

        def fake_urlopen(request, timeout=15):
            if is_active_owner_request(request):
                self.assertEqual(request.full_url, "http://agent-hq.local/api/v1/tasks/426/active-owner")
                return active_owner_response()
            return MockHttpResponse()

        with patch.dict("os.environ", {"AGENT_HQ_MCP_API_KEY": "agent-key"}), \
             patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            result = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit="direct-commit",
                dry_run=True,
            )

        self.assertTrue(result["ok"])
        metadata = result["lease"]["metadata"]
        authorization = metadata["active_owner_authorization"]
        self.assertEqual(authorization["task_id"], 426)
        self.assertEqual(authorization["authenticated_agent_id"], 94)
        self.assertEqual(authorization["active_instance_id"], 7001)
        self.assertEqual(authorization["active_instance_status"], "running")
        self.assertNotIn("agent-key", json.dumps(metadata))

    def test_queued_deploy_stores_authorization_and_sweep_does_not_revalidate(self) -> None:
        manager, tmp = self.make_manager(agent_hq={
            "base_url": "http://agent-hq.local",
        })
        source = Path(tmp.name) / "source"
        source.mkdir()
        active = manager.acquire("agent-hq-dev", "425", "anchor", commit="active")["lease"]
        active_owner_checks = 0

        def fake_urlopen(request, timeout=15):
            nonlocal active_owner_checks
            if is_active_owner_request(request):
                active_owner_checks += 1
                return active_owner_response()
            return MockHttpResponse()

        with patch.dict("os.environ", {"AGENT_HQ_MCP_API_KEY": "agent-key"}), \
             patch("dev_env_lease_manager.manager.urllib.request.urlopen", side_effect=fake_urlopen):
            queued = manager.lease_aware_deploy(
                "agent-hq-dev",
                "426",
                "cinder",
                str(source),
                branch="task-426",
                commit="queued-commit",
                dry_run=True,
                queue_if_busy=True,
            )["queue"]
            manager.force_release("operator", "free queue", lease_id=active["id"], sweep_queue_after_release=False)
            result = manager.sweep_deploy_queue("queue-worker", dry_run=True)

        self.assertEqual(active_owner_checks, 1)
        self.assertEqual(queued["metadata"]["active_owner_authorization"]["task_id"], 426)
        self.assertTrue(result["ok"])
        self.assertEqual(result["processed"][0]["queue"]["id"], queued["id"])

    def test_newer_same_task_queue_request_supersedes_existing_queued_request(self) -> None:
        manager, tmp = self.make_manager()
        source = Path(tmp.name) / "source"
        source.mkdir()
        manager.acquire("agent-hq-dev", "425", "anchor", commit="active")
        first = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="old-commit",
            queue_if_busy=True,
        )["queue"]
        second = manager.lease_aware_deploy(
            "agent-hq-dev",
            "426",
            "cinder",
            str(source),
            branch="task-426",
            commit="new-commit",
            queue_if_busy=True,
        )

        self.assertTrue(second["ok"])
        self.assertEqual(second["status"], "queued")
        self.assertEqual(second["superseded"][0]["id"], first["id"])
        queue = manager.queue_status("agent-hq-dev", include_terminal=True)["queue"]
        by_id = {entry["id"]: entry for entry in queue}
        self.assertEqual(by_id[first["id"]]["status"], "superseded")
        self.assertEqual(by_id[second["queue"]["id"]]["status"], "queued")
        self.assertEqual(by_id[second["queue"]["id"]]["commit_sha"], "new-commit")

    def test_deploy_failure_releases_environment(self) -> None:
        manager, tmp = self.make_manager(deploy_command="python3 -c 'import sys; sys.exit(3)'")
        source = Path(tmp.name) / "source"
        source.mkdir()

        result = manager.lease_aware_deploy("agent-hq-dev", "426", "cinder", str(source))

        self.assertTrue(result["ok"])
        self.assertEqual(result["release_reason"], "deploy_failed")
        self.assertTrue(manager.status()["environments"][0]["available"])

    def test_command_deploy_timeout_returns_structured_error(self) -> None:
        manager, tmp = self.make_manager(deploy_command="python3 -c 'import time; time.sleep(2)'")
        source = Path(tmp.name) / "source"
        source.mkdir()

        result = manager.lease_aware_deploy("agent-hq-dev", "426", "cinder", str(source), timeout_seconds=1)

        self.assertTrue(result["ok"])
        self.assertEqual(result["release_reason"], "deploy_failed")
        self.assertEqual(result["deploy"]["error"], "command_timed_out")
        self.assertEqual(result["deploy"]["timeout_seconds"], 1)

    def test_lease_aware_deploy_uses_native_mcp_deploy_when_configured(self) -> None:
        manager, tmp = self.make_manager(
            deploy_command="python3 -c 'raise SystemExit(99)'",
            metadata={"deploy_mode": "native"},
        )
        source = Path(tmp.name) / "source"
        source.mkdir()

        with patch("dev_env_lease_manager.manager.NativeDevDeployer") as deployer_class:
            deployer_class.return_value.deploy.return_value = {
                "ok": True,
                "mode": "native",
                "source_sha": "abc123",
            }
            result = manager.lease_aware_deploy("agent-hq-dev", "426", "cinder", str(source), commit="abc123")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "deployed_for_qa")
        self.assertEqual(result["deploy"]["mode"], "native")
        deployer_class.return_value.deploy.assert_called_once()


if __name__ == "__main__":
    unittest.main()
