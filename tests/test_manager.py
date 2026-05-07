from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from dev_env_lease_manager.config import load_config
from dev_env_lease_manager.manager import LeaseManager


class ManagerTestCase(unittest.TestCase):
    def make_manager(self, stale_after_seconds: int = 3600, deploy_command: str | None = None,
                     served_commit_command: str | None = None,
                     metadata: dict[str, object] | None = None,
                     agent_hq: dict[str, object] | None = None) -> tuple[LeaseManager, tempfile.TemporaryDirectory[str]]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        payload = {
            "data_path": str(Path(tmp.name) / "state.sqlite3"),
            "agent_hq": agent_hq or {},
            "environments": [
                {
                    "id": "agent-hq-dev",
                    "label": "Agent HQ Dev",
                    "base_url": "http://127.0.0.1:3510",
                    "repo_path": str(Path(tmp.name) / "dev"),
                    "stale_after_seconds": stale_after_seconds,
                    "deploy_command": deploy_command,
                    "served_commit_command": served_commit_command,
                    "metadata": metadata or {},
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

        with patch("dev_env_lease_manager.manager.urllib.request.urlopen", return_value=Response()):
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
