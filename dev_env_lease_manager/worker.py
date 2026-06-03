"""Background deploy worker.

Native dev deploys take minutes — longer than the MCP/OpenClaw tool-call boundary.
Running them inline inside the tool call means a caller timeout or session end kills
the deploy mid-flight, leaving the lease/queue row wedged in ``deploying`` with no
terminal callback. This worker decouples execution from the caller: it drains the
shared ``deploy_queue`` off-session, heartbeats progress, and writes the terminal
status + callback itself. A reconciler pass on each tick fails any orphaned row
(including one left by a previous crash of this worker).

Intended to run under pm2 alongside ``agent-hq-dev-api`` / ``agent-hq-dev-ui``.
"""
from __future__ import annotations

import argparse
import os
import signal
import socket
import time
from typing import Any, Dict, Optional

from .config import default_config_path, load_config
from .manager import LeaseManager


DEFAULT_DEPLOY_TIMEOUT_SECONDS = 1800


def default_worker_id() -> str:
    return f"deploy-worker:{socket.gethostname()}:{os.getpid()}"


class DeployWorker:
    def __init__(self, manager: LeaseManager, worker_id: Optional[str] = None,
                 poll_interval_seconds: Optional[int] = None,
                 timeout_seconds: int = DEFAULT_DEPLOY_TIMEOUT_SECONDS):
        self.manager = manager
        self.worker_id = worker_id or default_worker_id()
        configured_interval = getattr(manager.config, "deploy_worker_poll_interval_seconds", None)
        self.poll_interval_seconds = int(poll_interval_seconds or configured_interval or 5)
        self.timeout_seconds = int(timeout_seconds)
        self._stop = False

    def request_stop(self, *_args: Any) -> None:
        self._stop = True

    def reconcile(self) -> Dict[str, Any]:
        return self.manager.reconcile_stuck_deploys(self.worker_id)

    def run_once(self) -> Dict[str, Any]:
        """Reconcile orphans, then claim and run at most one deploy.

        Returns a structured summary so the loop (and tests) can see what happened.
        """
        reconciled = self.reconcile()
        claim = self.manager.claim_next_deploy(self.worker_id, self.worker_id, worker_pid=os.getpid())
        if not claim.get("ok"):
            return {"ok": False, "reconciled": reconciled, "claim": claim}
        if not claim.get("claimed"):
            return {"ok": True, "reconciled": reconciled, "deployed": False, "reason": claim.get("reason")}

        env = claim["environment"]
        queue = claim["queue"]
        lease = claim["lease"]
        queue_id = queue["id"]

        def on_phase(phase: str, _detail: Optional[Dict[str, Any]] = None) -> None:
            self.manager.update_deploy_heartbeat(queue_id, phase)

        finished = self.manager.execute_claimed_deploy(
            env,
            queue,
            lease,
            self.worker_id,
            timeout_seconds=self.timeout_seconds,
            on_phase=on_phase,
        )
        return {
            "ok": True,
            "reconciled": reconciled,
            "deployed": True,
            "queue_id": queue_id,
            "result": finished,
        }

    def run_forever(self) -> int:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        print(f"[deploy-worker] started worker_id={self.worker_id} poll={self.poll_interval_seconds}s", flush=True)
        while not self._stop:
            try:
                summary = self.run_once()
            except Exception as exc:  # never let one bad tick kill the worker
                print(f"[deploy-worker] tick error: {exc}", flush=True)
                summary = {"ok": False, "error": str(exc)}
            # When we deployed something there may be more queued work; loop again
            # immediately. Otherwise idle for the poll interval, but stay responsive
            # to SIGTERM by waking up every second.
            if summary.get("deployed"):
                continue
            for _ in range(self.poll_interval_seconds):
                if self._stop:
                    break
                time.sleep(1)
        print("[deploy-worker] stopped", flush=True)
        return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Agent HQ dev-environment deploy worker")
    parser.add_argument("--config", default=default_config_path())
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--poll-interval", type=int, default=None)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_DEPLOY_TIMEOUT_SECONDS)
    parser.add_argument("--once", action="store_true", help="Run a single tick and exit (for diagnostics)")
    args = parser.parse_args(argv)

    manager = LeaseManager(load_config(args.config))
    worker = DeployWorker(
        manager,
        worker_id=args.worker_id,
        poll_interval_seconds=args.poll_interval,
        timeout_seconds=args.timeout_seconds,
    )
    try:
        if args.once:
            summary = worker.run_once()
            print(f"[deploy-worker] once: {summary.get('reason') or ('deployed' if summary.get('deployed') else 'idle')}", flush=True)
            return 0
        return worker.run_forever()
    finally:
        manager.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
