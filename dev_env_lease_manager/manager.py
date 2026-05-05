from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import shlex
import sqlite3
import subprocess
from typing import Any, Dict, Iterable, Optional
from uuid import uuid4

from .config import LeaseManagerConfig
from .db import connect, sync_environments
from .deploy import NativeDeployError, NativeDevDeployer
from .models import ACTIVE_STATUSES, ALLOWED_TRANSITIONS, RELEASE_REASON_ALLOWED_FROM, RELEASE_REASON_TO_STATUS


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    data = dict(row)
    for key in ("tags_json", "metadata_json", "payload_json"):
        if key in data:
            out_key = key[:-5] if key.endswith("_json") else key
            try:
                data[out_key] = json.loads(data[key] or "{}")
            except Exception:
                data[out_key] = {} if key != "tags_json" else []
            del data[key]
    return data


class LeaseManager:
    def __init__(self, config: LeaseManagerConfig):
        self.config = config
        self.conn = connect(config.data_path)
        sync_environments(self.conn, config, utc_now())

    def close(self) -> None:
        self.conn.close()

    def _environment(self, environment_id: str) -> Optional[Dict[str, Any]]:
        return row_to_dict(self.conn.execute("SELECT * FROM environments WHERE id = ?", (environment_id,)).fetchone())

    def _active_lease(self, environment_id: str) -> Optional[Dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        return row_to_dict(self.conn.execute(
            f"""
            SELECT * FROM leases
            WHERE environment_id = ?
              AND released_at IS NULL
              AND status IN ({placeholders})
            ORDER BY acquired_at DESC
            LIMIT 1
            """,
            (environment_id, *sorted(ACTIVE_STATUSES)),
        ).fetchone())

    def _lease(self, lease_id: str) -> Optional[Dict[str, Any]]:
        return row_to_dict(self.conn.execute("SELECT * FROM leases WHERE id = ?", (lease_id,)).fetchone())

    def _event(self, lease_id: Optional[str], environment_id: str, task_id: Optional[str], actor: str,
               event_type: str, from_status: Optional[str], to_status: Optional[str],
               release_reason: Optional[str] = None, message: Optional[str] = None,
               payload: Optional[Dict[str, Any]] = None) -> None:
        self.conn.execute(
            """
            INSERT INTO lease_events (
              lease_id, environment_id, task_id, actor, event_type, from_status,
              to_status, release_reason, message, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lease_id,
                environment_id,
                task_id,
                actor,
                event_type,
                from_status,
                to_status,
                release_reason,
                message,
                json.dumps(payload or {}, sort_keys=True),
                utc_now(),
            ),
        )

    def _busy(self, env: Dict[str, Any], lease: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ok": False,
            "status": "blocked",
            "error": "environment_busy",
            "environment": env,
            "lease": lease,
            "owner": {
                "task_id": lease.get("task_id"),
                "agent_id": lease.get("agent_id"),
                "agent_name": lease.get("agent_name"),
                "branch": lease.get("branch"),
                "commit": lease.get("commit_sha"),
                "lease_id": lease.get("id"),
                "status": lease.get("status"),
            },
            "next_action": "Do not deploy or mutate the shared dev checkout. Post blocked/waiting with this lease id, or ask an operator to force release if the lease is stale.",
        }

    def acquire(self, environment_id: str, task_id: str, actor: str, agent_id: Optional[str] = None,
                agent_name: Optional[str] = None, branch: Optional[str] = None,
                commit: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not task_id:
            return {"ok": False, "error": "task_id is required"}
        if not actor:
            return {"ok": False, "error": "actor is required"}
        env = self._environment(environment_id)
        if not env:
            return {"ok": False, "error": "environment_not_found", "environment_id": environment_id}

        now = utc_now()
        lease_id = str(uuid4())
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            active = self._active_lease(environment_id)
            if active:
                self.conn.execute("ROLLBACK")
                return self._busy(env, active)
            self.conn.execute(
                """
                INSERT INTO leases (
                  id, environment_id, task_id, agent_id, agent_name, branch, commit_sha,
                  status, acquired_at, heartbeat_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'acquired', ?, ?, ?)
                """,
                (
                    lease_id,
                    environment_id,
                    str(task_id),
                    str(agent_id) if agent_id is not None else None,
                    agent_name,
                    branch,
                    commit,
                    now,
                    now,
                    json.dumps(metadata or {}, sort_keys=True),
                ),
            )
            self._event(lease_id, environment_id, str(task_id), actor, "acquire", None, "acquired", payload={"commit": commit, "branch": branch})
            self.conn.execute("COMMIT")
        except sqlite3.IntegrityError:
            self.conn.execute("ROLLBACK")
            active = self._active_lease(environment_id)
            return self._busy(env, active or {})

        return {"ok": True, "status": "acquired", "environment": env, "lease": self._lease(lease_id)}

    def transition(self, lease_id: str, event_type: str, actor: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if event_type not in ALLOWED_TRANSITIONS:
            return {"ok": False, "error": "unknown_transition", "transition": event_type}
        if not actor:
            return {"ok": False, "error": "actor is required"}
        lease = self._lease(lease_id)
        if not lease:
            return {"ok": False, "error": "lease_not_found", "lease_id": lease_id}

        allowed_from, to_status = ALLOWED_TRANSITIONS[event_type]
        from_status = str(lease["status"])
        if from_status not in allowed_from:
            return {
                "ok": False,
                "error": "invalid_transition",
                "lease_id": lease_id,
                "from_status": from_status,
                "to_status": to_status,
                "allowed_from": sorted(allowed_from),
            }

        now = utc_now()
        fields = {"status": to_status, "heartbeat_at": now}
        if to_status == "deploying":
            fields["deploying_at"] = now
        if to_status == "deployed_for_qa":
            fields["deployed_at"] = now
        if to_status == "prod_deploying":
            fields["prod_deploying_at"] = now
        assignments = ", ".join(f"{key} = ?" for key in fields)
        self.conn.execute(f"UPDATE leases SET {assignments} WHERE id = ?", (*fields.values(), lease_id))
        self._event(lease_id, lease["environment_id"], lease["task_id"], actor, event_type, from_status, to_status, payload=payload)
        updated = self._lease(lease_id)
        return {"ok": True, "status": to_status, "lease": updated, "agent_hq_evidence": self.review_evidence(updated)}

    def heartbeat(self, lease_id: str, actor: str) -> Dict[str, Any]:
        lease = self._lease(lease_id)
        if not lease:
            return {"ok": False, "error": "lease_not_found", "lease_id": lease_id}
        if lease["status"] not in ACTIVE_STATUSES:
            return {"ok": False, "error": "lease_not_active", "lease": lease}
        now = utc_now()
        self.conn.execute("UPDATE leases SET heartbeat_at = ? WHERE id = ?", (now, lease_id))
        self._event(lease_id, lease["environment_id"], lease["task_id"], actor, "heartbeat", lease["status"], lease["status"])
        return {"ok": True, "lease": self._lease(lease_id)}

    def release(self, lease_id: str, actor: str, reason: str, message: Optional[str] = None) -> Dict[str, Any]:
        if reason not in RELEASE_REASON_TO_STATUS:
            return {"ok": False, "error": "invalid_release_reason", "allowed_reasons": sorted(RELEASE_REASON_TO_STATUS)}
        if not actor:
            return {"ok": False, "error": "actor is required"}
        lease = self._lease(lease_id)
        if not lease:
            return {"ok": False, "error": "lease_not_found", "lease_id": lease_id}
        if lease["status"] not in ACTIVE_STATUSES:
            return {"ok": False, "error": "lease_not_active", "lease": lease}
        allowed_from = RELEASE_REASON_ALLOWED_FROM[reason]
        if lease["status"] not in allowed_from:
            return {
                "ok": False,
                "error": "invalid_release_transition",
                "lease_id": lease_id,
                "from_status": lease["status"],
                "release_reason": reason,
                "allowed_from": sorted(allowed_from),
            }

        to_status = RELEASE_REASON_TO_STATUS[reason]
        from_status = lease["status"]
        now = utc_now()
        self.conn.execute(
            "UPDATE leases SET status = ?, release_reason = ?, released_at = ?, heartbeat_at = ? WHERE id = ?",
            (to_status, reason, now, now, lease_id),
        )
        self._event(lease_id, lease["environment_id"], lease["task_id"], actor, "release", from_status, to_status, reason, message)
        updated = self._lease(lease_id)
        return {"ok": True, "status": to_status, "lease": updated, "release_reason": reason, "agent_hq_note": self.release_note(updated, message)}

    def force_release(self, actor: str, reason: str, lease_id: Optional[str] = None,
                      environment_id: Optional[str] = None) -> Dict[str, Any]:
        if not actor:
            return {"ok": False, "error": "actor is required"}
        if not reason:
            return {"ok": False, "error": "force release reason is required"}
        lease = self._lease(lease_id) if lease_id else (self._active_lease(environment_id or "") if environment_id else None)
        if not lease:
            return {"ok": False, "error": "active_lease_not_found"}
        from_status = lease["status"]
        now = utc_now()
        self.conn.execute(
            "UPDATE leases SET status = 'force_released', release_reason = ?, released_at = ?, heartbeat_at = ? WHERE id = ?",
            (reason, now, now, lease["id"]),
        )
        self._event(lease["id"], lease["environment_id"], lease["task_id"], actor, "force_release", from_status, "force_released", reason)
        return {"ok": True, "status": "force_released", "lease": self._lease(lease["id"]), "release_reason": reason}

    def status(self, environment_id: Optional[str] = None, include_events: bool = False) -> Dict[str, Any]:
        params: Iterable[Any] = (environment_id,) if environment_id else ()
        where = "WHERE id = ?" if environment_id else ""
        envs = [row_to_dict(r) for r in self.conn.execute(f"SELECT * FROM environments {where} ORDER BY id", tuple(params)).fetchall()]
        now_dt = parse_time(utc_now())
        output = []
        for env in envs:
            if not env:
                continue
            lease = self._active_lease(env["id"])
            stale = False
            if lease and lease.get("heartbeat_at"):
                stale = (now_dt - parse_time(lease["heartbeat_at"])).total_seconds() > int(env["stale_after_seconds"])
            item = {"environment": env, "available": lease is None, "active_lease": lease, "stale": stale}
            if include_events and lease:
                item["events"] = self.events(lease["id"])["events"]
            output.append(item)
        return {"ok": True, "environments": output}

    def events(self, lease_id: str) -> Dict[str, Any]:
        rows = self.conn.execute("SELECT * FROM lease_events WHERE lease_id = ? ORDER BY id", (lease_id,)).fetchall()
        return {"ok": True, "events": [row_to_dict(row) for row in rows]}

    def sweep_stale(self, actor: str) -> Dict[str, Any]:
        if not actor:
            return {"ok": False, "error": "actor is required"}
        status = self.status()["environments"]
        marked = []
        for item in status:
            lease = item.get("active_lease")
            if item.get("stale") and lease and lease["status"] != "stale":
                self.conn.execute("UPDATE leases SET status = 'stale' WHERE id = ?", (lease["id"],))
                self._event(lease["id"], lease["environment_id"], lease["task_id"], actor, "mark_stale", lease["status"], "stale")
                marked.append(self._lease(lease["id"]))
        return {"ok": True, "marked_stale": marked}

    def validate_qa(self, task_id: str, commit: str, environment_id: Optional[str] = None,
                    lease_id: Optional[str] = None) -> Dict[str, Any]:
        lease = self._lease(lease_id) if lease_id else (self._active_lease(environment_id or "") if environment_id else None)
        if not lease:
            return {"ok": False, "error": "active_lease_not_found"}
        errors = []
        if str(lease.get("task_id")) != str(task_id):
            errors.append(f"lease task_id {lease.get('task_id')} does not match QA task_id {task_id}")
        if lease.get("commit_sha") and commit and str(lease.get("commit_sha")) != str(commit):
            errors.append(f"lease commit {lease.get('commit_sha')} does not match QA commit {commit}")
        if lease.get("status") not in {"deployed_for_qa", "prod_deploying"}:
            errors.append(f"lease status {lease.get('status')} is not ready for QA")
        if errors:
            return {
                "ok": False,
                "error": "environment_integrity_failure",
                "errors": errors,
                "lease": lease,
                "next_action": "Do not QA this environment. Treat commit mismatch as environment integrity failure, not product failure.",
            }
        return {"ok": True, "lease": lease}

    def review_evidence(self, lease: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not lease:
            return None
        env = self._environment(lease["environment_id"]) or {}
        return {
            "review_branch": lease.get("branch"),
            "review_commit": lease.get("commit_sha"),
            "review_url": env.get("base_url"),
            "summary": f"Dev environment lease {lease['id']} owns {lease['environment_id']} for task {lease['task_id']}.",
            "lease": {
                "lease_id": lease["id"],
                "environment_id": lease["environment_id"],
                "task_id": lease["task_id"],
                "agent_id": lease.get("agent_id"),
                "agent_name": lease.get("agent_name"),
                "branch": lease.get("branch"),
                "commit": lease.get("commit_sha"),
                "status": lease.get("status"),
            },
        }

    def release_note(self, lease: Optional[Dict[str, Any]], message: Optional[str] = None) -> Optional[str]:
        if not lease:
            return None
        lines = [
            "Dev environment lease released\n"
            f"Lease: {lease['id']}\n"
            f"Environment: {lease['environment_id']}\n"
            f"Task: {lease['task_id']}\n"
            f"Reason: {lease.get('release_reason')}\n"
            f"Status: {lease.get('status')}"
        ]
        if message:
            lines.append(f"Message: {message}")
        return "\n".join(lines)

    def lease_aware_deploy(self, environment_id: str, task_id: str, actor: str, source_repo_path: str,
                           agent_id: Optional[str] = None, agent_name: Optional[str] = None,
                           branch: Optional[str] = None, commit: Optional[str] = None,
                           services: str = "both", health_check: bool = True,
                           dry_run: bool = False, timeout_seconds: int = 1800) -> Dict[str, Any]:
        env = self._environment(environment_id)
        if not env:
            return {"ok": False, "error": "environment_not_found", "environment_id": environment_id}
        if not os.path.isdir(source_repo_path):
            return {"ok": False, "error": "source_repo_path_not_found", "source_repo_path": source_repo_path}
        acquire = self.acquire(environment_id, task_id, actor, agent_id, agent_name, branch, commit, {"source_repo_path": source_repo_path})
        if not acquire.get("ok"):
            return acquire
        lease_id = acquire["lease"]["id"]
        deploying = self.transition(lease_id, "mark_deploying", actor)
        if not deploying.get("ok"):
            return deploying
        if dry_run:
            deployed = self.transition(lease_id, "mark_deployed_for_qa", actor, {"dry_run": True, "served_commit": commit})
            deployed["deploy"] = {"dry_run": True}
            return deployed

        metadata = env.get("metadata") or {}
        deploy_mode = str(metadata.get("deploy_mode") or ("command" if env.get("deploy_command") else "native")).strip().lower()
        if deploy_mode == "command":
            deploy_payload = self._command_deploy(env, lease_id, environment_id, task_id, actor, source_repo_path, commit, services, health_check, timeout_seconds)
            if not deploy_payload.get("ok"):
                released = self.release(lease_id, actor, "deploy_failed", deploy_payload.get("message") or deploy_payload.get("stderr") or "deploy command failed")
                released["deploy"] = deploy_payload
                return released
        else:
            try:
                deploy_payload = NativeDevDeployer().deploy(
                    env,
                    source_repo_path,
                    services=services,
                    health_check=health_check,
                    expected_commit=commit,
                    timeout_seconds=timeout_seconds,
                )
            except NativeDeployError as exc:
                deploy_payload = exc.payload()
                released = self.release(lease_id, actor, "deploy_failed", deploy_payload.get("error", "native deploy failed"))
                released["deploy"] = deploy_payload
                return released

        served_commit = commit
        served_cmd = env.get("served_commit_command")
        if served_cmd:
            served = subprocess.run(served_cmd, shell=True, text=True, capture_output=True, timeout=60)
            if served.returncode != 0:
                released = self.release(lease_id, actor, "deploy_failed", served.stderr[-1000:] or "served commit command failed")
                released["deploy"] = deploy_payload
                return released
            served_commit = served.stdout.strip()
        if commit and served_commit and commit != served_commit:
            released = self.release(lease_id, actor, "deploy_failed", f"served commit {served_commit} did not match expected {commit}")
            released["deploy"] = deploy_payload
            return released
        deployed = self.transition(lease_id, "mark_deployed_for_qa", actor, {"served_commit": served_commit, "deploy": deploy_payload})
        deployed["deploy"] = deploy_payload
        return deployed

    def _command_deploy(self, env: Dict[str, Any], lease_id: str, environment_id: str, task_id: str,
                        actor: str, source_repo_path: str, commit: Optional[str], services: str,
                        health_check: bool, timeout_seconds: int) -> Dict[str, Any]:
        command = env.get("deploy_command")
        if not command:
            return {"ok": False, "message": "No deploy_command configured"}
        proc_env = os.environ.copy()
        proc_env.update({
            "repo_path": source_repo_path,
            "REPO_PATH": source_repo_path,
            "DEV_LEASE_ID": lease_id,
            "DEV_LEASE_ENVIRONMENT_ID": environment_id,
            "DEV_LEASE_TASK_ID": str(task_id),
            "DEV_LEASE_ACTOR": actor,
            "DEV_LEASE_COMMIT": commit or "",
            "services": services,
            "SERVICES": services,
            "HEALTH_CHECK": "true" if health_check else "false",
        })
        if env.get("repo_path"):
            proc_env["dev_repo_path"] = env["repo_path"]
            proc_env["DEV_REPO_PATH"] = env["repo_path"]

        completed = subprocess.run(
            command if isinstance(command, str) else shlex.join(command),
            shell=True,
            cwd=source_repo_path,
            env=proc_env,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
        return {
            "ok": completed.returncode == 0,
            "mode": "command",
            "returncode": completed.returncode,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
        }
