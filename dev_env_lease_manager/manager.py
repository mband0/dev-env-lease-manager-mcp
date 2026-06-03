from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import shlex
import sqlite3
import subprocess
from typing import Any, Dict, Iterable, Optional
import urllib.error
import urllib.parse
import urllib.request
from uuid import uuid4

from .config import LeaseManagerConfig
from .db import connect, sync_environments
from .deploy import (
    DEPLOY_FAILURE_EVENTS,
    NativeDeployError,
    NativeDevDeployer,
    commit_matches_expected,
    inspect_native_deploy_lock,
    normalize_database_policy,
    sweep_native_deploy_lock,
)
from .models import (
    ACTIVE_STATUSES,
    ALLOWED_TRANSITIONS,
    QUEUE_ACTIVE_STATUSES,
    QUEUE_TERMINAL_STATUSES,
    RELEASE_REASON_ALLOWED_FROM,
    RELEASE_REASON_TO_STATUS,
)


CALLBACK_SOURCE = "dev_environment_lease_manager"
QA_FIXTURE_TASK_ID_PREFIX = "999"
QA_FIXTURE_TASK_ID_LENGTH = 6
PROTECTED_BRANCH_REFS = {
    "main",
    "master",
    "origin/main",
    "origin/master",
    "refs/heads/main",
    "refs/heads/master",
    "refs/remotes/origin/main",
    "refs/remotes/origin/master",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    data = dict(row)
    for key in ("tags_json", "metadata_json", "payload_json", "error_json"):
        if key in data:
            out_key = key[:-5] if key.endswith("_json") else key
            try:
                data[out_key] = json.loads(data[key] or "{}")
            except Exception:
                data[out_key] = {} if key != "tags_json" else []
            del data[key]
    return data


def is_qa_fixture_task_id(task_id: Any) -> bool:
    value = str(task_id).strip()
    return (
        len(value) == QA_FIXTURE_TASK_ID_LENGTH
        and value.isascii()
        and value.isdigit()
        and value.startswith(QA_FIXTURE_TASK_ID_PREFIX)
    )


def qa_fixture_task_rejection(task_id: Any) -> Dict[str, Any]:
    task_id_value = str(task_id).strip()
    return {
        "ok": False,
        "error": "qa_fixture_task_not_deployable",
        "task_id": task_id_value,
        "message": "Task IDs in the 999XXX range are QA fixtures and cannot be deployed.",
        "next_action": "Use a real Agent HQ task id before acquiring or deploying a dev environment lease.",
    }


class LeaseManager:
    def __init__(self, config: LeaseManagerConfig):
        self.config = config
        self.conn = connect(config.data_path)
        sync_environments(self.conn, config, utc_now())

    def close(self) -> None:
        self.conn.close()

    def _environment(self, environment_id: str) -> Optional[Dict[str, Any]]:
        return row_to_dict(self.conn.execute("SELECT * FROM environments WHERE id = ?", (environment_id,)).fetchone())

    def _environments(self) -> list[Dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM environments ORDER BY id").fetchall()
        return [env for env in (row_to_dict(row) for row in rows) if env]

    def _environment_tags(self, env: Optional[Dict[str, Any]]) -> set[str]:
        tags = (env or {}).get("tags") or []
        return {str(tag) for tag in tags if str(tag).strip()}

    def _environment_selector(self, env: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        tags = sorted(self._environment_tags(env))
        return {"tags": tags} if tags else {}

    def _candidate_environments_for_request(self, requested_environment_id: str) -> list[Dict[str, Any]]:
        requested = self._environment(requested_environment_id)
        if not requested:
            return []
        requested_tags = self._environment_tags(requested)
        candidates = []
        for env in self._environments():
            env_id = str(env["id"])
            if env_id == requested_environment_id:
                candidates.append(env)
                continue
            if requested_tags and requested_tags.issubset(self._environment_tags(env)):
                candidates.append(env)
        return sorted(
            candidates,
            key=lambda env: (str(env["id"]) != requested_environment_id, str(env["id"])),
        )

    def _available_environment_for_request(self, requested_environment_id: str) -> Optional[Dict[str, Any]]:
        for env in self._candidate_environments_for_request(requested_environment_id):
            deploy_lock = inspect_native_deploy_lock(env)
            if not self._active_lease(str(env["id"])) and not (deploy_lock and deploy_lock.get("blocks_deploy")):
                return env
        return None

    def _request_metadata(self, requested_env: Dict[str, Any], requested_environment_id: str,
                          source_repo_path: str, assigned_environment_id: Optional[str] = None,
                          extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {
            "source_repo_path": source_repo_path,
            "requested_environment_id": requested_environment_id,
        }
        selector = self._environment_selector(requested_env)
        if selector:
            metadata["environment_selector"] = selector
        if assigned_environment_id:
            metadata["assigned_environment_id"] = assigned_environment_id
        metadata.update(extra or {})
        return metadata

    def _queue_requested_environment_id(self, queue: Dict[str, Any]) -> str:
        metadata = queue.get("metadata") or {}
        return str(metadata.get("requested_environment_id") or queue.get("environment_id"))

    def _queue_selector_tags(self, queue: Dict[str, Any]) -> set[str]:
        metadata = queue.get("metadata") or {}
        selector = metadata.get("environment_selector") or {}
        if isinstance(selector, dict):
            tags = selector.get("tags") or []
            return {str(tag) for tag in tags if str(tag).strip()}
        return set()

    def _queue_candidate_environment_ids(self, queue: Dict[str, Any]) -> set[str]:
        requested_environment_id = self._queue_requested_environment_id(queue)
        return {str(env["id"]) for env in self._candidate_environments_for_request(requested_environment_id)}

    def _queue_matches_environment(self, queue: Dict[str, Any], env: Dict[str, Any]) -> bool:
        env_id = str(env["id"])
        requested_environment_id = self._queue_requested_environment_id(queue)
        if env_id == requested_environment_id:
            return True
        selector_tags = self._queue_selector_tags(queue)
        if not selector_tags:
            selector_tags = self._environment_tags(self._environment(requested_environment_id))
        return bool(selector_tags and selector_tags.issubset(self._environment_tags(env)))

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

    def _lease_is_stale(self, lease: Optional[Dict[str, Any]], env: Optional[Dict[str, Any]],
                        now_dt: Optional[datetime] = None) -> bool:
        if not lease or not env:
            return False
        if lease.get("status") == "stale":
            return True
        heartbeat_at = lease.get("heartbeat_at")
        if not heartbeat_at:
            return False
        try:
            heartbeat_dt = parse_time(str(heartbeat_at))
        except ValueError:
            return False
        now_value = now_dt or parse_time(utc_now())
        return (now_value - heartbeat_dt).total_seconds() > int(env["stale_after_seconds"])

    def _lease(self, lease_id: str) -> Optional[Dict[str, Any]]:
        return row_to_dict(self.conn.execute("SELECT * FROM leases WHERE id = ?", (lease_id,)).fetchone())

    def _callback_url(self, callback_url: Optional[str]) -> Optional[str]:
        return callback_url or self.config.agent_hq_base_url

    def _callback_api_key(self, callback_api_key: Optional[str]) -> Optional[str]:
        return callback_api_key or os.environ.get("AGENT_HQ_MCP_API_KEY")

    def _queue_row(self, queue_id: str) -> Optional[Dict[str, Any]]:
        return row_to_dict(self.conn.execute("SELECT * FROM deploy_queue WHERE id = ?", (queue_id,)).fetchone())

    def _public_queue(self, queue: Dict[str, Any]) -> Dict[str, Any]:
        public = dict(queue)
        public.pop("callback_api_key", None)
        metadata = public.get("metadata") or {}
        public["requested_environment_id"] = metadata.get("requested_environment_id") or public.get("environment_id")
        assigned_environment_id = metadata.get("assigned_environment_id")
        if public.get("status") != "queued" and assigned_environment_id is None:
            assigned_environment_id = public.get("environment_id")
        public["assigned_environment_id"] = assigned_environment_id
        return public

    def _owner_from_lease(self, lease: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not lease:
            return None
        return {
            "task_id": lease.get("task_id"),
            "agent_id": lease.get("agent_id"),
            "agent_name": lease.get("agent_name"),
            "branch": lease.get("branch"),
            "commit": lease.get("commit_sha"),
            "lease_id": lease.get("id"),
            "status": lease.get("status"),
        }

    def _git(self, repo_path: str, *args: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", repo_path, *args],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )

    def _git_payload(self, completed: subprocess.CompletedProcess[str]) -> Dict[str, Any]:
        return {
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }

    def _add_cleanup_check(self, checks: list[Dict[str, Any]], name: str, ok: bool,
                           detail: Optional[Dict[str, Any]] = None) -> None:
        check: Dict[str, Any] = {"name": name, "ok": bool(ok)}
        if detail is not None:
            check["detail"] = detail
        checks.append(check)

    def _normalize_cleanup_branch(self, source_branch: str, remote: str) -> tuple[str, str]:
        raw_branch = str(source_branch or "").strip()
        branch = raw_branch
        if branch.startswith("refs/heads/"):
            branch = branch[len("refs/heads/"):]
        remote_prefix = f"{remote}/"
        remote_ref_prefix = f"refs/remotes/{remote}/"
        if branch.startswith(remote_ref_prefix):
            branch = branch[len(remote_ref_prefix):]
        elif branch.startswith(remote_prefix):
            branch = branch[len(remote_prefix):]
        return raw_branch, branch

    def _branch_is_protected(self, raw_branch: str, normalized_branch: str, remote: str) -> bool:
        candidates = {
            raw_branch,
            normalized_branch,
            f"{remote}/{normalized_branch}",
            f"refs/heads/{normalized_branch}",
            f"refs/remotes/{remote}/{normalized_branch}",
        }
        return any(candidate in PROTECTED_BRANCH_REFS for candidate in candidates)

    def _resolve_commit_for_cleanup(self, repo_path: str, rev: str) -> Dict[str, Any]:
        completed = self._git(repo_path, "rev-parse", "--verify", f"{rev}^{{commit}}")
        payload = self._git_payload(completed)
        payload["ok"] = completed.returncode == 0 and bool(payload["stdout"])
        if payload["ok"]:
            payload["commit"] = payload["stdout"].splitlines()[-1].strip()
        return payload

    def _commit_is_ancestor(self, repo_path: str, ancestor: str, descendant: str) -> Dict[str, Any]:
        completed = self._git(repo_path, "merge-base", "--is-ancestor", ancestor, descendant)
        payload = self._git_payload(completed)
        payload["ok"] = completed.returncode == 0
        return payload

    def _local_branch_status(self, repo_path: str, branch: str) -> Dict[str, Any]:
        ref = f"refs/heads/{branch}"
        completed = self._git(repo_path, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
        payload = self._git_payload(completed)
        if completed.returncode == 0:
            tip = payload["stdout"].splitlines()[-1] if payload["stdout"] else None
            return {"exists": True, "ref": ref, "tip": tip, "git": payload}
        if completed.returncode == 1:
            return {"exists": False, "ref": ref, "tip": None, "git": payload}
        return {"exists": None, "ref": ref, "tip": None, "git": payload, "error": "local_branch_lookup_failed"}

    def _remote_branch_status(self, repo_path: str, remote: str, branch: str) -> Dict[str, Any]:
        completed = self._git(repo_path, "ls-remote", "--heads", remote, branch, timeout=120)
        payload = self._git_payload(completed)
        if completed.returncode != 0:
            return {"exists": None, "remote": remote, "branch": branch, "tip": None, "git": payload, "error": "remote_branch_lookup_failed"}
        if not payload["stdout"]:
            return {"exists": False, "remote": remote, "branch": branch, "tip": None, "git": payload}
        first_line = payload["stdout"].splitlines()[0]
        tip = first_line.split()[0] if first_line.split() else None
        return {"exists": True, "remote": remote, "branch": branch, "tip": tip, "git": payload}

    def _active_leases_using_branch(self, raw_branch: str, normalized_branch: str, remote: str) -> list[Dict[str, Any]]:
        branch_variants = {
            raw_branch,
            normalized_branch,
            f"{remote}/{normalized_branch}",
            f"refs/heads/{normalized_branch}",
            f"refs/remotes/{remote}/{normalized_branch}",
        }
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        rows = self.conn.execute(
            f"""
            SELECT *
            FROM leases
            WHERE released_at IS NULL
              AND status IN ({placeholders})
            ORDER BY acquired_at DESC
            """,
            tuple(sorted(ACTIVE_STATUSES)),
        ).fetchall()
        active: list[Dict[str, Any]] = []
        for row in rows:
            lease = row_to_dict(row)
            if lease and str(lease.get("branch") or "") in branch_variants:
                active.append(lease)
        return active

    def _worktrees_using_branch(self, repo_path: str, branch: str) -> Dict[str, Any]:
        completed = self._git(repo_path, "worktree", "list", "--porcelain")
        payload = self._git_payload(completed)
        if completed.returncode != 0:
            return {"ok": False, "git": payload, "worktrees": []}

        worktrees: list[Dict[str, Any]] = []
        current: Dict[str, Any] = {}
        for line in completed.stdout.splitlines():
            if not line.strip():
                if current:
                    worktrees.append(current)
                    current = {}
                continue
            key, _, value = line.partition(" ")
            if key == "worktree" and current:
                worktrees.append(current)
                current = {}
            current[key] = value
        if current:
            worktrees.append(current)

        branch_ref = f"refs/heads/{branch}"
        using_branch = [
            item for item in worktrees
            if item.get("branch") in {branch_ref, branch}
        ]
        return {"ok": True, "git": payload, "worktrees": using_branch}

    def _branch_tip_retained_check(self, repo_path: str, tip: Optional[str], source_commit: str,
                                   deployed_commit: str) -> Dict[str, Any]:
        if not tip:
            return {"ok": False, "reason": "branch_tip_missing"}
        if tip == source_commit:
            return {"ok": True, "reason": "tip_matches_source_commit"}
        ancestor = self._commit_is_ancestor(repo_path, tip, deployed_commit)
        if ancestor.get("ok"):
            return {"ok": True, "reason": "tip_is_ancestor_of_deployed_commit", "tip": tip}
        return {
            "ok": False,
            "reason": "branch_tip_not_retained_in_deployed_history",
            "tip": tip,
            "git": ancestor,
        }

    def _remote_tip_retained_check(self, repo_path: str, remote: str, branch: str, tip: Optional[str],
                                   source_commit: str, deployed_commit: str) -> Dict[str, Any]:
        if not tip:
            return {"ok": False, "reason": "remote_branch_tip_missing"}
        if tip == source_commit:
            return {"ok": True, "reason": "remote_tip_matches_source_commit"}

        fetch = self._git(repo_path, "fetch", "--no-tags", remote, f"refs/heads/{branch}", timeout=120)
        if fetch.returncode != 0:
            return {
                "ok": False,
                "reason": "remote_tip_not_proven_retained",
                "tip": tip,
                "fetch": self._git_payload(fetch),
            }
        ancestor = self._commit_is_ancestor(repo_path, tip, deployed_commit)
        if ancestor.get("ok"):
            return {"ok": True, "reason": "remote_tip_is_ancestor_of_deployed_commit", "tip": tip}
        return {
            "ok": False,
            "reason": "remote_branch_tip_not_retained_in_deployed_history",
            "tip": tip,
            "git": ancestor,
        }

    def cleanup_task_branch(self, repo_path: str, source_branch: str, source_commit: str,
                            deployed_commit: str, actor: str, remote: str = "origin",
                            dry_run: bool = True, delete_local: bool = True,
                            delete_remote: bool = True) -> Dict[str, Any]:
        checks: list[Dict[str, Any]] = []
        planned_actions: list[Dict[str, Any]] = []
        performed_actions: list[Dict[str, Any]] = []
        errors: list[Dict[str, Any]] = []

        raw_branch, branch = self._normalize_cleanup_branch(source_branch, remote or "origin")
        result: Dict[str, Any] = {
            "ok": False,
            "status": "checking",
            "dry_run": bool(dry_run),
            "repo_path": repo_path,
            "remote": remote or "origin",
            "source_branch": raw_branch,
            "normalized_branch": branch,
            "source_commit": source_commit,
            "deployed_commit": deployed_commit,
            "checks": checks,
            "planned_actions": planned_actions,
            "performed_actions": performed_actions,
            "errors": errors,
        }

        required = {
            "repo_path": repo_path,
            "source_branch": raw_branch,
            "source_commit": source_commit,
            "deployed_commit": deployed_commit,
            "actor": actor,
        }
        for name, value in required.items():
            ok = bool(str(value or "").strip())
            self._add_cleanup_check(checks, f"{name}_provided", ok)
            if not ok:
                errors.append({"check": f"{name}_provided", "error": "missing_required_argument", "argument": name})
        if errors:
            result.update({"status": "refused", "error": "unsafe_branch_cleanup_refused"})
            return result

        repo = os.path.abspath(os.path.expanduser(os.path.expandvars(str(repo_path))))
        result["repo_path"] = repo
        repo_exists = os.path.isdir(repo)
        self._add_cleanup_check(checks, "repo_path_exists", repo_exists, {"repo_path": repo})
        if not repo_exists:
            errors.append({"check": "repo_path_exists", "error": "repo_path_not_found", "repo_path": repo})
            result.update({"status": "refused", "error": "unsafe_branch_cleanup_refused"})
            return result

        git_repo = self._git(repo, "rev-parse", "--is-inside-work-tree")
        git_repo_ok = git_repo.returncode == 0 and git_repo.stdout.strip() == "true"
        self._add_cleanup_check(checks, "repo_is_git_worktree", git_repo_ok, self._git_payload(git_repo))
        if not git_repo_ok:
            errors.append({"check": "repo_is_git_worktree", "error": "repo_is_not_git_worktree", "git": self._git_payload(git_repo)})

        valid_branch = bool(branch) and self._git(repo, "check-ref-format", "--branch", branch).returncode == 0
        self._add_cleanup_check(checks, "source_branch_name_valid", valid_branch, {"branch": branch})
        if not valid_branch:
            errors.append({"check": "source_branch_name_valid", "error": "invalid_source_branch", "branch": branch})

        protected = self._branch_is_protected(raw_branch, branch, remote or "origin")
        self._add_cleanup_check(checks, "source_branch_not_protected", not protected, {"branch": raw_branch, "normalized_branch": branch})
        if protected:
            errors.append({"check": "source_branch_not_protected", "error": "protected_branch_refused", "branch": raw_branch})

        if errors:
            result.update({"status": "refused", "error": "unsafe_branch_cleanup_refused"})
            return result

        source = self._resolve_commit_for_cleanup(repo, str(source_commit).strip())
        self._add_cleanup_check(checks, "source_commit_resolves", bool(source.get("ok")), source)
        if not source.get("ok"):
            errors.append({"check": "source_commit_resolves", "error": "source_commit_not_found", "git": source})

        deployed = self._resolve_commit_for_cleanup(repo, str(deployed_commit).strip())
        self._add_cleanup_check(checks, "deployed_commit_resolves", bool(deployed.get("ok")), deployed)
        if not deployed.get("ok"):
            errors.append({"check": "deployed_commit_resolves", "error": "deployed_commit_not_found", "git": deployed})

        if errors:
            result.update({"status": "refused", "error": "unsafe_branch_cleanup_refused"})
            return result

        source_sha = str(source["commit"])
        deployed_sha = str(deployed["commit"])
        result["source_commit_resolved"] = source_sha
        result["deployed_commit_resolved"] = deployed_sha

        retained = self._commit_is_ancestor(repo, source_sha, deployed_sha)
        self._add_cleanup_check(checks, "source_commit_retained_in_deployed_history", bool(retained.get("ok")), retained)
        if not retained.get("ok"):
            errors.append({
                "check": "source_commit_retained_in_deployed_history",
                "error": "source_commit_not_retained_in_deployed_history",
                "git": retained,
            })

        active_leases = self._active_leases_using_branch(raw_branch, branch, remote or "origin")
        result["active_leases_using_branch"] = active_leases
        self._add_cleanup_check(checks, "no_active_lease_using_branch", not active_leases, {"active_leases": active_leases})
        if active_leases:
            errors.append({"check": "no_active_lease_using_branch", "error": "active_lease_using_branch", "active_leases": active_leases})

        worktrees = self._worktrees_using_branch(repo, branch)
        result["worktrees_using_branch"] = worktrees.get("worktrees", [])
        self._add_cleanup_check(checks, "worktree_list_available", bool(worktrees.get("ok")), worktrees.get("git"))
        if not worktrees.get("ok"):
            errors.append({"check": "worktree_list_available", "error": "worktree_list_failed", "git": worktrees.get("git")})
        self._add_cleanup_check(checks, "no_worktree_using_branch", not worktrees.get("worktrees"), {"worktrees": worktrees.get("worktrees", [])})
        if worktrees.get("worktrees"):
            errors.append({"check": "no_worktree_using_branch", "error": "worktree_using_branch", "worktrees": worktrees["worktrees"]})

        local = self._local_branch_status(repo, branch)
        result["local"] = local
        self._add_cleanup_check(checks, "local_branch_lookup", local.get("exists") is not None, local.get("git"))
        if local.get("exists") is None:
            errors.append({"check": "local_branch_lookup", "error": "local_branch_lookup_failed", "git": local.get("git")})
        elif local.get("exists"):
            local_retained = self._branch_tip_retained_check(repo, local.get("tip"), source_sha, deployed_sha)
            local["retained_check"] = local_retained
            self._add_cleanup_check(checks, "local_branch_tip_safe", bool(local_retained.get("ok")), local_retained)
            if not local_retained.get("ok"):
                errors.append({"check": "local_branch_tip_safe", "error": "local_branch_tip_not_safe", "detail": local_retained})

        remote_status = self._remote_branch_status(repo, remote or "origin", branch)
        result["remote_status"] = remote_status
        self._add_cleanup_check(checks, "remote_branch_lookup", remote_status.get("exists") is not None, remote_status.get("git"))
        if remote_status.get("exists") is None:
            errors.append({"check": "remote_branch_lookup", "error": "remote_branch_lookup_failed", "git": remote_status.get("git")})
        elif remote_status.get("exists"):
            remote_retained = self._remote_tip_retained_check(repo, remote or "origin", branch, remote_status.get("tip"), source_sha, deployed_sha)
            remote_status["retained_check"] = remote_retained
            self._add_cleanup_check(checks, "remote_branch_tip_safe", bool(remote_retained.get("ok")), remote_retained)
            if not remote_retained.get("ok"):
                errors.append({"check": "remote_branch_tip_safe", "error": "remote_branch_tip_not_safe", "detail": remote_retained})

        if errors:
            result.update({"status": "refused", "error": "unsafe_branch_cleanup_refused"})
            return result

        if delete_local:
            if local.get("exists"):
                planned_actions.append({"target": "local", "action": "delete_branch", "command": ["git", "branch", "-d", branch]})
                local["planned_action"] = "delete_branch"
            else:
                local["planned_action"] = "already_missing"
        else:
            local["planned_action"] = "skip"

        if delete_remote:
            if remote_status.get("exists"):
                planned_actions.append({"target": "remote", "action": "delete_branch", "command": ["git", "push", remote or "origin", "--delete", branch]})
                remote_status["planned_action"] = "delete_branch"
            else:
                remote_status["planned_action"] = "already_missing"
        else:
            remote_status["planned_action"] = "skip"

        if dry_run:
            result.update({"ok": True, "status": "dry_run"})
            return result

        if delete_remote and remote_status.get("exists"):
            recheck = self._remote_branch_status(repo, remote or "origin", branch)
            remote_status["pre_delete_recheck"] = recheck
            recheck_ok = recheck.get("exists") is False
            if recheck.get("exists"):
                recheck_retained = self._remote_tip_retained_check(repo, remote or "origin", branch, recheck.get("tip"), source_sha, deployed_sha)
                recheck["retained_check"] = recheck_retained
                recheck_ok = bool(recheck_retained.get("ok"))
            if not recheck_ok:
                result.update({
                    "status": "refused",
                    "error": "unsafe_branch_cleanup_refused",
                })
                errors.append({"check": "remote_branch_pre_delete_recheck", "error": "remote_branch_tip_drift", "remote": recheck})
                return result
            if recheck.get("exists"):
                push = self._git(repo, "push", remote or "origin", "--delete", branch, timeout=120)
                action = {"target": "remote", "action": "delete_branch", "git": self._git_payload(push)}
                performed_actions.append(action)
                if push.returncode != 0:
                    errors.append({"check": "remote_delete", "error": "cleanup_failed", "action": action})

        if delete_local and local.get("exists"):
            delete = self._git(repo, "branch", "-d", branch)
            action = {"target": "local", "action": "delete_branch", "git": self._git_payload(delete)}
            performed_actions.append(action)
            if delete.returncode != 0:
                errors.append({"check": "local_delete", "error": "cleanup_failed", "action": action})

        if errors:
            result.update({"status": "cleanup_failed", "error": "cleanup_failed"})
            return result

        result.update({"ok": True, "status": "cleaned"})
        return result

    def _queue_view(self, queue: Dict[str, Any], active_lease: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        public = self._public_queue(queue)
        metadata = dict(public.get("metadata") or {})
        requested_environment_id = metadata.get("requested_environment_id") or public.get("environment_id")
        assigned_environment_id = metadata.get("assigned_environment_id")
        if public.get("status") != "queued" and assigned_environment_id is None:
            assigned_environment_id = public.get("environment_id")
        queued_because_owner = metadata.pop("queued_because_owner", None)
        legacy_busy_owner = metadata.pop("busy_owner", None)
        if queued_because_owner is None:
            queued_because_owner = legacy_busy_owner

        busy_owner = self._owner_from_lease(active_lease)
        public["requested_environment_id"] = requested_environment_id
        public["assigned_environment_id"] = assigned_environment_id
        public["busy_owner"] = busy_owner
        if queued_because_owner is not None:
            public["queued_because_owner"] = queued_because_owner

        if busy_owner is not None:
            metadata["busy_owner"] = busy_owner
        if queued_because_owner is not None:
            metadata["queued_because_owner"] = queued_because_owner
        public["metadata"] = metadata
        return public

    def _queue_position(self, queue_id: str) -> Optional[int]:
        row = row_to_dict(self.conn.execute(
            "SELECT * FROM deploy_queue WHERE id = ? AND status = 'queued'",
            (queue_id,),
        ).fetchone())
        if row is None:
            return None
        queue_pool = self._queue_candidate_environment_ids(row)
        rows = self.conn.execute(
            """
            SELECT *
            FROM deploy_queue
            WHERE status = 'queued'
            ORDER BY priority DESC, requested_at ASC, id ASC
            """,
        ).fetchall()
        position = 1
        for entry in (row_to_dict(item) for item in rows):
            if not entry:
                continue
            if entry["id"] == queue_id:
                return position
            if self._queue_candidate_environment_ids(entry) & queue_pool:
                position += 1
        return position

    def _queue_entries(self, environment_id: Optional[str] = None,
                       include_terminal: bool = False) -> list[Dict[str, Any]]:
        statuses = set(QUEUE_ACTIVE_STATUSES)
        if include_terminal:
            statuses.update(QUEUE_TERMINAL_STATUSES)
        placeholders = ",".join("?" for _ in statuses)
        params: list[Any] = sorted(statuses)
        where = f"status IN ({placeholders})"
        if environment_id:
            where = f"environment_id = ? AND {where}"
            params.insert(0, environment_id)
        rows = self.conn.execute(
            f"""
            SELECT *
            FROM deploy_queue
            WHERE {where}
            ORDER BY
              CASE status WHEN 'deploying' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,
              priority DESC,
              requested_at ASC,
              id ASC
            """,
            tuple(params),
        ).fetchall()
        entries = [row_to_dict(row) for row in rows]
        output = [entry for entry in entries if entry]
        active_leases_by_environment: Dict[str, Optional[Dict[str, Any]]] = {}
        for entry in output:
            entry["position"] = self._queue_position(entry["id"])
            env_id = str(entry["environment_id"])
            if env_id not in active_leases_by_environment:
                active_leases_by_environment[env_id] = self._active_lease(env_id)
        return [self._queue_view(entry, active_leases_by_environment.get(str(entry["environment_id"]))) for entry in output]

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

    def _record_callback_attempt(self, *, queue: Dict[str, Any], event: str,
                                 lease_id: Optional[str], callback_url: Optional[str],
                                 endpoint: Optional[str], auth_present: bool, ok: bool,
                                 outcome: str, http_status: Optional[int] = None,
                                 response_body: Optional[str] = None,
                                 error: Optional[str] = None,
                                 payload: Optional[Dict[str, Any]] = None) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO callback_attempts (
              queue_id, lease_id, environment_id, task_id, event, callback_url,
              endpoint, auth_present, ok, outcome, http_status, response_body,
              error, payload_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                queue.get("id"),
                lease_id,
                str(queue.get("environment_id")),
                queue.get("task_id"),
                event,
                callback_url,
                endpoint,
                1 if auth_present else 0,
                1 if ok else 0,
                outcome,
                http_status,
                response_body[-4000:] if response_body else None,
                error,
                json.dumps(payload or {}, sort_keys=True),
                utc_now(),
            ),
        )
        return int(cursor.lastrowid)

    def callback_attempts(self, queue_id: Optional[str] = None, lease_id: Optional[str] = None,
                          task_id: Optional[str] = None, environment_id: Optional[str] = None,
                          limit: int = 50) -> Dict[str, Any]:
        clauses: list[str] = []
        params: list[Any] = []
        if queue_id:
            clauses.append("queue_id = ?")
            params.append(queue_id)
        if lease_id:
            clauses.append("lease_id = ?")
            params.append(lease_id)
        if task_id:
            clauses.append("task_id = ?")
            params.append(task_id)
        if environment_id:
            clauses.append("environment_id = ?")
            params.append(environment_id)

        bounded_limit = max(1, min(int(limit), 200))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.conn.execute(
            f"""
            SELECT *
            FROM callback_attempts
            {where}
            ORDER BY id DESC
            LIMIT ?
            """,
            (*params, bounded_limit),
        ).fetchall()
        attempts = []
        for row in rows:
            attempt = row_to_dict(row)
            if not attempt:
                continue
            attempt["ok"] = bool(attempt.get("ok"))
            attempt["auth_present"] = bool(attempt.get("auth_present"))
            attempts.append(attempt)
        return {"ok": True, "callback_attempts": attempts}

    def _busy(self, env: Dict[str, Any], lease: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ok": False,
            "status": "blocked",
            "error": "environment_busy",
            "environment": env,
            "lease": lease,
            "owner": self._owner_from_lease(lease),
            "next_action": "Do not deploy or mutate the shared dev checkout. Post blocked/waiting with this lease id, or ask an operator to force release if the lease is stale.",
        }

    def _callback_endpoint(self, callback_url: str) -> str:
        normalized = callback_url.rstrip("/")
        if normalized.endswith("/api/v1/external/task-events"):
            return normalized
        return f"{normalized}/api/v1/external/task-events"

    def _agent_hq_base_url(self, callback_url: Optional[str]) -> Optional[str]:
        raw_url = self._callback_url(callback_url)
        if not raw_url:
            return None
        normalized = str(raw_url).rstrip("/")
        task_events_suffix = "/api/v1/external/task-events"
        api_suffix = "/api/v1"
        if normalized.endswith(task_events_suffix):
            normalized = normalized[:-len(task_events_suffix)]
        elif normalized.endswith(api_suffix):
            normalized = normalized[:-len(api_suffix)]
        return normalized.rstrip("/") or None

    def _task_active_owner_endpoint(self, base_url: str, task_id: str) -> str:
        encoded_task_id = urllib.parse.quote(str(task_id), safe="")
        return f"{base_url.rstrip('/')}/api/v1/tasks/{encoded_task_id}/active-owner"

    def _parse_json_body(self, body: str) -> Dict[str, Any]:
        try:
            parsed = json.loads(body or "{}")
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _validate_task_active_owner(self, task_id: str, callback_url: Optional[str],
                                    callback_api_key: Optional[str]) -> Dict[str, Any]:
        base_url = self._agent_hq_base_url(callback_url)
        if not base_url:
            return {"ok": True, "skipped": True, "reason": "agent_hq_not_configured"}

        api_key = self._callback_api_key(callback_api_key)
        if not api_key:
            return {
                "ok": False,
                "error": "callback_api_key_required",
                "message": "AGENT_HQ_MCP_API_KEY is required in the lease manager MCP server env when Agent HQ callbacks are configured.",
            }

        endpoint = self._task_active_owner_endpoint(base_url, task_id)
        request = urllib.request.Request(
            endpoint,
            headers={"Accept": "application/json", "x-api-key": str(api_key)},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                response_body = response.read().decode("utf-8", errors="replace")
                status = int(response.status)
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            authorization = self._parse_json_body(response_body)
            return {
                "ok": False,
                "error": "deploy_authorization_failed",
                "reason": authorization.get("reason") or authorization.get("code") or "active_owner_check_http_failure",
                "message": authorization.get("error") or f"Agent HQ active owner check failed with HTTP {exc.code}.",
                "http_status": exc.code,
                "endpoint": endpoint,
                "authorization": authorization,
            }
        except Exception as exc:
            return {
                "ok": False,
                "error": "deploy_authorization_check_failed",
                "reason": "active_owner_check_transport_error",
                "message": str(exc),
                "endpoint": endpoint,
            }

        authorization = self._parse_json_body(response_body)
        if not 200 <= status < 300:
            return {
                "ok": False,
                "error": "deploy_authorization_failed",
                "reason": authorization.get("reason") or authorization.get("code") or "active_owner_check_http_failure",
                "message": authorization.get("error") or f"Agent HQ active owner check failed with HTTP {status}.",
                "http_status": status,
                "endpoint": endpoint,
                "authorization": authorization,
            }
        if not authorization:
            return {
                "ok": False,
                "error": "deploy_authorization_check_failed",
                "reason": "active_owner_check_invalid_response",
                "message": "Agent HQ active owner endpoint returned an invalid JSON object.",
                "http_status": status,
                "endpoint": endpoint,
            }
        if authorization.get("is_active_owner") is True:
            return {
                "ok": True,
                "endpoint": endpoint,
                "http_status": status,
                "authorization": authorization,
            }
        return {
            "ok": False,
            "error": "deploy_authorization_failed",
            "reason": authorization.get("reason") or authorization.get("code") or "not_active_task_owner",
            "message": authorization.get("error") or "Authenticated agent is not the active owner for the requested task.",
            "http_status": status,
            "endpoint": endpoint,
            "authorization": authorization,
        }

    def _active_owner_authorization_metadata(self, authorization_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        authorization = authorization_result.get("authorization")
        if not isinstance(authorization, dict) or authorization.get("is_active_owner") is not True:
            return None
        return {
            "validated_at": utc_now(),
            "task_id": authorization.get("task_id"),
            "authenticated_agent_id": authorization.get("authenticated_agent_id"),
            "authenticated_agent_slug": authorization.get("authenticated_agent_slug"),
            "active_instance_id": authorization.get("active_instance_id"),
            "active_instance_agent_id": authorization.get("active_instance_agent_id"),
            "active_instance_status": authorization.get("active_instance_status"),
            "reason": authorization.get("reason"),
        }

    def _failure_class_from_error(self, error: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(error, dict):
            return None
        candidates = [error.get("failure_class")]
        result = error.get("result")
        if isinstance(result, dict):
            candidates.extend([result.get("failure_class"), result.get("error")])
            deploy = result.get("deploy")
            if isinstance(deploy, dict):
                candidates.extend([deploy.get("failure_class"), deploy.get("error")])
        for candidate in candidates:
            if isinstance(candidate, str) and candidate in DEPLOY_FAILURE_EVENTS:
                return candidate
        return None

    def _failure_event_name(self, error: Optional[Dict[str, Any]]) -> str:
        return self._failure_class_from_error(error) or "deploy_failed"

    def _failure_phase_from_error(self, error: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(error, dict):
            return None
        candidates = [error.get("phase")]
        result = error.get("result")
        if isinstance(result, dict):
            candidates.append(result.get("phase"))
            deploy = result.get("deploy")
            if isinstance(deploy, dict):
                candidates.append(deploy.get("phase"))
        candidates.append(error.get("stage"))
        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return None

    def _send_callback(self, queue: Dict[str, Any], event: str, message: str,
                       lease: Optional[Dict[str, Any]] = None,
                       error: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        callback_url = self._callback_url(queue.get("callback_url"))
        env = self._environment(str(queue["environment_id"])) or {}
        lease_id = (lease or {}).get("id") or queue.get("lease_id") or queue["id"]
        commit_sha = queue.get("commit_sha") or (lease or {}).get("commit_sha")
        metadata = queue.get("metadata") or {}
        requested_environment_id = metadata.get("requested_environment_id")
        assigned_environment_id = metadata.get("assigned_environment_id")
        if queue.get("status") != "queued" and assigned_environment_id is None:
            assigned_environment_id = queue.get("environment_id")
        payload: Dict[str, Any] = {
            "source": CALLBACK_SOURCE,
            "event": event,
            "task_id": queue["task_id"],
            "environment_id": queue["environment_id"],
            "queue_id": queue["id"],
            "lease_id": lease_id,
            "branch": queue.get("branch"),
            "commit_sha": commit_sha,
            "review_url": env.get("base_url"),
            "message": message,
        }
        if requested_environment_id:
            payload["requested_environment_id"] = requested_environment_id
        if assigned_environment_id:
            payload["assigned_environment_id"] = assigned_environment_id
        if error:
            payload["error"] = error
            failure_class = self._failure_class_from_error(error)
            if failure_class:
                payload["failure_class"] = failure_class
            failure_phase = self._failure_phase_from_error(error)
            if failure_phase:
                payload["phase"] = failure_phase

        if not callback_url:
            attempt_id = self._record_callback_attempt(
                queue=queue,
                event=event,
                lease_id=str(lease_id) if lease_id else None,
                callback_url=None,
                endpoint=None,
                auth_present=False,
                ok=True,
                outcome="skipped",
                error="callback_url_not_configured",
                payload=payload,
            )
            return {
                "ok": True,
                "skipped": True,
                "reason": "callback_url_not_configured",
                "attempt_id": attempt_id,
            }

        endpoint = self._callback_endpoint(str(callback_url))
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        api_key = self._callback_api_key(queue.get("callback_api_key"))
        auth_present = bool(api_key)
        if api_key:
            request.add_header("x-api-key", str(api_key))

        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                response_body = response.read().decode("utf-8", errors="replace")
                ok = 200 <= response.status < 300
                attempt_id = self._record_callback_attempt(
                    queue=queue,
                    event=event,
                    lease_id=str(lease_id) if lease_id else None,
                    callback_url=str(callback_url),
                    endpoint=endpoint,
                    auth_present=auth_present,
                    ok=ok,
                    outcome="http_success" if ok else "http_failure",
                    http_status=response.status,
                    response_body=response_body,
                    payload=payload,
                )
                return {
                    "ok": ok,
                    "status": response.status,
                    "body": response_body[-4000:],
                    "payload": payload,
                    "attempt_id": attempt_id,
                }
        except urllib.error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="replace")
            attempt_id = self._record_callback_attempt(
                queue=queue,
                event=event,
                lease_id=str(lease_id) if lease_id else None,
                callback_url=str(callback_url),
                endpoint=endpoint,
                auth_present=auth_present,
                ok=False,
                outcome="http_failure",
                http_status=exc.code,
                response_body=response_body,
                payload=payload,
            )
            return {
                "ok": False,
                "status": exc.code,
                "body": response_body[-4000:],
                "payload": payload,
                "attempt_id": attempt_id,
            }
        except Exception as exc:
            attempt_id = self._record_callback_attempt(
                queue=queue,
                event=event,
                lease_id=str(lease_id) if lease_id else None,
                callback_url=str(callback_url),
                endpoint=endpoint,
                auth_present=auth_present,
                ok=False,
                outcome="transport_error",
                error=str(exc),
                payload=payload,
            )
            return {"ok": False, "error": str(exc), "payload": payload, "attempt_id": attempt_id}

    def _direct_deploy_callback_queue(self, *, environment_id: str, requested_environment_id: str,
                                      task_id: str, lease_id: str, agent_id: Optional[str],
                                      agent_name: Optional[str], branch: Optional[str],
                                      commit: Optional[str], source_repo_path: str,
                                      callback_url: Optional[str],
                                      callback_api_key: Optional[str]) -> Dict[str, Any]:
        return {
            "id": lease_id,
            "environment_id": environment_id,
            "task_id": task_id,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "branch": branch,
            "commit_sha": commit,
            "source_repo_path": source_repo_path,
            "status": "deploying",
            "callback_url": callback_url,
            "callback_api_key": callback_api_key,
            "metadata": {
                "requested_environment_id": requested_environment_id,
                "assigned_environment_id": environment_id,
                "direct_deploy": True,
            },
        }

    def _deploy_failure_summary(self, result: Dict[str, Any]) -> str:
        deploy = result.get("deploy") if isinstance(result.get("deploy"), dict) else {}
        error = (
            deploy.get("error")
            or result.get("error")
            or result.get("message")
            or result.get("release_reason")
            or "deploy failed"
        )
        details: list[str] = []
        health = deploy.get("health")
        if isinstance(health, dict):
            for name, value in sorted(health.items()):
                if not isinstance(value, dict):
                    continue
                status = "ok" if value.get("ok") else "failed"
                detail = value.get("detail")
                url = value.get("url")
                if detail is not None and url:
                    details.append(f"{name} {status} at {url}: {detail}")
                elif detail is not None:
                    details.append(f"{name} {status}: {detail}")
                else:
                    details.append(f"{name} {status}")
        elif deploy.get("stderr"):
            stderr_lines = str(deploy.get("stderr")).strip().splitlines()
            if stderr_lines:
                details.append(stderr_lines[-1])
        elif deploy.get("stdout"):
            stdout_lines = str(deploy.get("stdout")).strip().splitlines()
            if stdout_lines:
                details.append(stdout_lines[-1])
        if details:
            return f"{error} ({'; '.join(detail for detail in details if detail)})"
        return str(error)

    def _json_safe_callback_value(self, value: Any, seen: Optional[set[int]] = None) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if seen is None:
            seen = set()

        value_id = id(value)
        if value_id in seen:
            return "<circular-reference>"

        if isinstance(value, dict):
            seen.add(value_id)
            try:
                return {
                    str(key): self._json_safe_callback_value(item, seen)
                    for key, item in value.items()
                    if key != "callbacks"
                }
            finally:
                seen.remove(value_id)

        if isinstance(value, (list, tuple, set)):
            seen.add(value_id)
            try:
                return [self._json_safe_callback_value(item, seen) for item in value]
            finally:
                seen.remove(value_id)

        return str(value)

    def _callback_error(self, stage: str, result: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "stage": stage,
            "result": self._json_safe_callback_value(result),
        }

    def _set_queue_status(self, queue_id: str, status: str, *,
                          lease_id: Optional[str] = None,
                          error: Optional[Dict[str, Any]] = None,
                          environment_id: Optional[str] = None,
                          metadata_update: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        now = utc_now()
        fields: Dict[str, Any] = {
            "status": status,
            "updated_at": now,
        }
        if environment_id is not None:
            fields["environment_id"] = environment_id
        if status == "deploying":
            fields["started_at"] = now
        if status in QUEUE_TERMINAL_STATUSES:
            fields["completed_at"] = now
        if lease_id is not None:
            fields["lease_id"] = lease_id
        if error is not None:
            fields["error_json"] = json.dumps(error, sort_keys=True)
        if metadata_update is not None:
            current = self._queue_row(queue_id) or {}
            metadata = dict(current.get("metadata") or {})
            metadata.update(metadata_update)
            fields["metadata_json"] = json.dumps(metadata, sort_keys=True)
        assignments = ", ".join(f"{key} = ?" for key in fields)
        self.conn.execute(f"UPDATE deploy_queue SET {assignments} WHERE id = ?", (*fields.values(), queue_id))
        return self._queue_row(queue_id) or {"id": queue_id, "status": status}

    def acquire(self, environment_id: str, task_id: str, actor: str, agent_id: Optional[str] = None,
                agent_name: Optional[str] = None, branch: Optional[str] = None,
                commit: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
                supersede_same_task_review: bool = False) -> Dict[str, Any]:
        if not task_id:
            return {"ok": False, "error": "task_id is required"}
        task_rejection = qa_fixture_task_rejection(task_id) if is_qa_fixture_task_id(task_id) else None
        if task_rejection:
            return task_rejection
        if not actor:
            return {"ok": False, "error": "actor is required"}
        env = self._environment(environment_id)
        if not env:
            return {"ok": False, "error": "environment_not_found", "environment_id": environment_id}

        now = utc_now()
        lease_id = str(uuid4())
        superseded_lease = None
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            active = self._active_lease(environment_id)
            if active:
                can_supersede = (
                    supersede_same_task_review
                    and str(active.get("task_id")) == str(task_id)
                    and active.get("status") in {"deployed_for_qa", "stale"}
                )
                if not can_supersede:
                    self.conn.execute("ROLLBACK")
                    return self._busy(env, active)
                message = (
                    "Superseded by same-task redeploy"
                    f" for task {task_id}"
                    f" from commit {active.get('commit_sha') or 'unknown'}"
                    f" to commit {commit or 'unknown'}"
                )
                self.conn.execute(
                    "UPDATE leases SET status = 'superseded', release_reason = 'superseded', released_at = ?, heartbeat_at = ? WHERE id = ?",
                    (now, now, active["id"]),
                )
                self._event(active["id"], active["environment_id"], active["task_id"], actor, "release", active["status"], "superseded", "superseded", message)
                self._event(
                    active["id"],
                    active["environment_id"],
                    active["task_id"],
                    actor,
                    "supersede_for_redeploy",
                    active["status"],
                    "superseded",
                    "superseded",
                    message,
                    {
                        "superseded_by": {
                            "task_id": str(task_id),
                            "actor": actor,
                            "branch": branch,
                            "commit": commit,
                        }
                    },
                )
                superseded_lease = self._lease(active["id"])
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

        result = {"ok": True, "status": "acquired", "environment": env, "lease": self._lease(lease_id)}
        if superseded_lease:
            result["superseded_lease"] = superseded_lease
        return result

    def enqueue_deploy_request(self, environment_id: str, task_id: str, actor: str, source_repo_path: str,
                               agent_id: Optional[str] = None, agent_name: Optional[str] = None,
                               branch: Optional[str] = None, commit: Optional[str] = None,
                               services: str = "both", health_check: bool = True,
                               priority: int = 0, callback_url: Optional[str] = None,
                               callback_api_key: Optional[str] = None,
                               metadata: Optional[Dict[str, Any]] = None,
                               database_policy: str = "preflight_and_apply") -> Dict[str, Any]:
        if not task_id:
            return {"ok": False, "error": "task_id is required"}
        task_rejection = qa_fixture_task_rejection(task_id) if is_qa_fixture_task_id(task_id) else None
        if task_rejection:
            return task_rejection
        if not actor:
            return {"ok": False, "error": "actor is required"}
        env = self._environment(environment_id)
        if not env:
            return {"ok": False, "error": "environment_not_found", "environment_id": environment_id}
        if not os.path.isdir(source_repo_path):
            return {"ok": False, "error": "source_repo_path_not_found", "source_repo_path": source_repo_path}

        now = utc_now()
        queue_id = str(uuid4())
        resolved_callback_url = self._callback_url(callback_url)
        resolved_callback_api_key = self._callback_api_key(callback_api_key)
        if resolved_callback_url and not resolved_callback_api_key:
            return {
                "ok": False,
                "error": "callback_api_key_required",
                "message": "AGENT_HQ_MCP_API_KEY is required in the lease manager MCP server env when Agent HQ callbacks are configured.",
                "environment_id": environment_id,
            }
        queue_metadata = self._request_metadata(env, environment_id, source_repo_path, extra={**(metadata or {}), "database_policy": normalize_database_policy(database_policy)})
        superseded: list[Dict[str, Any]] = []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            existing_rows = self.conn.execute(
                """
                SELECT *
                FROM deploy_queue
                WHERE task_id = ?
                  AND status = 'queued'
                ORDER BY requested_at ASC
                """,
                (str(task_id),),
            ).fetchall()
            for row in existing_rows:
                old = row_to_dict(row)
                if not old:
                    continue
                self._set_queue_status(old["id"], "superseded", error={
                    "reason": "newer_request_for_same_task",
                    "superseded_by": queue_id,
                    "commit_sha": commit,
                })
                superseded.append(self._queue_row(old["id"]) or old)

            self.conn.execute(
                """
                INSERT INTO deploy_queue (
                  id, environment_id, task_id, actor, agent_id, agent_name,
                  branch, commit_sha, source_repo_path, services, health_check,
                  priority, status, callback_url, callback_api_key,
                  requested_at, updated_at, metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)
                """,
                (
                    queue_id,
                    environment_id,
                    str(task_id),
                    actor,
                    str(agent_id) if agent_id is not None else None,
                    agent_name,
                    branch,
                    commit,
                    source_repo_path,
                    services,
                    1 if health_check else 0,
                    int(priority),
                    resolved_callback_url,
                    resolved_callback_api_key,
                    now,
                    now,
                    json.dumps(queue_metadata, sort_keys=True),
                ),
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

        queue = self._queue_row(queue_id) or {}
        callbacks = []
        for old in superseded:
            callbacks.append(self._send_callback(
                old,
                "superseded",
                f"Queued deploy request superseded by newer request {queue_id}.",
                error={"superseded_by": queue_id},
            ))
        callbacks.append(self._send_callback(
            queue,
            "dev_deploy_queued",
            f"Deploy queued for {environment_id}; position {self._queue_position(queue_id)}.",
        ))
        return {
            "ok": True,
            "status": "queued",
            "environment": env,
            "queue": self._public_queue({**queue, "position": self._queue_position(queue_id)}),
            "superseded": [self._public_queue(entry) for entry in superseded],
            "callbacks": callbacks,
        }

    def queue_status(self, environment_id: Optional[str] = None, include_terminal: bool = False) -> Dict[str, Any]:
        return {
            "ok": True,
            "queue": self._queue_entries(environment_id, include_terminal=include_terminal),
        }

    def cancel_queue_request(self, queue_id: str, actor: str, message: Optional[str] = None) -> Dict[str, Any]:
        if not actor:
            return {"ok": False, "error": "actor is required"}
        queue = self._queue_row(queue_id)
        if not queue:
            return {"ok": False, "error": "queue_request_not_found", "queue_id": queue_id}
        if queue["status"] != "queued":
            return {
                "ok": False,
                "error": "queue_request_not_cancellable",
                "queue": self._public_queue(queue),
                "allowed_status": "queued",
            }
        cancelled = self._set_queue_status(queue_id, "cancelled", error={"actor": actor, "message": message})
        callback = self._send_callback(
            cancelled,
            "cancelled",
            message or f"Queued deploy request {queue_id} cancelled by {actor}.",
        )
        return {"ok": True, "status": "cancelled", "queue": self._public_queue(cancelled), "callback": callback}

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
            served_commit = payload.get("served_commit") if isinstance(payload, dict) else None
            if served_commit and not lease.get("commit_sha"):
                fields["commit_sha"] = str(served_commit)
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

    def release(self, lease_id: str, actor: str, reason: str, message: Optional[str] = None,
                sweep_queue_after_release: bool = True) -> Dict[str, Any]:
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
        result = {"ok": True, "status": to_status, "lease": updated, "release_reason": reason, "agent_hq_note": self.release_note(updated, message)}
        if sweep_queue_after_release:
            try:
                result["queue_sweep"] = self.sweep_deploy_queue("queue-worker", environment_id=lease["environment_id"], limit=1)
            except Exception as exc:
                result["queue_sweep"] = {"ok": False, "error": "queue_sweep_failed", "message": str(exc)}
        return result

    def force_release(self, actor: str, reason: str, lease_id: Optional[str] = None,
                      environment_id: Optional[str] = None,
                      sweep_queue_after_release: bool = True) -> Dict[str, Any]:
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
        result = {"ok": True, "status": "force_released", "lease": self._lease(lease["id"]), "release_reason": reason}
        if sweep_queue_after_release:
            try:
                result["queue_sweep"] = self.sweep_deploy_queue("queue-worker", environment_id=lease["environment_id"], limit=1)
            except Exception as exc:
                result["queue_sweep"] = {"ok": False, "error": "queue_sweep_failed", "message": str(exc)}
        return result

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
            stale = self._lease_is_stale(lease, env, now_dt)
            queue_entries = self._queue_entries(env["id"])
            deploy_lock = inspect_native_deploy_lock(env)
            deploy_lock_blocks = bool(deploy_lock and deploy_lock.get("blocks_deploy"))
            item = {
                "environment": env,
                "available": lease is None and not deploy_lock_blocks,
                "active_lease": lease,
                "stale": stale,
                "queue": queue_entries,
                "queue_depth": len([entry for entry in queue_entries if entry.get("status") == "queued"]),
            }
            if deploy_lock:
                item["native_deploy_lock"] = deploy_lock
            if deploy_lock_blocks:
                item["blocked_by"] = "native_deploy_lock"
            if include_events and lease:
                item["events"] = self.events(lease["id"])["events"]
            output.append(item)
        return {"ok": True, "environments": output}

    def sweep_deploy_locks(
        self,
        actor: str,
        reason: str,
        environment_id: Optional[str] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        if not actor:
            return {"ok": False, "error": "actor is required"}
        if not reason:
            return {"ok": False, "error": "reason is required"}
        envs = [self._environment(environment_id)] if environment_id else self._environments()
        results = []
        for env in envs:
            if not env:
                results.append({"ok": False, "environment_id": environment_id, "error": "environment_not_found"})
                continue
            results.append(sweep_native_deploy_lock(env, actor, reason, force))
        return {
            "ok": all(result.get("ok") for result in results),
            "results": results,
            "removed": [entry for result in results for entry in result.get("removed", [])],
            "skipped": [entry for result in results for entry in result.get("skipped", [])],
        }

    def release_stale_leases(self, actor: str, message: Optional[str] = None) -> Dict[str, Any]:
        if not actor:
            return {"ok": False, "error": "actor is required"}
        now_dt = parse_time(utc_now())
        marked = []
        released = []
        skipped = []
        for env in self._environments():
            lease = self._active_lease(str(env["id"]))
            if not self._lease_is_stale(lease, env, now_dt):
                continue
            if not lease:
                continue
            if lease["status"] != "stale":
                self.conn.execute("UPDATE leases SET status = 'stale' WHERE id = ?", (lease["id"],))
                self._event(lease["id"], lease["environment_id"], lease["task_id"], actor, "mark_stale", lease["status"], "stale")
                lease = self._lease(lease["id"])
                if lease:
                    marked.append(lease)
            if not lease:
                continue
            result = self.release(
                lease["id"],
                actor,
                "stale_released",
                message or "stale lease released by MCP preflight cleanup",
                sweep_queue_after_release=False,
            )
            if result.get("ok"):
                released.append(result.get("lease"))
            else:
                skipped.append({"lease_id": lease["id"], "environment_id": env["id"], "result": result})
        return {"ok": True, "marked_stale": marked, "released": released, "skipped": skipped}

    def mcp_preflight_cleanup(self, actor: str = "mcp:auto-sweep") -> Dict[str, Any]:
        stale_leases = self.release_stale_leases(actor)
        deploy_locks = self.sweep_deploy_locks(actor, "mcp preflight stale deploy lock cleanup")
        return {
            "ok": bool(stale_leases.get("ok")) and bool(deploy_locks.get("ok")),
            "stale_leases": stale_leases,
            "deploy_locks": deploy_locks,
        }

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

    def _deploy_acquired_lease(self, env: Dict[str, Any], lease_id: str, environment_id: str,
                               task_id: str, actor: str, source_repo_path: str,
                               commit: Optional[str], services: str, health_check: bool,
                               dry_run: bool, timeout_seconds: int,
                               database_policy: str = "preflight_and_apply") -> Dict[str, Any]:
        if dry_run:
            deployed = self.transition(lease_id, "mark_deployed_for_qa", actor, {"dry_run": True, "served_commit": commit})
            deployed["deploy"] = {"dry_run": True}
            return deployed

        metadata = env.get("metadata") or {}
        deploy_mode = str(metadata.get("deploy_mode") or ("command" if env.get("deploy_command") else "native")).strip().lower()
        if deploy_mode == "command":
            deploy_payload = self._command_deploy(env, lease_id, environment_id, task_id, actor, source_repo_path, commit, services, health_check, timeout_seconds)
            if not deploy_payload.get("ok"):
                released = self.release(
                    lease_id,
                    actor,
                    "deploy_failed",
                    deploy_payload.get("message") or deploy_payload.get("stderr") or "deploy command failed",
                    sweep_queue_after_release=False,
                )
                released["deploy"] = deploy_payload
                return released
        else:
            try:
                current_lease = self._lease(lease_id) or {}
                deploy_payload = NativeDevDeployer().deploy(
                    env,
                    source_repo_path,
                    services=services,
                    health_check=health_check,
                    expected_commit=commit,
                    timeout_seconds=timeout_seconds,
                    database_policy=database_policy,
                    lock_metadata={
                        "lease_id": lease_id,
                        "task_id": task_id,
                        "actor": actor,
                        "branch": current_lease.get("branch"),
                        "commit": commit,
                    },
                )
            except NativeDeployError as exc:
                deploy_payload = exc.payload()
                released = self.release(
                    lease_id,
                    actor,
                    "deploy_failed",
                    deploy_payload.get("error", "native deploy failed"),
                    sweep_queue_after_release=False,
                )
                released["deploy"] = deploy_payload
                return released

        served_commit = commit
        served_cmd = env.get("served_commit_command")
        if served_cmd:
            served = subprocess.run(served_cmd, shell=True, text=True, capture_output=True, timeout=60)
            if served.returncode != 0:
                released = self.release(
                    lease_id,
                    actor,
                    "deploy_failed",
                    served.stderr[-1000:] or "served commit command failed",
                    sweep_queue_after_release=False,
                )
                released["deploy"] = deploy_payload
                return released
            served_commit = served.stdout.strip()
        if served_commit and not commit_matches_expected(served_commit, commit):
            released = self.release(
                lease_id,
                actor,
                "deploy_failed",
                f"served commit {served_commit} did not match expected {commit}",
                sweep_queue_after_release=False,
            )
            released["deploy"] = deploy_payload
            return released
        deployed = self.transition(lease_id, "mark_deployed_for_qa", actor, {"served_commit": served_commit, "deploy": deploy_payload})
        deployed["deploy"] = deploy_payload
        return deployed

    def lease_aware_deploy(self, environment_id: str, task_id: str, actor: str, source_repo_path: str,
                           agent_id: Optional[str] = None, agent_name: Optional[str] = None,
                           branch: Optional[str] = None, commit: Optional[str] = None,
                           services: str = "both", health_check: bool = True,
                           dry_run: bool = False, timeout_seconds: int = 1800,
                           queue_if_busy: bool = False, priority: int = 0,
                           callback_url: Optional[str] = None,
                           callback_api_key: Optional[str] = None,
                           database_policy: str = "preflight_and_apply") -> Dict[str, Any]:
        task_rejection = qa_fixture_task_rejection(task_id) if is_qa_fixture_task_id(task_id) else None
        if task_rejection:
            return task_rejection
        env = self._environment(environment_id)
        if not env:
            return {"ok": False, "error": "environment_not_found", "environment_id": environment_id}
        if not os.path.isdir(source_repo_path):
            return {"ok": False, "error": "source_repo_path_not_found", "source_repo_path": source_repo_path}
        database_policy = normalize_database_policy(database_policy)
        active_owner_check = self._validate_task_active_owner(str(task_id), callback_url, callback_api_key)
        if not active_owner_check.get("ok"):
            return active_owner_check
        deploy_metadata: Dict[str, Any] = {"database_policy": database_policy}
        authorization_metadata = self._active_owner_authorization_metadata(active_owner_check)
        if authorization_metadata:
            deploy_metadata["active_owner_authorization"] = authorization_metadata
        target_env = env
        target_environment_id = environment_id
        acquire = self.acquire(
            target_environment_id,
            task_id,
            actor,
            agent_id,
            agent_name,
            branch,
            commit,
            self._request_metadata(
                env,
                environment_id,
                source_repo_path,
                assigned_environment_id=target_environment_id,
                extra=deploy_metadata,
            ),
            supersede_same_task_review=True,
        )
        superseded_lease = acquire.get("superseded_lease")
        if not acquire.get("ok") and queue_if_busy and acquire.get("error") == "environment_busy":
            queued_because_owner = acquire.get("owner")
            available_env = self._available_environment_for_request(environment_id)
            if available_env:
                target_env = available_env
                target_environment_id = str(available_env["id"])
                acquire = self.acquire(
                    target_environment_id,
                    task_id,
                    actor,
                    agent_id,
                    agent_name,
                    branch,
                    commit,
                    self._request_metadata(
                        env,
                        environment_id,
                        source_repo_path,
                        assigned_environment_id=target_environment_id,
                        extra={**deploy_metadata, "selected_after_busy_owner": queued_because_owner},
                    ),
                    supersede_same_task_review=True,
                )
                superseded_lease = acquire.get("superseded_lease")
                if acquire.get("ok"):
                    acquire["requested_environment_id"] = environment_id
                    acquire["assigned_environment_id"] = target_environment_id
                elif acquire.get("error") != "environment_busy":
                    if superseded_lease:
                        acquire["superseded_lease"] = superseded_lease
                    return acquire
            if not acquire.get("ok"):
                if acquire.get("error") == "environment_busy" and not queued_because_owner:
                    queued_because_owner = acquire.get("owner")
                metadata = {**deploy_metadata, "queued_because_owner": queued_because_owner}
                if target_environment_id != environment_id:
                    metadata["alternate_busy_owner"] = acquire.get("owner")
                    metadata["attempted_assignment_environment_id"] = target_environment_id
                return self.enqueue_deploy_request(
                    environment_id,
                    task_id,
                    actor,
                    source_repo_path,
                    agent_id=agent_id,
                    agent_name=agent_name,
                    branch=branch,
                    commit=commit,
                    services=services,
                    health_check=health_check,
                    priority=priority,
                    callback_url=callback_url,
                    callback_api_key=callback_api_key,
                    metadata=metadata,
                    database_policy=database_policy,
                )
        if not acquire.get("ok"):
            if superseded_lease:
                acquire["superseded_lease"] = superseded_lease
            return acquire
        lease_id = acquire["lease"]["id"]
        deploying = self.transition(lease_id, "mark_deploying", actor)
        callback_queue = self._direct_deploy_callback_queue(
            environment_id=target_environment_id,
            requested_environment_id=environment_id,
            task_id=task_id,
            lease_id=lease_id,
            agent_id=agent_id,
            agent_name=agent_name,
            branch=branch,
            commit=commit,
            source_repo_path=source_repo_path,
            callback_url=callback_url,
            callback_api_key=callback_api_key,
        )
        callbacks: list[Dict[str, Any]] = []
        if not deploying.get("ok"):
            error = self._callback_error("mark_deploying", deploying)
            callbacks.append(self._send_callback(
                callback_queue,
                self._failure_event_name(error),
                f"Direct deploy lease {lease_id} failed before deployment.",
                lease=acquire.get("lease"),
                error=error,
            ))
            deploying["callbacks"] = callbacks
            if superseded_lease:
                deploying["superseded_lease"] = superseded_lease
            return deploying
        callbacks.append(self._send_callback(
            callback_queue,
            "dev_deploying",
            f"Direct deploy lease {lease_id} is deploying to {target_environment_id}.",
            lease=deploying.get("lease") or acquire.get("lease"),
        ))

        deployed = self._deploy_acquired_lease(
            target_env,
            lease_id,
            target_environment_id,
            task_id,
            actor,
            source_repo_path,
            commit,
            services,
            health_check,
            dry_run,
            timeout_seconds,
            database_policy,
        )
        latest_lease = self._lease(lease_id) or acquire.get("lease")
        if deployed.get("status") == "deployed_for_qa":
            callbacks.append(self._send_callback(
                callback_queue,
                "deployed_for_qa",
                f"Direct deploy lease {lease_id} completed and is ready for QA.",
                lease=latest_lease,
            ))
        else:
            error = self._callback_error("deploy", deployed)
            failure_summary = self._deploy_failure_summary(deployed)
            callbacks.append(self._send_callback(
                callback_queue,
                self._failure_event_name(error),
                f"Direct deploy lease {lease_id} failed: {failure_summary}.",
                lease=latest_lease,
                error=error,
            ))
        deployed["callbacks"] = callbacks
        if superseded_lease:
            deployed["superseded_lease"] = superseded_lease
        if target_environment_id != environment_id:
            deployed["requested_environment_id"] = environment_id
            deployed["assigned_environment_id"] = target_environment_id
        return deployed

    def _next_queued_request(self, environment_id: str) -> Optional[Dict[str, Any]]:
        env = self._environment(environment_id)
        if not env:
            return None
        rows = self.conn.execute(
            """
            SELECT *
            FROM deploy_queue
            WHERE status = 'queued'
            ORDER BY priority DESC, requested_at ASC, id ASC
            """,
        ).fetchall()
        for row in rows:
            queue = row_to_dict(row)
            if queue and self._queue_matches_environment(queue, env):
                return queue
        return None

    def sweep_deploy_queue(self, actor: str, environment_id: Optional[str] = None,
                           limit: int = 1, dry_run: bool = False,
                           timeout_seconds: int = 1800) -> Dict[str, Any]:
        if not actor:
            return {"ok": False, "error": "actor is required"}
        if limit <= 0:
            return {"ok": False, "error": "limit must be positive"}

        env_ids = [environment_id] if environment_id else [
            item["environment"]["id"] for item in self.status()["environments"]
        ]
        processed = []
        skipped = []
        for env_id in env_ids:
            if len(processed) >= limit:
                break
            env = self._environment(str(env_id))
            if not env:
                skipped.append({"environment_id": env_id, "reason": "environment_not_found"})
                continue
            active = self._active_lease(str(env_id))
            if active:
                skipped.append({"environment_id": env_id, "reason": "environment_busy", "lease": active})
                continue
            deploy_lock = inspect_native_deploy_lock(env)
            if deploy_lock and deploy_lock.get("blocks_deploy"):
                skipped.append({"environment_id": env_id, "reason": "native_deploy_lock", "native_deploy_lock": deploy_lock})
                continue
            queue = self._next_queued_request(str(env_id))
            if not queue:
                skipped.append({"environment_id": env_id, "reason": "queue_empty"})
                continue

            task_rejection = qa_fixture_task_rejection(queue["task_id"]) if is_qa_fixture_task_id(queue["task_id"]) else None
            if task_rejection:
                error = self._callback_error("reject_qa_fixture_task", task_rejection)
                failed_queue = self._set_queue_status(queue["id"], "failed", error=error)
                callbacks = [
                    self._send_callback(
                        failed_queue,
                        self._failure_event_name(error),
                        f"Queued deploy {queue['id']} rejected: {task_rejection['message']}",
                        error=error,
                    )
                ]
                processed.append({"queue": self._public_queue(failed_queue), "result": task_rejection, "callbacks": callbacks})
                continue

            acquire = self.acquire(
                str(env_id),
                str(queue["task_id"]),
                actor,
                queue.get("agent_id"),
                queue.get("agent_name"),
                queue.get("branch"),
                queue.get("commit_sha"),
                {
                    "source_repo_path": queue.get("source_repo_path"),
                    "queue_id": queue["id"],
                },
                supersede_same_task_review=True,
            )
            if not acquire.get("ok"):
                skipped.append({"environment_id": env_id, "queue": self._public_queue(queue), "reason": acquire.get("error"), "result": acquire})
                continue

            lease = acquire["lease"]
            requested_environment_id = self._queue_requested_environment_id(queue)
            queue = self._set_queue_status(
                queue["id"],
                "deploying",
                lease_id=lease["id"],
                environment_id=str(env_id),
                metadata_update={
                    "requested_environment_id": requested_environment_id,
                    "assigned_environment_id": str(env_id),
                },
            )
            callbacks = [
                self._send_callback(
                    queue,
                    "dev_deploying",
                    f"Queued deploy {queue['id']} is deploying to {env_id}.",
                    lease=lease,
                )
            ]

            deploying = self.transition(lease["id"], "mark_deploying", actor)
            if not deploying.get("ok"):
                error = self._callback_error("mark_deploying", deploying)
                failed_queue = self._set_queue_status(queue["id"], "failed", lease_id=lease["id"], error=error)
                callbacks.append(self._send_callback(
                    failed_queue,
                    self._failure_event_name(error),
                    f"Queued deploy {queue['id']} failed before deployment.",
                    lease=lease,
                    error=error,
                ))
                processed.append({"queue": self._public_queue(failed_queue), "result": deploying, "callbacks": callbacks})
                continue

            queue_metadata = queue.get("metadata") or {}
            queue_database_policy = normalize_database_policy(str(queue_metadata.get("database_policy") or "preflight_and_apply"))
            result = self._deploy_acquired_lease(
                env,
                lease["id"],
                str(env_id),
                str(queue["task_id"]),
                actor,
                str(queue["source_repo_path"]),
                queue.get("commit_sha"),
                str(queue.get("services") or "both"),
                bool(queue.get("health_check")),
                dry_run,
                timeout_seconds,
                queue_database_policy,
            )

            latest_lease = self._lease(lease["id"]) or lease
            if result.get("status") == "deployed_for_qa":
                final_queue = self._set_queue_status(queue["id"], "deployed", lease_id=lease["id"])
                callbacks.append(self._send_callback(
                    final_queue,
                    "deployed_for_qa",
                    f"Queued deploy {queue['id']} completed and is ready for QA.",
                    lease=latest_lease,
                ))
            else:
                error = self._callback_error("deploy", result)
                failure_summary = self._deploy_failure_summary(result)
                final_queue = self._set_queue_status(queue["id"], "failed", lease_id=lease["id"], error=error)
                callbacks.append(self._send_callback(
                    final_queue,
                    self._failure_event_name(error),
                    f"Queued deploy {queue['id']} failed: {failure_summary}.",
                    lease=latest_lease,
                    error=error,
                ))

            processed.append({"queue": self._public_queue(final_queue), "result": result, "callbacks": callbacks})

        return {"ok": True, "processed": processed, "skipped": skipped}

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

        try:
            completed = subprocess.run(
                command if isinstance(command, str) else shlex.join(command),
                shell=True,
                cwd=source_repo_path,
                env=proc_env,
                text=True,
                capture_output=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            return {
                "ok": False,
                "mode": "command",
                "error": "command_timed_out",
                "timeout_seconds": timeout_seconds,
                "stdout": stdout[-4000:],
                "stderr": stderr[-4000:],
            }
        return {
            "ok": completed.returncode == 0,
            "mode": "command",
            "returncode": completed.returncode,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
        }
