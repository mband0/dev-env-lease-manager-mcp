from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import fcntl
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import time
from typing import Any, Callable, Dict, Iterable, Optional
import urllib.request


Runner = Callable[..., subprocess.CompletedProcess[str]]


DEPLOY_FAILURE_EVENTS = {
    "database_backup_failed",
    "database_migration_failed",
    "database_integrity_failed",
    "api_boot_failed",
    "api_health_failed",
    "ui_health_failed",
    "process_restart_failed",
    "checkout_failed",
    "build_failed",
}

NATIVE_DEPLOY_LOCK_FILE = "deploy.lock"
LEGACY_NATIVE_DEPLOY_LOCK_DIR = "lock"
DEFAULT_DEPLOY_LOCK_STALE_AFTER_SECONDS = 7200


def normalize_database_policy(value: str | None) -> str:
    policy = (value or "preflight_and_apply").strip()
    allowed = {"none", "status_only", "preflight_only", "preflight_and_apply"}
    if policy not in allowed:
        raise NativeDeployError("invalid_database_policy", {"database_policy": policy, "allowed": sorted(allowed)})
    return policy


def command_failure_class(args: list[str]) -> str:
    if args and args[0] == "git":
        return "checkout_failed"
    if args[:2] == ["npm", "run"] or args[:1] == ["npm"]:
        return "build_failed"
    if args and args[0] == "pm2":
        if len(args) > 1 and args[1] == "start" and any(part.endswith("-api") for part in args):
            return "api_boot_failed"
        return "process_restart_failed"
    return "process_restart_failed"


def commit_matches_expected(actual: str, expected: Optional[str]) -> bool:
    if not expected:
        return True
    actual = actual.strip().lower()
    expected = expected.strip().lower()
    if not actual or not expected:
        return False
    if actual == expected:
        return True
    return len(expected) >= 7 and len(expected) < 40 and actual.startswith(expected)


@dataclass
class NativeDeployError(Exception):
    error: str
    detail: Optional[Dict[str, Any]] = None

    def payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"ok": False, "error": self.error}
        if self.error in DEPLOY_FAILURE_EVENTS:
            payload["failure_class"] = self.error
        if self.detail:
            payload.update(self.detail)
            if "failure_class" not in payload and self.detail.get("failure_class"):
                payload["failure_class"] = self.detail["failure_class"]
        return payload


def normalize_services(value: str) -> list[str]:
    normalized = (value or "both").strip()
    if normalized == "both":
        return ["api", "ui"]
    parts = [part.strip() for part in normalized.split(",") if part.strip()]
    if parts == ["ui", "api"]:
        parts = ["api", "ui"]
    if not parts or any(part not in {"api", "ui"} for part in parts):
        raise NativeDeployError("services must be one of: api, ui, both, api,ui")
    return parts


def expand_path(value: str) -> str:
    return str(Path(os.path.expandvars(os.path.expanduser(value))).resolve())


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def process_alive(pid: object) -> Optional[bool]:
    try:
        parsed = int(pid)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    try:
        os.kill(parsed, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def native_deploy_state_dir(env: Dict[str, Any]) -> Path:
    metadata = env.get("metadata") or {}
    return Path(expand_path(str(metadata.get("state_dir") or "~/.agent-hq-dev-deploy")))


def native_deploy_lock_stale_after_seconds(env: Dict[str, Any]) -> int:
    metadata = env.get("metadata") or {}
    raw = metadata.get("deploy_lock_stale_after_seconds") or env.get("stale_after_seconds")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = DEFAULT_DEPLOY_LOCK_STALE_AFTER_SECONDS
    return value if value > 0 else DEFAULT_DEPLOY_LOCK_STALE_AFTER_SECONDS


def production_deploy_state_dir(env: Dict[str, Any]) -> Path:
    metadata = env.get("metadata") or {}
    return Path(expand_path(str(metadata.get("production_state_dir") or "~/.agent-hq-prod-deploy")))


def production_deploy_lock_env(env: Dict[str, Any]) -> Dict[str, Any]:
    metadata = dict(env.get("metadata") or {})
    metadata["state_dir"] = str(production_deploy_state_dir(env))
    return {**env, "metadata": metadata}


def _read_lock_metadata(path: Path) -> Dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return {}
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw[:1000]}
    return parsed if isinstance(parsed, dict) else {"raw": parsed}


def _lock_file_is_held(path: Path) -> bool:
    try:
        with path.open("a+", encoding="utf-8") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in (errno.EACCES, errno.EAGAIN):
                    return True
                raise
            finally:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
    except FileNotFoundError:
        return False
    return False


def _lock_age_seconds(acquired_at: Optional[str], stat_mtime: float, now: Optional[datetime] = None) -> Optional[int]:
    now_dt = now or datetime.now(timezone.utc)
    acquired = parse_time(acquired_at)
    if acquired is None:
        acquired = datetime.fromtimestamp(stat_mtime, timezone.utc)
    return max(0, int((now_dt - acquired).total_seconds()))


def _lock_entry(path: Path, kind: str, stale_after_seconds: int, now: Optional[datetime] = None) -> Dict[str, Any]:
    metadata = _read_lock_metadata(path) if path.is_file() else {}
    stat = path.stat()
    acquired_at = metadata.get("acquired_at") if isinstance(metadata.get("acquired_at"), str) else None
    age_seconds = _lock_age_seconds(acquired_at, stat.st_mtime, now)
    pid_alive = process_alive(metadata.get("pid")) if metadata else None
    locked = _lock_file_is_held(path) if kind == "file" else False
    stale = (
        (pid_alive is False)
        or (age_seconds is not None and age_seconds > stale_after_seconds)
        or (kind == "legacy_dir" and age_seconds is not None and age_seconds > stale_after_seconds)
    )
    return {
        "kind": kind,
        "path": str(path),
        "locked": locked,
        "blocks_deploy": locked or kind == "legacy_dir",
        "stale": stale,
        "age_seconds": age_seconds,
        "stale_after_seconds": stale_after_seconds,
        "pid_alive": pid_alive,
        "metadata": metadata,
    }


def inspect_native_deploy_lock(env: Dict[str, Any], now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    state_dir = native_deploy_state_dir(env)
    stale_after_seconds = native_deploy_lock_stale_after_seconds(env)
    entries: list[Dict[str, Any]] = []
    lock_file = state_dir / NATIVE_DEPLOY_LOCK_FILE
    legacy_lock_dir = state_dir / LEGACY_NATIVE_DEPLOY_LOCK_DIR

    if lock_file.exists():
        entries.append(_lock_entry(lock_file, "file", stale_after_seconds, now))
    if legacy_lock_dir.exists():
        entries.append(_lock_entry(legacy_lock_dir, "legacy_dir", stale_after_seconds, now))
    if not entries:
        return None

    return {
        "state_dir": str(state_dir),
        "blocks_deploy": any(bool(entry.get("blocks_deploy")) for entry in entries),
        "stale": any(bool(entry.get("stale")) for entry in entries),
        "entries": entries,
    }


def sweep_native_deploy_lock(env: Dict[str, Any], actor: str, reason: str, force: bool = False) -> Dict[str, Any]:
    if not actor:
        return {"ok": False, "error": "actor is required"}
    if not reason:
        return {"ok": False, "error": "reason is required"}

    state_dir = native_deploy_state_dir(env)
    lock = inspect_native_deploy_lock(env)
    if not lock:
        return {"ok": True, "environment_id": env.get("id"), "removed": [], "skipped": []}

    removed: list[Dict[str, Any]] = []
    skipped: list[Dict[str, Any]] = []
    for entry in lock["entries"]:
        path = Path(str(entry["path"]))
        kind = str(entry["kind"])
        locked = bool(entry.get("locked"))
        stale = bool(entry.get("stale"))
        if locked:
            skipped.append({**entry, "reason": "lock_is_currently_held"})
            continue
        if not (stale or force):
            skipped.append({**entry, "reason": "lock_not_stale"})
            continue
        try:
            if kind == "legacy_dir":
                path.rmdir()
            else:
                path.unlink()
            removed.append({**entry, "actor": actor, "reason": reason})
        except OSError as exc:
            skipped.append({**entry, "reason": "remove_failed", "error": str(exc)})

    return {
        "ok": True,
        "environment_id": env.get("id"),
        "state_dir": str(state_dir),
        "removed": removed,
        "skipped": skipped,
    }


class NativeDeployLock:
    def __init__(self, state_dir: str, metadata: Dict[str, Any], env: Dict[str, Any]):
        self.state_dir = Path(state_dir)
        self.metadata = metadata
        self.env = env
        self.path = self.state_dir / NATIVE_DEPLOY_LOCK_FILE
        self.handle: Optional[Any] = None

    def __enter__(self) -> "NativeDeployLock":
        self.state_dir.mkdir(parents=True, exist_ok=True)
        legacy_lock_dir = self.state_dir / LEGACY_NATIVE_DEPLOY_LOCK_DIR
        if legacy_lock_dir.exists():
            raise NativeDeployError("deploy lock already held", {"deploy_lock": inspect_native_deploy_lock(self.env)})

        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            self.handle = None
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise NativeDeployError("deploy lock already held", {"deploy_lock": inspect_native_deploy_lock(self.env)})
            raise

        payload = {
            **self.metadata,
            "pid": os.getpid(),
            "acquired_at": utc_now(),
            "lock_file": str(self.path),
        }
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(json.dumps(payload, indent=2, sort_keys=True))
        self.handle.write("\n")
        self.handle.flush()
        os.fsync(self.handle.fileno())
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if not self.handle:
            return
        try:
            self.handle.seek(0)
            self.handle.truncate()
            self.handle.flush()
            os.fsync(self.handle.fileno())
        finally:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


class NativeDevDeployer:
    """Native Python port of Agent HQ's deploy_dev_worktree capability."""

    def __init__(self, runner: Runner | None = None):
        self.runner = runner or subprocess.run

    def _run(
        self,
        args: list[str],
        cwd: str | None = None,
        env: Dict[str, str] | None = None,
        timeout: int = 1800,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        try:
            completed = self.runner(
                args,
                cwd=cwd,
                env=env,
                text=True,
                capture_output=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
            failure_class = command_failure_class(args)
            raise NativeDeployError(
                failure_class,
                {
                    "failure_class": failure_class,
                    "command": args,
                    "cwd": cwd,
                    "timeout_seconds": timeout,
                    "stdout": stdout[-4000:],
                    "stderr": stderr[-4000:],
                },
            )
        if check and completed.returncode != 0:
            failure_class = command_failure_class(args)
            raise NativeDeployError(
                failure_class,
                {
                    "failure_class": failure_class,
                    "command": args,
                    "cwd": cwd,
                    "returncode": completed.returncode,
                    "stdout": completed.stdout[-4000:],
                    "stderr": completed.stderr[-4000:],
                },
            )
        return completed

    def _git(self, repo_path: str, *args: str, timeout: int = 1800, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self._run(["git", "-C", repo_path, *args], timeout=timeout, check=check)

    def _capture_pm2(self, name: str) -> Optional[Dict[str, Any]]:
        completed = self._run(["pm2", "jlist"], check=False, timeout=60)
        if completed.returncode != 0:
            return None
        try:
            items = json.loads(completed.stdout or "[]")
        except json.JSONDecodeError:
            return None
        for item in items:
            env = item.get("pm2_env", {}) if isinstance(item, dict) else {}
            if env.get("name") == name or item.get("name") == name:
                return {
                    "cwd": env.get("pm_cwd"),
                    "args": env.get("args") or [],
                    "exec_path": env.get("pm_exec_path"),
                    "name": item.get("name") or env.get("name"),
                }
        return None

    def _copy_if_missing(self, canonical_root: str, dev_repo_path: str, relative_path: str) -> None:
        src = Path(canonical_root) / relative_path
        dest = Path(dev_repo_path) / relative_path
        if src.exists() and not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            if src.is_dir():
                shutil.copytree(src, dest)
            else:
                shutil.copy2(src, dest)

    def _require_file(self, path: str, error: str) -> None:
        if not Path(path).is_file():
            raise NativeDeployError(error)

    def _ensure_package_scripts(self, dev_repo_path: str) -> None:
        errors: list[str] = []
        for package_name, scripts in (
            ("api/package.json", ("build", "start")),
            ("ui/package.json", ("build", "start-dev")),
        ):
            package_path = Path(dev_repo_path) / package_name
            try:
                package = json.loads(package_path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise NativeDeployError(f"{package_name} could not be read", {"detail": str(exc)})
            package_scripts = package.get("scripts", {})
            for script in scripts:
                if script not in package_scripts:
                    errors.append(f"{package_name} missing {script} script")
        if errors:
            raise NativeDeployError("package_scripts_missing", {"errors": errors})

    def _dependency_fingerprint(self, package_dir: str) -> str:
        digest = hashlib.sha256()
        for filename in ("package.json", "package-lock.json"):
            path = Path(package_dir) / filename
            digest.update(filename.encode("utf-8"))
            digest.update(b"\0")
            if path.exists():
                digest.update(path.read_bytes())
            digest.update(b"\0")
        return digest.hexdigest()

    def _direct_dependencies(self, package_dir: str) -> list[str]:
        package_path = Path(package_dir) / "package.json"
        try:
            package = json.loads(package_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise NativeDeployError("package_json_unreadable", {"package_dir": package_dir, "detail": str(exc)})

        names: list[str] = []
        for section in ("dependencies", "devDependencies"):
            entries = package.get(section) or {}
            if isinstance(entries, dict):
                names.extend(str(name) for name in entries.keys())
        return sorted(set(names))

    def _missing_direct_dependencies(self, package_dir: str) -> list[str]:
        node_modules = Path(package_dir) / "node_modules"
        missing: list[str] = []
        for name in self._direct_dependencies(package_dir):
            dependency_path = node_modules.joinpath(*name.split("/"))
            if not dependency_path.exists():
                missing.append(name)
        return missing

    def _ensure_deps(self, package_dir: str, timeout_seconds: int) -> Dict[str, Any]:
        node_modules = Path(package_dir) / "node_modules"
        marker = node_modules / ".agent-hq-deploy-deps.sha256"
        fingerprint = self._dependency_fingerprint(package_dir)
        marker_value = marker.read_text(encoding="utf-8").strip() if marker.exists() else None
        missing_before = self._missing_direct_dependencies(package_dir) if node_modules.is_dir() else []
        install_reason: str | None = None

        if not node_modules.is_dir():
            install_reason = "node_modules_missing"
        elif marker_value != fingerprint:
            install_reason = "dependency_manifest_changed"
        elif missing_before:
            install_reason = "direct_dependencies_missing"

        install_command: list[str] | None = None
        if install_reason:
            install_command = ["npm", "ci", "--include=dev"] if (Path(package_dir) / "package-lock.json").is_file() else ["npm", "install", "--include=dev"]
            self._run(install_command, cwd=package_dir, timeout=timeout_seconds)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(f"{fingerprint}\n", encoding="utf-8")

        missing_after = self._missing_direct_dependencies(package_dir)
        if missing_after:
            raise NativeDeployError(
                "package_dependencies_incomplete",
                {
                    "package_dir": package_dir,
                    "missing_dependencies": missing_after,
                    "install_reason": install_reason,
                    "install_command": install_command,
                },
            )

        return {
            "package_dir": package_dir,
            "installed": bool(install_reason),
            "install_reason": install_reason,
            "install_command": install_command,
            "missing_before": missing_before,
        }

    def _prepare_dev_checkout(self, dev_repo_path: str, timeout_seconds: int) -> Dict[str, Any]:
        status = self._git(dev_repo_path, "status", "--porcelain=v1", "--untracked-files=all").stdout.strip()
        diff = self._git(dev_repo_path, "diff", "--quiet", check=False)
        staged = self._git(dev_repo_path, "diff", "--cached", "--quiet", check=False)
        tracked_dirty = diff.returncode != 0 or staged.returncode != 0
        untracked = self._git(dev_repo_path, "ls-files", "--others", "--exclude-standard").stdout.strip().splitlines()
        cleanup: Dict[str, Any] = {
            "cleaned": False,
            "tracked_dirty": tracked_dirty,
            "untracked_files": untracked,
            "status": status.splitlines() if status else [],
        }
        if tracked_dirty:
            reset = self._git(dev_repo_path, "reset", "--hard", timeout=timeout_seconds)
            cleanup["cleaned"] = True
            cleanup["reset_stdout"] = reset.stdout[-1000:]
        if untracked:
            clean = self._git(dev_repo_path, "clean", "-ffd", timeout=timeout_seconds)
            cleanup["cleaned"] = True
            cleanup["clean_stdout"] = clean.stdout[-1000:]
        return cleanup

    def _health_check(self, services: Iterable[str], api_port: str, ui_port: str) -> Dict[str, Dict[str, Any]]:
        checks = []
        if "api" in services:
            checks.append(("api", f"http://127.0.0.1:{api_port}/health"))
        if "ui" in services:
            checks.append(("ui", f"http://127.0.0.1:{ui_port}"))

        results: Dict[str, Dict[str, Any]] = {}
        for name, url in checks:
            ok = False
            detail: Any = None
            for _ in range(45):
                try:
                    with urllib.request.urlopen(url, timeout=2) as response:
                        detail = response.status
                        ok = True
                        break
                except Exception as exc:
                    detail = str(exc)
                    time.sleep(1)
            results[name] = {"ok": ok, "detail": detail, "url": url}
        if not all(result["ok"] for result in results.values()):
            failure_class = "api_health_failed" if not results.get("api", {}).get("ok", True) else "ui_health_failed"
            raise NativeDeployError(failure_class, {"failure_class": failure_class, "health": results})
        return results

    def _package_script(self, package_dir: str, script: str) -> bool:
        package_path = Path(package_dir) / "package.json"
        try:
            package = json.loads(package_path.read_text(encoding="utf-8"))
        except Exception:
            return False
        scripts = package.get("scripts") or {}
        return isinstance(scripts, dict) and script in scripts

    def _run_migration_script(
        self,
        api_dir: str,
        script: str,
        db_path: str,
        api_port: str,
        timeout_seconds: int,
    ) -> Dict[str, Any]:
        env = os.environ.copy()
        env.update({
            "AGENT_HQ_DB_PATH": db_path,
            "PORT": api_port,
            "AGENT_HQ_APP_COMMIT": env.get("AGENT_HQ_APP_COMMIT", ""),
        })
        try:
            completed = self._run(["npm", "run", script], cwd=api_dir, env=env, timeout=timeout_seconds)
        except NativeDeployError as exc:
            payload = exc.payload()
            raise NativeDeployError(
                "database_migration_failed",
                {
                    "failure_class": "database_migration_failed",
                    "phase": script,
                    "db_path": db_path,
                    "script": script,
                    "command_error": payload,
                },
            )
        return {
            "script": script,
            "db_path": db_path,
            "stdout": completed.stdout[-4000:],
            "stderr": completed.stderr[-4000:],
        }

    def _sqlite_integrity_check(self, db_path: str, phase: str) -> Dict[str, Any]:
        try:
            with sqlite3.connect(db_path) as conn:
                rows = [row[0] for row in conn.execute("PRAGMA integrity_check").fetchall()]
        except Exception as exc:
            raise NativeDeployError(
                "database_integrity_failed",
                {"failure_class": "database_integrity_failed", "phase": phase, "db_path": db_path, "detail": str(exc)},
            )
        ok = rows == ["ok"]
        if not ok:
            raise NativeDeployError(
                "database_integrity_failed",
                {"failure_class": "database_integrity_failed", "phase": phase, "db_path": db_path, "integrity": rows},
            )
        return {"ok": True, "db_path": db_path, "integrity": rows}

    def _copy_sqlite_snapshot(self, source_path: str, target_path: str, phase: str) -> None:
        try:
            Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(f"file:{source_path}?mode=ro", uri=True) as source_conn:
                with sqlite3.connect(target_path) as target_conn:
                    source_conn.backup(target_conn)
        except Exception as exc:
            raise NativeDeployError(
                "database_backup_failed",
                {
                    "failure_class": "database_backup_failed",
                    "phase": phase,
                    "db_path": source_path,
                    "target_path": target_path,
                    "detail": str(exc),
                },
            )

    def _backup_database(self, db_path: str, backup_dir: str, source_sha: str) -> str | None:
        source = Path(db_path)
        if not source.exists():
            return None
        backup_path = Path(backup_dir) / f"{source.name}.{int(time.time())}.{source_sha[:12]}.bak"
        self._copy_sqlite_snapshot(db_path, str(backup_path), "backup")
        return str(backup_path)

    def _run_database_migrations(
        self,
        api_dir: str,
        db_path: str,
        backup_dir: str,
        api_port: str,
        source_sha: str,
        database_policy: str,
        timeout_seconds: int,
    ) -> Dict[str, Any]:
        policy = normalize_database_policy(database_policy)
        result: Dict[str, Any] = {"policy": policy, "db_path": db_path, "applied": False, "preflight": None}
        if policy == "none":
            return result

        has_status = self._package_script(api_dir, "db:migrate:status")
        has_preflight = self._package_script(api_dir, "db:migrate:preflight")
        has_migrate = self._package_script(api_dir, "db:migrate")
        if not any((has_status, has_preflight, has_migrate)):
            result["skipped"] = "migration_scripts_missing"
            return result

        if has_status:
            result["status_before"] = self._run_migration_script(api_dir, "db:migrate:status", db_path, api_port, timeout_seconds)
        if policy == "status_only":
            return result

        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        preflight_script = "db:migrate:preflight" if has_preflight else "db:migrate"
        with tempfile.TemporaryDirectory(prefix="dev-env-db-preflight-") as tmp_dir:
            preflight_db = str(Path(tmp_dir) / Path(db_path).name)
            if Path(db_path).exists():
                self._copy_sqlite_snapshot(db_path, preflight_db, "preflight")
            result["preflight"] = self._run_migration_script(api_dir, preflight_script, preflight_db, api_port, timeout_seconds)
            result["preflight_integrity"] = self._sqlite_integrity_check(preflight_db, "preflight")

        if policy == "preflight_only":
            return result
        if not has_migrate:
            raise NativeDeployError(
                "database_migration_failed",
                {"failure_class": "database_migration_failed", "phase": "apply", "detail": "api/package.json missing db:migrate script"},
            )

        backup_path = self._backup_database(db_path, backup_dir, source_sha)
        result["backup_path"] = backup_path
        result["apply"] = self._run_migration_script(api_dir, "db:migrate", db_path, api_port, timeout_seconds)
        result["apply_integrity"] = self._sqlite_integrity_check(db_path, "apply")
        result["applied"] = True
        if has_status:
            result["status_after"] = self._run_migration_script(api_dir, "db:migrate:status", db_path, api_port, timeout_seconds)
        return result

    def deploy(
        self,
        env: Dict[str, Any],
        source_repo_path: str,
        services: str = "both",
        health_check: bool = True,
        expected_commit: Optional[str] = None,
        timeout_seconds: int = 1800,
        database_policy: str = "preflight_and_apply",
        lock_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        metadata = env.get("metadata") or {}
        dev_repo_path = env.get("repo_path")
        if not dev_repo_path:
            raise NativeDeployError("environment repo_path is required for native deploy")

        canonical_root = expand_path(str(metadata.get("canonical_root") or source_repo_path))
        state_dir = expand_path(str(metadata.get("state_dir") or "~/.agent-hq-dev-deploy"))
        state_file = expand_path(str(metadata.get("state_file") or str(Path(state_dir) / "current-target.json")))
        api_name = str(metadata.get("pm2_api") or "agent-hq-dev-api")
        ui_name = str(metadata.get("pm2_ui") or "agent-hq-dev-ui")
        api_port = str(metadata.get("api_port") or 3511)
        ui_port = str(metadata.get("ui_port") or 3510)
        dev_db_path = expand_path(str(metadata.get("dev_db_path") or str(Path(dev_repo_path) / "agent-hq-dev.db")))
        backup_dir = expand_path(str(metadata.get("backup_dir") or str(Path(state_dir) / "db-backups")))
        database_policy = normalize_database_policy(str(metadata.get("database_policy") or database_policy))
        service_list = normalize_services(services)

        source_repo_path = expand_path(source_repo_path)
        dev_repo_path = expand_path(str(dev_repo_path))

        lock_payload = {
            "environment_id": env.get("id"),
            "source_repo_path": source_repo_path,
            "expected_commit": expected_commit,
            "services": service_list,
            **(lock_metadata or {}),
        }
        with NativeDeployLock(state_dir, lock_payload, env):
            if not Path(source_repo_path).is_dir():
                raise NativeDeployError("repo_path does not exist")
            if not Path(canonical_root).is_dir():
                raise NativeDeployError("canonical repo root does not exist")

            source_root = self._git(source_repo_path, "rev-parse", "--show-toplevel").stdout.strip()
            self._require_file(str(Path(source_root) / "package.json"), "source repo root missing package.json")
            self._require_file(str(Path(source_root) / "api/package.json"), "source api/package.json missing")
            self._require_file(str(Path(source_root) / "ui/package.json"), "source ui/package.json missing")

            source_status = self._git(source_root, "status", "--porcelain=v1", "--untracked-files=all").stdout.strip()
            if source_status:
                raise NativeDeployError(
                    "repo_path has uncommitted or untracked changes; commit or clean the workspace before deploying to dev",
                    {"status": source_status.splitlines()},
                )

            source_sha = self._git(source_root, "rev-parse", "HEAD").stdout.strip()
            if not commit_matches_expected(source_sha, expected_commit):
                raise NativeDeployError(
                    "source HEAD does not match expected commit",
                    {"source_sha": source_sha, "expected_commit": expected_commit},
                )
            source_branch = self._git(source_root, "branch", "--show-current", check=False).stdout.strip() or None

            if not Path(dev_repo_path).exists():
                Path(dev_repo_path).parent.mkdir(parents=True, exist_ok=True)
                self._run(["git", "clone", canonical_root, dev_repo_path], timeout=timeout_seconds)
            if not Path(dev_repo_path).is_dir():
                raise NativeDeployError("dev_repo_path is not a directory")
            self._git(dev_repo_path, "rev-parse", "--show-toplevel")
            dev_predeploy_cleanup = self._prepare_dev_checkout(dev_repo_path, timeout_seconds)

            for relative_path in ("agent-hq-dev.db", ".env", ".env.local", "api/.env", "api/.env.local", "ui/.env", "ui/.env.local"):
                self._copy_if_missing(canonical_root, dev_repo_path, relative_path)

            self._require_file(str(Path(dev_repo_path) / "package.json"), "dev repo root missing package.json")
            self._require_file(str(Path(dev_repo_path) / "api/package.json"), "dev repo api/package.json missing")
            self._require_file(str(Path(dev_repo_path) / "ui/package.json"), "dev repo ui/package.json missing")
            self._ensure_package_scripts(dev_repo_path)

            previous_dev_sha = self._git(dev_repo_path, "rev-parse", "HEAD", check=False).stdout.strip() or None
            previous_api = self._capture_pm2(api_name)
            previous_ui = self._capture_pm2(ui_name)

            self._git(dev_repo_path, "fetch", "--no-tags", source_root, "HEAD", timeout=timeout_seconds)
            fetch_sha = self._git(dev_repo_path, "rev-parse", "FETCH_HEAD").stdout.strip()
            if fetch_sha != source_sha:
                raise NativeDeployError("failed to fetch the exact source HEAD into the dev repo", {"fetch_sha": fetch_sha, "source_sha": source_sha})
            self._git(dev_repo_path, "reset", "--hard", source_sha, timeout=timeout_seconds)

            shutil.rmtree(Path(dev_repo_path) / "api/dist", ignore_errors=True)
            shutil.rmtree(Path(dev_repo_path) / "ui/.next", ignore_errors=True)

            dependency_setup: Dict[str, Any] = {}
            if "api" in service_list:
                api_dir = str(Path(dev_repo_path) / "api")
                dependency_setup["api"] = self._ensure_deps(api_dir, timeout_seconds)
                self._run(["npm", "run", "build"], cwd=api_dir, timeout=timeout_seconds)
                migration_setup = self._run_database_migrations(
                    api_dir,
                    dev_db_path,
                    backup_dir,
                    api_port,
                    source_sha,
                    database_policy,
                    timeout_seconds,
                )
                self._run(["pm2", "delete", api_name], timeout=60, check=False)
                api_env = os.environ.copy()
                api_env.update({
                    "PORT": api_port,
                    "AGENT_HQ_DB_PATH": dev_db_path,
                    "OPENCLAW_GATEWAY_URL": os.environ.get("OPENCLAW_GATEWAY_URL", "https://127.0.0.1:18789"),
                    "OPENCLAW_HOOKS_TOKEN": os.environ.get("OPENCLAW_HOOKS_TOKEN", ""),
                    "GATEWAY_TOKEN": os.environ.get("GATEWAY_TOKEN", ""),
                    "GATEWAY_WS_URL": os.environ.get("GATEWAY_WS_URL", "wss://127.0.0.1:18789"),
                    "GATEWAY_URL": os.environ.get("GATEWAY_URL", "https://localhost:18789"),
                    "NODE_TLS_REJECT_UNAUTHORIZED": os.environ.get("NODE_TLS_REJECT_UNAUTHORIZED", "0"),
                })
                self._run(["pm2", "start", "npm", "--name", api_name, "--cwd", api_dir, "--", "start"], env=api_env, timeout=timeout_seconds)

            if "ui" in service_list:
                ui_dir = str(Path(dev_repo_path) / "ui")
                dependency_setup["ui"] = self._ensure_deps(ui_dir, timeout_seconds)
                self._run(["npm", "run", "build"], cwd=ui_dir, timeout=timeout_seconds)
                self._run(["pm2", "delete", ui_name], timeout=60, check=False)
                ui_env = os.environ.copy()
                ui_env.update({
                    "PORT": ui_port,
                    "AGENT_HQ_INTERNAL_BASE_URL": f"http://localhost:{api_port}",
                    "NEXT_PUBLIC_API_URL": f"http://localhost:{api_port}",
                })
                self._run(["pm2", "start", "npm", "--name", ui_name, "--cwd", ui_dir, "--", "run", "start-dev"], env=ui_env, timeout=timeout_seconds)

            health = self._health_check(service_list, api_port, ui_port) if health_check else {}
            state = {
                "previous": {
                    "dev_sha": previous_dev_sha,
                    "api": previous_api,
                    "ui": previous_ui,
                },
                "current": {
                    "source_path": source_root,
                    "source_branch": source_branch,
                    "source_sha": source_sha,
                    "dev_repo_path": dev_repo_path,
                    "services": service_list,
                    "api": {"cwd": f"{dev_repo_path}/api", "name": api_name, "args": ["start"]},
                    "ui": {"cwd": f"{dev_repo_path}/ui", "name": ui_name, "args": ["run", "start-dev"]},
                    "dev_predeploy_cleanup": dev_predeploy_cleanup,
                    "dependency_setup": dependency_setup,
                    "database_policy": database_policy,
                    "database_migration": migration_setup if "api" in service_list else None,
                },
            }
            Path(state_file).parent.mkdir(parents=True, exist_ok=True)
            Path(state_file).write_text(json.dumps(state, indent=2), encoding="utf-8")
            return {
                "ok": True,
                "mode": "native",
                "source_path": source_root,
                "source_branch": source_branch,
                "source_sha": source_sha,
                "dev_repo_path": dev_repo_path,
                "state_file": state_file,
                "services": service_list,
                "dev_predeploy_cleanup": dev_predeploy_cleanup,
                "dependency_setup": dependency_setup,
                "database_policy": database_policy,
                "database_migration": migration_setup if "api" in service_list else None,
                "health": health,
            }


class NativeProductionDeployer(NativeDevDeployer):
    """Exact-commit production deployer for Agent HQ-style repos."""

    DEFAULT_ALLOWED_UNTRACKED = [
        ".env",
        ".env.*",
        "api/.env",
        "api/.env.*",
        "ui/.env",
        "ui/.env.*",
        "node_modules/**",
        "api/node_modules/**",
        "ui/node_modules/**",
        "api/dist/**",
        "ui/.next/**",
        "ui/out/**",
        "agent-hq*.db",
        "*.sqlite",
        "*.sqlite3",
        "*.log",
        "logs/**",
    ]

    def _allowed_untracked_patterns(self, metadata: Dict[str, Any]) -> list[str]:
        configured = metadata.get("production_allowed_untracked")
        if isinstance(configured, list):
            return [str(item) for item in configured if str(item).strip()]
        return list(self.DEFAULT_ALLOWED_UNTRACKED)

    def _checkout_safety(self, repo_path: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        status = self._git(repo_path, "status", "--porcelain=v1", "--untracked-files=all").stdout.strip()
        diff = self._git(repo_path, "diff", "--quiet", check=False)
        staged = self._git(repo_path, "diff", "--cached", "--quiet", check=False)
        untracked = [
            line.strip()
            for line in self._git(repo_path, "ls-files", "--others", "--exclude-standard").stdout.splitlines()
            if line.strip()
        ]
        patterns = self._allowed_untracked_patterns(metadata)
        disallowed_untracked = [
            path for path in untracked
            if not any(fnmatch.fnmatch(path, pattern) for pattern in patterns)
        ]
        safety = {
            "status": status.splitlines() if status else [],
            "tracked_dirty": diff.returncode != 0 or staged.returncode != 0,
            "untracked_files": untracked,
            "allowed_untracked_patterns": patterns,
            "disallowed_untracked": disallowed_untracked,
        }
        if safety["tracked_dirty"]:
            raise NativeDeployError("production_checkout_dirty", {**safety, "failure_class": "checkout_failed"})
        if disallowed_untracked:
            raise NativeDeployError("production_checkout_untracked_files", {**safety, "failure_class": "checkout_failed"})
        return safety

    def _verify_expected_commit(
        self,
        repo_path: str,
        remote: str,
        branch: str,
        expected_commit: str,
        timeout_seconds: int,
    ) -> Dict[str, Any]:
        if not expected_commit:
            raise NativeDeployError("expected_commit is required", {"failure_class": "checkout_failed"})
        self._git(repo_path, "fetch", "--no-tags", remote, branch, timeout=timeout_seconds)
        resolved = self._git(repo_path, "rev-parse", f"{expected_commit}^{{commit}}", timeout=timeout_seconds).stdout.strip()
        remote_ref = f"{remote}/{branch}"
        ancestor = self._git(repo_path, "merge-base", "--is-ancestor", resolved, remote_ref, timeout=timeout_seconds, check=False)
        if ancestor.returncode != 0:
            raise NativeDeployError(
                "expected_commit_not_reachable_from_remote",
                {
                    "failure_class": "checkout_failed",
                    "expected_commit": expected_commit,
                    "resolved_commit": resolved,
                    "remote_ref": remote_ref,
                },
            )
        remote_head = self._git(repo_path, "rev-parse", remote_ref, timeout=timeout_seconds).stdout.strip()
        return {
            "expected_commit": expected_commit,
            "resolved_commit": resolved,
            "remote": remote,
            "branch": branch,
            "remote_ref": remote_ref,
            "remote_head": remote_head,
        }

    def _ensure_production_package_scripts(self, repo_path: str, services: list[str]) -> None:
        errors: list[str] = []
        checks = [("api/package.json", ("build", "start"))] if "api" in services else []
        if "ui" in services:
            checks.append(("ui/package.json", ("build", "start")))
        for package_name, scripts in checks:
            package_path = Path(repo_path) / package_name
            try:
                package = json.loads(package_path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise NativeDeployError(f"{package_name} could not be read", {"detail": str(exc)})
            package_scripts = package.get("scripts", {})
            for script in scripts:
                if script not in package_scripts:
                    errors.append(f"{package_name} missing {script} script")
        if errors:
            raise NativeDeployError("package_scripts_missing", {"errors": errors, "failure_class": "build_failed"})

    def deploy(
        self,
        env: Dict[str, Any],
        services: str = "both",
        health_check: bool = True,
        expected_commit: Optional[str] = None,
        timeout_seconds: int = 1800,
        database_policy: str = "preflight_and_apply",
        dry_run: bool = True,
        lock_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        metadata = env.get("metadata") or {}
        production_command = metadata.get("production_deploy_command")
        production_repo_path = expand_path(str(metadata.get("production_repo_path") or metadata.get("canonical_root") or "~/agent-hq"))
        state_dir = expand_path(str(metadata.get("production_state_dir") or "~/.agent-hq-prod-deploy"))
        state_file = expand_path(str(metadata.get("production_state_file") or str(Path(state_dir) / "current-production-target.json")))
        remote = str(metadata.get("production_remote") or "origin")
        branch = str(metadata.get("production_branch") or "main")
        api_name = str(metadata.get("production_pm2_api") or "agent-hq-api")
        ui_name = str(metadata.get("production_pm2_ui") or "agent-hq-ui")
        api_port = str(metadata.get("production_api_port") or 3501)
        ui_port = str(metadata.get("production_ui_port") or 3500)
        db_path = expand_path(str(metadata.get("production_db_path") or str(Path(production_repo_path) / "agent-hq.db")))
        backup_dir = expand_path(str(metadata.get("production_backup_dir") or str(Path(state_dir) / "db-backups")))
        database_policy = normalize_database_policy(str(metadata.get("production_database_policy") or metadata.get("database_policy") or database_policy))
        service_list = normalize_services(services)

        if not Path(production_repo_path).is_dir():
            raise NativeDeployError("production_repo_path_not_found", {"production_repo_path": production_repo_path, "failure_class": "checkout_failed"})

        lock_payload = {
            "environment_id": env.get("id"),
            "production_repo_path": production_repo_path,
            "expected_commit": expected_commit,
            "services": service_list,
            "dry_run": dry_run,
            **(lock_metadata or {}),
        }
        lock_env = production_deploy_lock_env(env)

        def run_plan() -> Dict[str, Any]:
            self._git(production_repo_path, "rev-parse", "--show-toplevel", timeout=timeout_seconds)
            self._require_file(str(Path(production_repo_path) / "package.json"), "production repo root missing package.json")
            if production_command:
                checkout_safety = self._checkout_safety(production_repo_path, metadata)
                commit_check = self._verify_expected_commit(production_repo_path, remote, branch, str(expected_commit or ""), timeout_seconds)
                previous_sha = self._git(production_repo_path, "rev-parse", "HEAD", timeout=timeout_seconds, check=False).stdout.strip() or None
                planned_actions = [
                    {"action": "fetch", "remote": remote, "branch": branch},
                    {"action": "reset_exact_commit", "commit": commit_check["resolved_commit"]},
                    {"action": "run_production_command", "command": str(production_command)},
                    {"action": "health_check", "enabled": health_check},
                ]
                base = {
                    "ok": True,
                    "mode": "production_command",
                    "dry_run": dry_run,
                    "production_repo_path": production_repo_path,
                    "previous": {"production_sha": previous_sha},
                    "checkout_safety": checkout_safety,
                    "commit_check": commit_check,
                    "planned_actions": planned_actions,
                    "database_policy": database_policy,
                }
                if dry_run:
                    return base
                command_env = os.environ.copy()
                command_env.update({
                    "PRODUCTION_REPO_PATH": production_repo_path,
                    "PRODUCTION_REMOTE": remote,
                    "PRODUCTION_BRANCH": branch,
                    "EXPECTED_COMMIT": str(commit_check["resolved_commit"]),
                    "HEALTH_CHECK": "true" if health_check else "false",
                    "SERVICES": ",".join(service_list),
                })
                completed = subprocess.run(
                    str(production_command),
                    shell=True,
                    cwd=production_repo_path,
                    env=command_env,
                    text=True,
                    capture_output=True,
                    timeout=timeout_seconds,
                )
                if completed.returncode != 0:
                    raise NativeDeployError("production_command_failed", {
                        "failure_class": "process_restart_failed",
                        "returncode": completed.returncode,
                        "stdout": completed.stdout[-4000:],
                        "stderr": completed.stderr[-4000:],
                    })
                deployed_commit = self._git(production_repo_path, "rev-parse", "HEAD", timeout=timeout_seconds).stdout.strip()
                if deployed_commit != commit_check["resolved_commit"]:
                    raise NativeDeployError("served commit did not match expected production commit", {
                        "failure_class": "checkout_failed",
                        "deployed_commit": deployed_commit,
                        "expected_commit": commit_check["resolved_commit"],
                    })
                state = {
                    "previous": base["previous"],
                    "current": {
                        "production_repo_path": production_repo_path,
                        "deployed_commit": deployed_commit,
                        "command": str(production_command),
                        "stdout": completed.stdout[-4000:],
                    },
                }
                Path(state_file).parent.mkdir(parents=True, exist_ok=True)
                Path(state_file).write_text(json.dumps(state, indent=2), encoding="utf-8")
                return {
                    **base,
                    "dry_run": False,
                    "deployed_commit": deployed_commit,
                    "state_file": state_file,
                    "performed_actions": planned_actions,
                    "stdout": completed.stdout[-4000:],
                }
            self._require_file(str(Path(production_repo_path) / "api/package.json"), "production api/package.json missing")
            self._require_file(str(Path(production_repo_path) / "ui/package.json"), "production ui/package.json missing")
            self._ensure_production_package_scripts(production_repo_path, service_list)
            checkout_safety = self._checkout_safety(production_repo_path, metadata)
            commit_check = self._verify_expected_commit(production_repo_path, remote, branch, str(expected_commit or ""), timeout_seconds)
            previous_sha = self._git(production_repo_path, "rev-parse", "HEAD", timeout=timeout_seconds, check=False).stdout.strip() or None
            previous_api = self._capture_pm2(api_name)
            previous_ui = self._capture_pm2(ui_name)
            planned_actions = [
                {"action": "fetch", "remote": remote, "branch": branch},
                {"action": "reset_exact_commit", "commit": commit_check["resolved_commit"]},
                {"action": "build", "services": service_list},
                {"action": "database_migrations", "policy": database_policy, "db_path": db_path},
                {"action": "restart_pm2", "services": service_list},
                {"action": "health_check", "enabled": health_check, "services": service_list},
            ]
            base = {
                "ok": True,
                "mode": "production",
                "dry_run": dry_run,
                "production_repo_path": production_repo_path,
                "services": service_list,
                "previous": {"production_sha": previous_sha, "api": previous_api, "ui": previous_ui},
                "checkout_safety": checkout_safety,
                "commit_check": commit_check,
                "planned_actions": planned_actions,
                "database_policy": database_policy,
            }
            if dry_run:
                return base

            resolved_commit = str(commit_check["resolved_commit"])
            self._git(production_repo_path, "reset", "--hard", resolved_commit, timeout=timeout_seconds)
            shutil.rmtree(Path(production_repo_path) / "api/dist", ignore_errors=True)
            shutil.rmtree(Path(production_repo_path) / "ui/.next", ignore_errors=True)

            dependency_setup: Dict[str, Any] = {}
            migration_setup: Optional[Dict[str, Any]] = None
            if "api" in service_list:
                api_dir = str(Path(production_repo_path) / "api")
                dependency_setup["api"] = self._ensure_deps(api_dir, timeout_seconds)
                self._run(["npm", "run", "build"], cwd=api_dir, timeout=timeout_seconds)
                migration_setup = self._run_database_migrations(
                    api_dir,
                    db_path,
                    backup_dir,
                    api_port,
                    resolved_commit,
                    database_policy,
                    timeout_seconds,
                )
                self._run(["pm2", "delete", api_name], timeout=60, check=False)
                api_env = os.environ.copy()
                api_env.update({
                    "PORT": api_port,
                    "AGENT_HQ_DB_PATH": db_path,
                    "AGENT_HQ_APP_COMMIT": resolved_commit,
                    "OPENCLAW_GATEWAY_URL": os.environ.get("OPENCLAW_GATEWAY_URL", "https://127.0.0.1:18789"),
                    "OPENCLAW_HOOKS_TOKEN": os.environ.get("OPENCLAW_HOOKS_TOKEN", ""),
                    "GATEWAY_TOKEN": os.environ.get("GATEWAY_TOKEN", ""),
                    "GATEWAY_WS_URL": os.environ.get("GATEWAY_WS_URL", "wss://127.0.0.1:18789"),
                    "GATEWAY_URL": os.environ.get("GATEWAY_URL", "https://localhost:18789"),
                    "NODE_TLS_REJECT_UNAUTHORIZED": os.environ.get("NODE_TLS_REJECT_UNAUTHORIZED", "0"),
                })
                self._run(["pm2", "start", "npm", "--name", api_name, "--cwd", api_dir, "--", "start"], env=api_env, timeout=timeout_seconds)

            if "ui" in service_list:
                ui_dir = str(Path(production_repo_path) / "ui")
                dependency_setup["ui"] = self._ensure_deps(ui_dir, timeout_seconds)
                self._run(["npm", "run", "build"], cwd=ui_dir, timeout=timeout_seconds)
                self._run(["pm2", "delete", ui_name], timeout=60, check=False)
                ui_env = os.environ.copy()
                ui_env.update({
                    "PORT": ui_port,
                    "AGENT_HQ_INTERNAL_BASE_URL": f"http://localhost:{api_port}",
                    "NEXT_PUBLIC_API_URL": f"http://localhost:{api_port}",
                    "AGENT_HQ_APP_COMMIT": resolved_commit,
                })
                self._run(["pm2", "start", "npm", "--name", ui_name, "--cwd", ui_dir, "--", "run", "start"], env=ui_env, timeout=timeout_seconds)

            health = self._health_check(service_list, api_port, ui_port) if health_check else {}
            deployed_commit = self._git(production_repo_path, "rev-parse", "HEAD", timeout=timeout_seconds).stdout.strip()
            if deployed_commit != resolved_commit:
                raise NativeDeployError(
                    "served commit did not match expected production commit",
                    {"failure_class": "checkout_failed", "deployed_commit": deployed_commit, "expected_commit": resolved_commit},
                )
            state = {
                "previous": base["previous"],
                "current": {
                    "production_repo_path": production_repo_path,
                    "deployed_commit": deployed_commit,
                    "services": service_list,
                    "api": {"cwd": f"{production_repo_path}/api", "name": api_name, "args": ["start"]},
                    "ui": {"cwd": f"{production_repo_path}/ui", "name": ui_name, "args": ["run", "start"]},
                    "checkout_safety": checkout_safety,
                    "dependency_setup": dependency_setup,
                    "database_policy": database_policy,
                    "database_migration": migration_setup if "api" in service_list else None,
                    "health": health,
                },
            }
            Path(state_file).parent.mkdir(parents=True, exist_ok=True)
            Path(state_file).write_text(json.dumps(state, indent=2), encoding="utf-8")
            return {
                **base,
                "dry_run": False,
                "deployed_commit": deployed_commit,
                "state_file": state_file,
                "performed_actions": planned_actions,
                "dependency_setup": dependency_setup,
                "database_migration": migration_setup if "api" in service_list else None,
                "health": health,
            }

        if dry_run:
            return run_plan()
        with NativeDeployLock(state_dir, lock_payload, lock_env):
            return run_plan()
