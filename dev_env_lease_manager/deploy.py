from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Callable, Dict, Iterable, Optional
import urllib.request


Runner = Callable[..., subprocess.CompletedProcess[str]]


@dataclass
class NativeDeployError(Exception):
    error: str
    detail: Optional[Dict[str, Any]] = None

    def payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"ok": False, "error": self.error}
        if self.detail:
            payload.update(self.detail)
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
        completed = self.runner(
            args,
            cwd=cwd,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if check and completed.returncode != 0:
            raise NativeDeployError(
                "command_failed",
                {
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

    def _ensure_deps(self, package_dir: str) -> None:
        if not (Path(package_dir) / "node_modules").is_dir():
            self._run(["npm", "install", "--production=false"], cwd=package_dir)

    def _check_dev_clean(self, dev_repo_path: str) -> None:
        diff = self._git(dev_repo_path, "diff", "--quiet", check=False)
        staged = self._git(dev_repo_path, "diff", "--cached", "--quiet", check=False)
        if diff.returncode != 0 or staged.returncode != 0:
            raise NativeDeployError("dev repo has tracked modifications; clean or commit it before promotion")
        untracked = self._git(dev_repo_path, "ls-files", "--others", "--exclude-standard").stdout.strip()
        if untracked:
            raise NativeDeployError(
                "dev repo has untracked non-ignored files; remove or ignore them before promotion",
                {"files": untracked.splitlines()},
            )

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
            raise NativeDeployError("health_check_failed", {"health": results})
        return results

    def deploy(
        self,
        env: Dict[str, Any],
        source_repo_path: str,
        services: str = "both",
        health_check: bool = True,
        expected_commit: Optional[str] = None,
        timeout_seconds: int = 1800,
    ) -> Dict[str, Any]:
        metadata = env.get("metadata") or {}
        dev_repo_path = env.get("repo_path")
        if not dev_repo_path:
            raise NativeDeployError("environment repo_path is required for native deploy")

        canonical_root = expand_path(str(metadata.get("canonical_root") or source_repo_path))
        state_dir = expand_path(str(metadata.get("state_dir") or "~/.agent-hq-dev-deploy"))
        state_file = expand_path(str(metadata.get("state_file") or str(Path(state_dir) / "current-target.json")))
        lock_dir = Path(state_dir) / "lock"
        api_name = str(metadata.get("pm2_api") or "agent-hq-dev-api")
        ui_name = str(metadata.get("pm2_ui") or "agent-hq-dev-ui")
        api_port = str(metadata.get("api_port") or 3511)
        ui_port = str(metadata.get("ui_port") or 3510)
        dev_db_path = expand_path(str(metadata.get("dev_db_path") or str(Path(dev_repo_path) / "agent-hq-dev.db")))
        service_list = normalize_services(services)

        source_repo_path = expand_path(source_repo_path)
        dev_repo_path = expand_path(str(dev_repo_path))
        Path(state_dir).mkdir(parents=True, exist_ok=True)
        try:
            lock_dir.mkdir()
        except FileExistsError:
            raise NativeDeployError("deploy lock already held")

        try:
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
            if expected_commit and source_sha != expected_commit:
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

            for relative_path in ("agent-hq-dev.db", ".env", ".env.local", "api/.env", "api/.env.local", "ui/.env", "ui/.env.local"):
                self._copy_if_missing(canonical_root, dev_repo_path, relative_path)

            self._require_file(str(Path(dev_repo_path) / "package.json"), "dev repo root missing package.json")
            self._require_file(str(Path(dev_repo_path) / "api/package.json"), "dev repo api/package.json missing")
            self._require_file(str(Path(dev_repo_path) / "ui/package.json"), "dev repo ui/package.json missing")
            self._ensure_package_scripts(dev_repo_path)
            self._check_dev_clean(dev_repo_path)

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

            if "api" in service_list:
                api_dir = str(Path(dev_repo_path) / "api")
                self._ensure_deps(api_dir)
                self._run(["npm", "run", "build"], cwd=api_dir, timeout=timeout_seconds)
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
                self._ensure_deps(ui_dir)
                self._run(["npm", "run", "build"], cwd=ui_dir, timeout=timeout_seconds)
                self._run(["pm2", "delete", ui_name], timeout=60, check=False)
                ui_env = os.environ.copy()
                ui_env.update({
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
                "health": health,
            }
        finally:
            try:
                lock_dir.rmdir()
            except OSError:
                pass
