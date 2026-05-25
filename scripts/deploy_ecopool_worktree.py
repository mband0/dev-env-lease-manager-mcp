#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request


def run(args: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None, timeout: int = 900) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout)
    if completed.returncode != 0:
        print(f"command failed: {' '.join(args)}", file=sys.stderr)
        print(completed.stdout[-4000:], file=sys.stderr)
        print(completed.stderr[-4000:], file=sys.stderr)
        raise SystemExit(completed.returncode)
    return completed


def git(repo: str, *args: str, timeout: int = 900) -> subprocess.CompletedProcess[str]:
    return run(["git", "-C", repo, *args], timeout=timeout)


def package_scripts(package_json: Path) -> dict[str, str]:
    try:
        payload = json.loads(package_json.read_text(encoding="utf-8"))
    except Exception:
        return {}
    scripts = payload.get("scripts") or {}
    return scripts if isinstance(scripts, dict) else {}


def is_managed_runtime_artifact(path: str) -> bool:
    managed_paths = {
        ".agent-hq-run-context.json",
        ".atlas-gh-identity.env",
        ".atlas-gh-token",
        "skills/.agent-hq-managed-skills.json",
    }
    return path in managed_paths or path.startswith(".atlas-gh-") or path.startswith("skills/")


def blocking_status_lines(status: str) -> list[str]:
    lines: list[str] = []
    for line in status.splitlines():
        if len(line) < 4:
            continue
        code = line[:2]
        path = line[3:]
        if code == "??" and is_managed_runtime_artifact(path):
            continue
        lines.append(line)
    return lines


def wait_for_health(url: str) -> None:
    last_error = ""
    for _ in range(45):
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if 200 <= response.status < 500:
                    return
        except Exception as exc:
            last_error = str(exc)
        time.sleep(1)
    raise SystemExit(f"health check failed for {url}: {last_error}")


def promote_source(source_repo_path: str, dev_repo_path: str, expected_commit: str) -> str:
    source_root = git(source_repo_path, "rev-parse", "--show-toplevel").stdout.strip()
    status = git(source_root, "status", "--porcelain=v1", "--untracked-files=all").stdout.strip()
    blocking = blocking_status_lines(status)
    if blocking:
        print("source repo has uncommitted or untracked changes", file=sys.stderr)
        print("\n".join(blocking), file=sys.stderr)
        raise SystemExit(2)

    source_sha = git(source_root, "rev-parse", "HEAD").stdout.strip()
    if expected_commit and source_sha != expected_commit and not source_sha.startswith(expected_commit):
        raise SystemExit(f"source HEAD {source_sha} does not match expected {expected_commit}")

    dev_path = Path(dev_repo_path).expanduser().resolve()
    if dev_path.exists() and not (dev_path / ".git").exists():
        shutil.rmtree(dev_path)
    if not dev_path.exists():
        dev_path.parent.mkdir(parents=True, exist_ok=True)
        run(["git", "clone", source_root, str(dev_path)])

    git(str(dev_path), "fetch", "--no-tags", source_root, "HEAD")
    fetched = git(str(dev_path), "rev-parse", "FETCH_HEAD").stdout.strip()
    if fetched != source_sha:
        raise SystemExit(f"fetched {fetched} but expected {source_sha}")
    git(str(dev_path), "reset", "--hard", source_sha)
    git(str(dev_path), "clean", "-ffd")
    return str(dev_path)


def deploy_node_app(dev_repo_path: str, port: str, pm2_name: str) -> str:
    root = Path(dev_repo_path)
    scripts = package_scripts(root / "package.json")
    install = ["npm", "ci"] if (root / "package-lock.json").is_file() else ["npm", "install"]
    run(install, cwd=dev_repo_path, timeout=900)
    if "build" in scripts:
        run(["npm", "run", "build"], cwd=dev_repo_path, timeout=900)

    run(["pm2", "delete", pm2_name], timeout=60) if pm2_process_exists(pm2_name) else None
    env = os.environ.copy()
    env["PORT"] = port
    start_script = "start" if "start" in scripts else "dev"
    if start_script not in scripts:
        raise SystemExit("package.json must define a start or dev script")
    run(["pm2", "start", "npm", "--name", pm2_name, "--cwd", dev_repo_path, "--", "run", start_script], env=env, timeout=120)
    return f"http://127.0.0.1:{port}"


def deploy_fastapi_app(dev_repo_path: str, port: str, pm2_name: str) -> str:
    root = Path(dev_repo_path)
    venv = root / ".venv"
    pip_path = venv / "bin" / "pip"
    python_path = venv / "bin" / "python"
    if venv.exists() and (not pip_path.is_file() or not python_path.is_file()):
        shutil.rmtree(venv)
    if not venv.exists():
        run([sys.executable, "-m", "venv", str(venv)], cwd=dev_repo_path, timeout=300)
    pip = str(pip_path)
    python = str(python_path)
    run([python, "-m", "ensurepip", "--upgrade"], cwd=dev_repo_path, timeout=300)
    run([pip, "install", "-r", "requirements.txt"], cwd=dev_repo_path, timeout=900)

    run(["pm2", "delete", pm2_name], timeout=60) if pm2_process_exists(pm2_name) else None
    env = os.environ.copy()
    env["PORT"] = port
    run(
        [
            "pm2",
            "start",
            python,
            "--name",
            pm2_name,
            "--cwd",
            dev_repo_path,
            "--",
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "127.0.0.1",
            "--port",
            port,
        ],
        env=env,
        timeout=120,
    )
    return f"http://127.0.0.1:{port}"


def pm2_process_exists(name: str) -> bool:
    completed = subprocess.run(["pm2", "jlist"], text=True, capture_output=True, timeout=60)
    if completed.returncode != 0:
        return False
    try:
        items = json.loads(completed.stdout or "[]")
    except json.JSONDecodeError:
        return False
    return any((item.get("name") == name or (item.get("pm2_env") or {}).get("name") == name) for item in items if isinstance(item, dict))


def main() -> int:
    source_repo_path = os.environ["REPO_PATH"]
    dev_repo_path = os.environ["DEV_REPO_PATH"]
    expected_commit = os.environ.get("DEV_LEASE_COMMIT", "")
    port = os.environ.get("ECOPOOL_PORT", "3610")
    pm2_name = os.environ.get("ECOPOOL_PM2_NAME", "ecopool-dev")
    health_check = os.environ.get("HEALTH_CHECK", "true").lower() == "true"

    promoted = promote_source(source_repo_path, dev_repo_path, expected_commit)
    root = Path(promoted)

    if (root / "package.json").is_file():
        url = deploy_node_app(promoted, port, pm2_name)
    elif (root / "requirements.txt").is_file() and (root / "main.py").is_file():
        url = deploy_fastapi_app(promoted, port, pm2_name)
    else:
        raise SystemExit("unsupported Ecopool app layout: expected package.json or requirements.txt + main.py")

    if health_check:
        wait_for_health(f"{url}/health")
    state_dir = Path(os.environ.get("ECOPOOL_STATE_DIR", "~/.ecopool-dev-deploy")).expanduser()
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "current-target.json").write_text(
        json.dumps({"repo_path": promoted, "url": url, "pm2_name": pm2_name, "commit": git(promoted, "rev-parse", "HEAD").stdout.strip()}, indent=2),
        encoding="utf-8",
    )
    print(json.dumps({"ok": True, "url": url, "repo_path": promoted, "pm2_name": pm2_name}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
