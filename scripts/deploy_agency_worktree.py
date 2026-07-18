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


def run(
    args: list[str],
    *,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 900,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout)
    if completed.returncode != 0:
        print(f"command failed: {' '.join(args)}", file=sys.stderr)
        print(completed.stdout[-4000:], file=sys.stderr)
        print(completed.stderr[-4000:], file=sys.stderr)
        raise SystemExit(completed.returncode)
    return completed


def git(repo: str, *args: str, timeout: int = 900) -> subprocess.CompletedProcess[str]:
    return run(["git", "-C", repo, *args], timeout=timeout)


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
    for _ in range(60):
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if (
                    response.status == 200
                    and payload.get("status") == "healthy"
                    and (payload.get("checks") or {}).get("required_tables") == "present"
                    and not payload.get("missing_tables")
                ):
                    return
                last_error = f"unhealthy response: HTTP {response.status} {payload}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(1)
    raise SystemExit(f"health check failed for {url}: {last_error}")


def pm2_process_exists(name: str) -> bool:
    completed = subprocess.run(["pm2", "jlist"], text=True, capture_output=True, timeout=60)
    if completed.returncode != 0:
        return False
    try:
        items = json.loads(completed.stdout or "[]")
    except json.JSONDecodeError:
        return False
    return any((item.get("name") == name or (item.get("pm2_env") or {}).get("name") == name) for item in items if isinstance(item, dict))


def runtime_env(env_file: str) -> dict[str, str]:
    env = os.environ.copy()
    for raw in Path(env_file).expanduser().read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    env["AGENCY_CRM_ENV_FILE"] = str(Path(env_file).expanduser())
    return env


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


def deploy_agency_app(dev_repo_path: str, port: str, pm2_name: str, env_file: str) -> str:
    root = Path(dev_repo_path)
    if not (root / "package.json").is_file():
        raise SystemExit("unsupported Agency app layout: expected package.json")
    if not Path(env_file).expanduser().is_file():
        raise SystemExit(f"missing Agency CRM env file: {env_file}")

    run(["pnpm", "install", "--frozen-lockfile"], cwd=dev_repo_path, timeout=900)
    deploy_env = runtime_env(env_file)
    run(["pnpm", "run", "db:bootstrap"], cwd=dev_repo_path, env=deploy_env, timeout=900)
    run(["pnpm", "run", "db:validate"], cwd=dev_repo_path, env=deploy_env, timeout=900)
    run(["./scripts/build-production.sh"], cwd=dev_repo_path, timeout=900)

    subprocess.run(["pm2", "delete", pm2_name], text=True, capture_output=True, timeout=60)

    env = deploy_env.copy()
    env["PORT"] = port
    env["AGENCY_CRM_ENV_FILE"] = env_file
    run(
        ["pm2", "start", "scripts/run-production.sh", "--name", pm2_name, "--interpreter", "/bin/bash"],
        cwd=dev_repo_path,
        env=env,
        timeout=120,
    )
    return f"http://127.0.0.1:{port}"


def main() -> int:
    source_repo_path = os.environ["REPO_PATH"]
    dev_repo_path = os.environ["DEV_REPO_PATH"]
    expected_commit = os.environ.get("DEV_LEASE_COMMIT", "")
    port = os.environ.get("AGENCY_CRM_PORT", "3640")
    pm2_name = os.environ.get("AGENCY_CRM_PM2_NAME", "agency-crm")
    env_file = os.environ.get("AGENCY_CRM_ENV_FILE", str(Path.home() / ".config/agency/crm.env"))
    state_dir = Path(os.environ.get("AGENCY_CRM_STATE_DIR", "~/.agency-crm-dev-deploy")).expanduser()
    health_check = os.environ.get("HEALTH_CHECK", "true").lower() == "true"

    promoted = promote_source(source_repo_path, dev_repo_path, expected_commit)
    url = deploy_agency_app(promoted, port, pm2_name, env_file)

    if health_check:
        health_url = os.environ.get("AGENCY_CRM_HEALTH_URL", f"{url}/api/system/health")
        wait_for_health(health_url)

    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "current-target.json").write_text(
        json.dumps(
            {
                "repo_path": promoted,
                "url": url,
                "pm2_name": pm2_name,
                "commit": git(promoted, "rev-parse", "HEAD").stdout.strip(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps({"ok": True, "url": url, "repo_path": promoted, "pm2_name": pm2_name}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
