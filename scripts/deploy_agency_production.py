#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request


def run(args: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None, timeout: int = 900) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout)
    if completed.returncode != 0:
        print(completed.stdout[-4000:], file=sys.stderr)
        print(completed.stderr[-4000:], file=sys.stderr)
        raise SystemExit(completed.returncode)
    return completed


def wait_for_health(url: str) -> dict:
    last_error = ""
    for _ in range(60):
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
                if response.status == 200 and payload.get("status") == "healthy" and (payload.get("checks") or {}).get("required_tables") == "present" and not payload.get("missing_tables"):
                    return payload
                last_error = f"HTTP {response.status}: {payload}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(1)
    raise SystemExit(f"production health check failed: {last_error}")


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


def main() -> int:
    repo = Path(os.environ["PRODUCTION_REPO_PATH"]).expanduser().resolve()
    expected = os.environ["EXPECTED_COMMIT"]
    remote = os.environ.get("PRODUCTION_REMOTE", "origin")
    branch = os.environ.get("PRODUCTION_BRANCH", "main")
    env_file = str(Path(os.environ.get("AGENCY_CRM_ENV_FILE", "~/.config/agency/crm.env")).expanduser())
    port = os.environ.get("AGENCY_CRM_PORT", "3640")
    pm2_name = os.environ.get("AGENCY_CRM_PM2_NAME", "agency-crm")
    if not Path(env_file).is_file():
        raise SystemExit(f"missing Agency CRM env file: {env_file}")

    run(["git", "-C", str(repo), "fetch", "--no-tags", remote, branch])
    resolved = run(["git", "-C", str(repo), "rev-parse", f"{expected}^{{commit}}"]).stdout.strip()
    ancestor = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", resolved, f"{remote}/{branch}"])
    if ancestor.returncode != 0:
        raise SystemExit(f"expected commit {resolved} is not reachable from {remote}/{branch}")
    run(["git", "-C", str(repo), "reset", "--hard", resolved])

    deploy_env = runtime_env(env_file)
    deploy_env["PORT"] = port
    run(["pnpm", "install", "--frozen-lockfile"], cwd=str(repo), env=deploy_env)
    run(["pnpm", "run", "db:bootstrap"], cwd=str(repo), env=deploy_env)
    run(["pnpm", "run", "db:validate"], cwd=str(repo), env=deploy_env)
    run(["./scripts/build-production.sh"], cwd=str(repo), env=deploy_env)
    subprocess.run(["pm2", "delete", pm2_name], text=True, capture_output=True)
    run(["pm2", "start", "scripts/run-production.sh", "--name", pm2_name, "--interpreter", "/bin/bash"], cwd=str(repo), env=deploy_env, timeout=120)
    health = wait_for_health(f"http://127.0.0.1:{port}/api/system/health")
    deployed = run(["git", "-C", str(repo), "rev-parse", "HEAD"]).stdout.strip()
    if deployed != resolved:
        raise SystemExit(f"deployed commit {deployed} does not match expected {resolved}")
    print(json.dumps({"ok": True, "deployed_commit": deployed, "url": f"http://127.0.0.1:{port}", "health": health}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
