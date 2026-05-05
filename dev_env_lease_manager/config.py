from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from .models import DEFAULT_STALE_AFTER_SECONDS


ENV_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$")


@dataclass(frozen=True)
class EnvironmentConfig:
    id: str
    label: str
    base_url: Optional[str] = None
    health_url: Optional[str] = None
    repo_path: Optional[str] = None
    deploy_command: Optional[str] = None
    served_commit_command: Optional[str] = None
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LeaseManagerConfig:
    data_path: str
    default_stale_after_seconds: int
    environments: List[EnvironmentConfig]
    agent_hq_base_url: Optional[str] = None


def _expand_path(value: str) -> str:
    return str(Path(os.path.expandvars(os.path.expanduser(value))).resolve())


def load_config(path: str) -> LeaseManagerConfig:
    config_path = Path(os.path.expandvars(os.path.expanduser(path))).resolve()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    default_stale = int(raw.get("default_stale_after_seconds", DEFAULT_STALE_AFTER_SECONDS))
    data_path = _expand_path(raw.get("data_path", "~/.dev-environment-lease-manager/state.sqlite3"))
    agent_hq = raw.get("agent_hq") or {}

    environments: List[EnvironmentConfig] = []
    seen = set()
    for item in raw.get("environments", []):
        env_id = str(item.get("id", "")).strip()
        if not env_id or not ENV_ID_RE.match(env_id):
            raise ValueError(f"invalid environment id: {env_id!r}")
        if env_id in seen:
            raise ValueError(f"duplicate environment id: {env_id}")
        seen.add(env_id)

        stale_after = int(item.get("stale_after_seconds", default_stale))
        if stale_after <= 0:
            raise ValueError(f"environment {env_id} stale_after_seconds must be positive")

        repo_path = item.get("repo_path")
        environments.append(EnvironmentConfig(
            id=env_id,
            label=str(item.get("label") or env_id),
            base_url=item.get("base_url"),
            health_url=item.get("health_url"),
            repo_path=_expand_path(repo_path) if repo_path else None,
            deploy_command=item.get("deploy_command"),
            served_commit_command=item.get("served_commit_command"),
            stale_after_seconds=stale_after,
            tags=list(item.get("tags") or []),
            metadata=dict(item.get("metadata") or {}),
        ))

    if not environments:
        raise ValueError("at least one environment must be configured")

    return LeaseManagerConfig(
        data_path=data_path,
        default_stale_after_seconds=default_stale,
        environments=environments,
        agent_hq_base_url=agent_hq.get("base_url"),
    )


def default_config_path() -> str:
    return os.environ.get(
        "DEV_ENV_LEASE_CONFIG",
        str(Path(__file__).resolve().parents[1] / "config" / "environments.json"),
    )

