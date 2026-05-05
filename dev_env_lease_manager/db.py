from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3

from .config import LeaseManagerConfig


def connect(db_path: str) -> sqlite3.Connection:
    Path(os.path.expanduser(db_path)).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(os.path.expanduser(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_schema(conn)
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS environments (
      id TEXT PRIMARY KEY,
      label TEXT NOT NULL,
      base_url TEXT,
      health_url TEXT,
      repo_path TEXT,
      deploy_command TEXT,
      served_commit_command TEXT,
      tags_json TEXT NOT NULL DEFAULT '[]',
      stale_after_seconds INTEGER NOT NULL,
      metadata_json TEXT NOT NULL DEFAULT '{}',
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS leases (
      id TEXT PRIMARY KEY,
      environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
      task_id TEXT NOT NULL,
      agent_id TEXT,
      agent_name TEXT,
      branch TEXT,
      commit_sha TEXT,
      status TEXT NOT NULL,
      acquired_at TEXT NOT NULL,
      deploying_at TEXT,
      deployed_at TEXT,
      prod_deploying_at TEXT,
      heartbeat_at TEXT NOT NULL,
      released_at TEXT,
      release_reason TEXT,
      metadata_json TEXT NOT NULL DEFAULT '{}'
    );

    CREATE TABLE IF NOT EXISTS lease_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lease_id TEXT,
      environment_id TEXT NOT NULL,
      task_id TEXT,
      actor TEXT NOT NULL,
      event_type TEXT NOT NULL,
      from_status TEXT,
      to_status TEXT,
      release_reason TEXT,
      message TEXT,
      payload_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_active_environment_lease
      ON leases(environment_id)
      WHERE released_at IS NULL
        AND status IN ('acquired', 'deploying', 'deployed_for_qa', 'prod_deploying', 'stale');

    CREATE INDEX IF NOT EXISTS idx_lease_events_lease ON lease_events(lease_id, id);
    CREATE INDEX IF NOT EXISTS idx_leases_task ON leases(task_id);
    """)


def sync_environments(conn: sqlite3.Connection, config: LeaseManagerConfig, now: str) -> None:
    statement = conn.execute
    for env in config.environments:
        statement(
            """
            INSERT INTO environments (
              id, label, base_url, health_url, repo_path, deploy_command,
              served_commit_command, tags_json, stale_after_seconds, metadata_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              label = excluded.label,
              base_url = excluded.base_url,
              health_url = excluded.health_url,
              repo_path = excluded.repo_path,
              deploy_command = excluded.deploy_command,
              served_commit_command = excluded.served_commit_command,
              tags_json = excluded.tags_json,
              stale_after_seconds = excluded.stale_after_seconds,
              metadata_json = excluded.metadata_json,
              updated_at = excluded.updated_at
            """,
            (
                env.id,
                env.label,
                env.base_url,
                env.health_url,
                env.repo_path,
                env.deploy_command,
                env.served_commit_command,
                json.dumps(env.tags),
                env.stale_after_seconds,
                json.dumps(env.metadata, sort_keys=True),
                now,
            ),
        )

