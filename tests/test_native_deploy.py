from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sqlite3
import tempfile
import unittest

from dev_env_lease_manager.deploy import NativeDeployError, NativeDevDeployer, commit_matches_expected, normalize_services


def write_package(path: Path, scripts: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"scripts": scripts}), encoding="utf-8")


class NativeDeployTests(unittest.TestCase):
    def make_layout(self) -> tuple[tempfile.TemporaryDirectory[str], Path, Path, Path]:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        source = root / "source"
        dev = root / "dev"
        canonical = root / "canonical"
        for repo in (source, dev, canonical):
            write_package(repo / "package.json", {})
            write_package(repo / "api/package.json", {"build": "tsc", "start": "node dist/index.js"})
            write_package(repo / "ui/package.json", {"build": "next build", "start-dev": "next start"})
        return tmp, source, dev, canonical

    def fake_runner(self, source: Path, dev: Path, commands: list[list[str]], command_envs: list[tuple[list[str], dict[str, str]]] | None = None):
        source_path = str(source.resolve())
        dev_path = str(dev.resolve())

        def run(args, cwd=None, env=None, text=True, capture_output=True, timeout=None):
            commands.append(list(args))
            if command_envs is not None:
                command_envs.append((list(args), dict(env or {})))
            stdout = ""
            returncode = 0
            if args[:4] == ["git", "-C", source_path, "rev-parse"] and args[4:] == ["--show-toplevel"]:
                stdout = f"{source_path}\n"
            elif args[:4] == ["git", "-C", source_path, "status"]:
                stdout = ""
            elif args[:4] == ["git", "-C", source_path, "rev-parse"] and args[4:] == ["HEAD"]:
                stdout = "abc123\n"
            elif args[:4] == ["git", "-C", source_path, "branch"]:
                stdout = "feature/native-deploy\n"
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["--show-toplevel"]:
                stdout = f"{dev_path}\n"
            elif args[:4] == ["git", "-C", dev_path, "diff"]:
                returncode = 0
            elif args[:4] == ["git", "-C", dev_path, "ls-files"]:
                stdout = ""
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["HEAD"]:
                stdout = "previous\n"
            elif args[:4] == ["git", "-C", dev_path, "fetch"]:
                stdout = ""
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["FETCH_HEAD"]:
                stdout = "abc123\n"
            elif args[:4] == ["git", "-C", dev_path, "reset"]:
                stdout = "HEAD is now at abc123\n"
            elif args == ["pm2", "jlist"]:
                stdout = "[]"
            return subprocess.CompletedProcess(args, returncode, stdout, "")

        return run

    def test_normalize_services(self) -> None:
        self.assertEqual(normalize_services("both"), ["api", "ui"])
        self.assertEqual(normalize_services("ui,api"), ["api", "ui"])
        self.assertEqual(normalize_services("api"), ["api"])
        with self.assertRaises(NativeDeployError):
            normalize_services("worker")

    def test_commit_matches_expected_accepts_git_abbreviations(self) -> None:
        self.assertTrue(commit_matches_expected("94e572c63b18fe419cffc7368c417c3c0828723a", "94e572c"))
        self.assertTrue(commit_matches_expected("94e572c63b18fe419cffc7368c417c3c0828723a", "94e572c63b18fe419cffc7368c417c3c0828723a"))
        self.assertFalse(commit_matches_expected("94e572c63b18fe419cffc7368c417c3c0828723a", "94e572d"))
        self.assertFalse(commit_matches_expected("94e572c63b18fe419cffc7368c417c3c0828723a", "94e"))

    def test_native_deploy_promotes_exact_head_and_restarts_services(self) -> None:
        _, source, dev, canonical = self.make_layout()
        commands: list[list[str]] = []
        command_envs: list[tuple[list[str], dict[str, str]]] = []
        deployer = NativeDevDeployer(self.fake_runner(source, dev, commands, command_envs))
        state_dir = Path(source.parent) / "state"

        result = deployer.deploy(
            {
                "id": "agent-hq-dev",
                "repo_path": str(dev),
                "metadata": {
                    "canonical_root": str(canonical),
                    "state_dir": str(state_dir),
                    "api_port": 3511,
                    "ui_port": 3510,
                    "pm2_api": "agent-hq-dev-api",
                    "pm2_ui": "agent-hq-dev-ui",
                },
            },
            str(source),
            services="both",
            health_check=False,
            expected_commit="abc123",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "native")
        self.assertEqual(result["source_sha"], "abc123")
        self.assertIn(["git", "-C", str(dev.resolve()), "fetch", "--no-tags", str(source.resolve()), "HEAD"], commands)
        self.assertIn(["git", "-C", str(dev.resolve()), "reset", "--hard", "abc123"], commands)
        self.assertIn(["npm", "run", "build"], commands)
        self.assertIn(["pm2", "start", "npm", "--name", "agent-hq-dev-api", "--cwd", str(dev.resolve() / "api"), "--", "start"], commands)
        self.assertIn(["pm2", "start", "npm", "--name", "agent-hq-dev-ui", "--cwd", str(dev.resolve() / "ui"), "--", "run", "start-dev"], commands)
        ui_start_env = next(
            env for command, env in command_envs
            if command == ["pm2", "start", "npm", "--name", "agent-hq-dev-ui", "--cwd", str(dev.resolve() / "ui"), "--", "run", "start-dev"]
        )
        self.assertEqual(ui_start_env["PORT"], "3510")
        self.assertEqual(ui_start_env["AGENT_HQ_INTERNAL_BASE_URL"], "http://localhost:3511")
        self.assertEqual(ui_start_env["NEXT_PUBLIC_API_URL"], "http://localhost:3511")

        state = json.loads((state_dir / "current-target.json").read_text(encoding="utf-8"))
        self.assertEqual(state["current"]["source_sha"], "abc123")
        self.assertEqual(state["current"]["services"], ["api", "ui"])

    def test_native_deploy_rejects_dirty_source_before_mutating_dev(self) -> None:
        _, source, dev, canonical = self.make_layout()
        source_path = str(source.resolve())

        def dirty_runner(args, cwd=None, env=None, text=True, capture_output=True, timeout=None):
            if args[:4] == ["git", "-C", source_path, "rev-parse"] and args[4:] == ["--show-toplevel"]:
                return subprocess.CompletedProcess(args, 0, f"{source_path}\n", "")
            if args[:4] == ["git", "-C", source_path, "status"]:
                return subprocess.CompletedProcess(args, 0, " M api/src/index.ts\n", "")
            return subprocess.CompletedProcess(args, 0, "", "")

        state_dir = source.parent / "state"
        deployer = NativeDevDeployer(dirty_runner)

        with self.assertRaises(NativeDeployError) as caught:
            deployer.deploy(
                {
                    "id": "agent-hq-dev",
                    "repo_path": str(dev),
                    "metadata": {"canonical_root": str(canonical), "state_dir": str(state_dir)},
                },
                str(source),
                health_check=False,
            )

        self.assertIn("uncommitted", caught.exception.error)
        self.assertFalse((state_dir / "lock").exists())

    def test_native_deploy_cleans_dirty_dev_checkout_before_promotion(self) -> None:
        _, source, dev, canonical = self.make_layout()
        source_path = str(source.resolve())
        dev_path = str(dev.resolve())
        commands: list[list[str]] = []

        def dirty_dev_runner(args, cwd=None, env=None, text=True, capture_output=True, timeout=None):
            commands.append(list(args))
            stdout = ""
            returncode = 0
            if args[:4] == ["git", "-C", source_path, "rev-parse"] and args[4:] == ["--show-toplevel"]:
                stdout = f"{source_path}\n"
            elif args[:4] == ["git", "-C", source_path, "status"]:
                stdout = ""
            elif args[:4] == ["git", "-C", source_path, "rev-parse"] and args[4:] == ["HEAD"]:
                stdout = "abc123\n"
            elif args[:4] == ["git", "-C", source_path, "branch"]:
                stdout = "feature/native-deploy\n"
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["--show-toplevel"]:
                stdout = f"{dev_path}\n"
            elif args[:4] == ["git", "-C", dev_path, "status"]:
                stdout = " M api/src/index.ts\n?? scratch.txt\n"
            elif args[:4] == ["git", "-C", dev_path, "diff"]:
                returncode = 1
            elif args[:4] == ["git", "-C", dev_path, "ls-files"]:
                stdout = "scratch.txt\n"
            elif args[:4] == ["git", "-C", dev_path, "reset"]:
                stdout = "HEAD is now at abc123\n"
            elif args[:4] == ["git", "-C", dev_path, "clean"]:
                stdout = "Removing scratch.txt\n"
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["HEAD"]:
                stdout = "previous\n"
            elif args[:4] == ["git", "-C", dev_path, "fetch"]:
                stdout = ""
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["FETCH_HEAD"]:
                stdout = "abc123\n"
            elif args == ["pm2", "jlist"]:
                stdout = "[]"
            return subprocess.CompletedProcess(args, returncode, stdout, "")

        deployer = NativeDevDeployer(dirty_dev_runner)
        state_dir = source.parent / "state"

        result = deployer.deploy(
            {
                "id": "agent-hq-dev",
                "repo_path": str(dev),
                "metadata": {"canonical_root": str(canonical), "state_dir": str(state_dir)},
            },
            str(source),
            services="api",
            health_check=False,
            expected_commit="abc123",
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["dev_predeploy_cleanup"]["cleaned"])
        self.assertTrue(result["dev_predeploy_cleanup"]["tracked_dirty"])
        self.assertEqual(result["dev_predeploy_cleanup"]["untracked_files"], ["scratch.txt"])
        self.assertIn(["git", "-C", str(dev.resolve()), "reset", "--hard"], commands)
        self.assertIn(["git", "-C", str(dev.resolve()), "clean", "-ffd"], commands)

    def test_ensure_deps_repairs_existing_node_modules_missing_dev_dependency(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        package_dir = Path(tmp.name) / "ui"
        package_dir.mkdir()
        (package_dir / "package.json").write_text(
            json.dumps({
                "scripts": {"lint": "eslint lib/*.ts"},
                "devDependencies": {"eslint": "^9.27.0"},
            }),
            encoding="utf-8",
        )
        (package_dir / "package-lock.json").write_text("{}", encoding="utf-8")
        (package_dir / "node_modules").mkdir()
        commands: list[list[str]] = []

        def runner(args, cwd=None, env=None, text=True, capture_output=True, timeout=None):
            commands.append(list(args))
            if args == ["npm", "ci", "--include=dev"]:
                (Path(cwd) / "node_modules" / "eslint").mkdir()
            return subprocess.CompletedProcess(args, 0, "", "")

        result = NativeDevDeployer(runner)._ensure_deps(str(package_dir), timeout_seconds=60)

        self.assertTrue(result["installed"])
        self.assertEqual(result["install_reason"], "dependency_manifest_changed")
        self.assertEqual(result["install_command"], ["npm", "ci", "--include=dev"])
        self.assertEqual(result["missing_before"], ["eslint"])
        self.assertIn(["npm", "ci", "--include=dev"], commands)
        self.assertTrue((package_dir / "node_modules" / ".agent-hq-deploy-deps.sha256").is_file())

    def test_database_migration_preflight_runs_before_api_restart(self) -> None:
        _, source, dev, canonical = self.make_layout()
        write_package(dev / "api/package.json", {
            "build": "tsc",
            "start": "node dist/index.js",
            "db:migrate": "node dist/db/migrate.js",
            "db:migrate:preflight": "node dist/db/migrate.js",
            "db:migrate:status": "node dist/db/migrateStatus.js",
        })
        commands: list[list[str]] = []
        deployer = NativeDevDeployer(self.fake_runner(source, dev, commands))
        state_dir = Path(source.parent) / "state"

        result = deployer.deploy(
            {
                "id": "agent-hq-dev",
                "repo_path": str(dev),
                "metadata": {
                    "canonical_root": str(canonical),
                    "state_dir": str(state_dir),
                    "dev_db_path": str(dev / "agent-hq-dev.db"),
                },
            },
            str(source),
            services="api",
            health_check=False,
            expected_commit="abc123",
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["database_migration"]["applied"])
        migrate_preflight = commands.index(["npm", "run", "db:migrate:preflight"])
        migrate_apply = commands.index(["npm", "run", "db:migrate"])
        pm2_delete = commands.index(["pm2", "delete", "agent-hq-dev-api"])
        self.assertLess(migrate_preflight, pm2_delete)
        self.assertLess(migrate_apply, pm2_delete)

    def test_database_backup_retains_only_latest_snapshot_by_default(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        db_path = root / "agent-hq-dev.db"
        backup_dir = root / "db-backups"
        backup_dir.mkdir()
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO tasks (id) VALUES (1)")

        old_backup = backup_dir / "agent-hq-dev.db.100.oldoldoldold.bak"
        old_backup.write_text("old", encoding="utf-8")
        old_sidecar = backup_dir / "agent-hq-dev.db.100.oldoldoldold.bak-journal"
        old_sidecar.write_text("journal", encoding="utf-8")
        unrelated_backup = backup_dir / "other.db.100.oldoldoldold.bak"
        unrelated_backup.write_text("other", encoding="utf-8")
        old_time = 1_700_000_000
        os.utime(old_backup, (old_time, old_time))
        os.utime(old_sidecar, (old_time, old_time))

        result = NativeDevDeployer()._backup_database(str(db_path), str(backup_dir), "abc123456789")

        self.assertIsNotNone(result)
        retained = list(backup_dir.glob("agent-hq-dev.db.*.bak"))
        self.assertEqual(retained, [Path(result["path"])])
        self.assertFalse(old_backup.exists())
        self.assertFalse(old_sidecar.exists())
        self.assertTrue(unrelated_backup.exists())
        self.assertEqual(result["retained_count"], 1)
        self.assertIn(str(old_backup), result["pruned"])
        self.assertIn(str(old_sidecar), result["pruned"])

    def test_database_backup_can_retain_extra_snapshots_when_configured(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        db_path = root / "agent-hq-dev.db"
        backup_dir = root / "db-backups"
        backup_dir.mkdir()
        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY)")

        first = NativeDevDeployer()._backup_database(str(db_path), str(backup_dir), "111111111111", retain_count=2)
        second = NativeDevDeployer()._backup_database(str(db_path), str(backup_dir), "222222222222", retain_count=2)
        third = NativeDevDeployer()._backup_database(str(db_path), str(backup_dir), "333333333333", retain_count=2)

        retained = sorted(path.name for path in backup_dir.glob("agent-hq-dev.db.*.bak"))
        self.assertEqual(len(retained), 2)
        self.assertNotIn(Path(first["path"]).name, retained)
        self.assertIn(Path(second["path"]).name, retained)
        self.assertIn(Path(third["path"]).name, retained)

    def test_database_migration_preflight_failure_does_not_restart_api(self) -> None:
        _, source, dev, canonical = self.make_layout()
        write_package(dev / "api/package.json", {
            "build": "tsc",
            "start": "node dist/index.js",
            "db:migrate": "node dist/db/migrate.js",
            "db:migrate:preflight": "node dist/db/migrate.js",
        })
        source_path = str(source.resolve())
        dev_path = str(dev.resolve())
        commands: list[list[str]] = []

        def runner(args, cwd=None, env=None, text=True, capture_output=True, timeout=None):
            commands.append(list(args))
            stdout = ""
            returncode = 0
            if args[:4] == ["git", "-C", source_path, "rev-parse"] and args[4:] == ["--show-toplevel"]:
                stdout = f"{source_path}\n"
            elif args[:4] == ["git", "-C", source_path, "status"]:
                stdout = ""
            elif args[:4] == ["git", "-C", source_path, "rev-parse"] and args[4:] == ["HEAD"]:
                stdout = "abc123\n"
            elif args[:4] == ["git", "-C", source_path, "branch"]:
                stdout = "feature/native-deploy\n"
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["--show-toplevel"]:
                stdout = f"{dev_path}\n"
            elif args[:4] == ["git", "-C", dev_path, "diff"]:
                returncode = 0
            elif args[:4] == ["git", "-C", dev_path, "ls-files"]:
                stdout = ""
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["HEAD"]:
                stdout = "previous\n"
            elif args[:4] == ["git", "-C", dev_path, "fetch"]:
                stdout = ""
            elif args[:4] == ["git", "-C", dev_path, "rev-parse"] and args[4:] == ["FETCH_HEAD"]:
                stdout = "abc123\n"
            elif args[:4] == ["git", "-C", dev_path, "reset"]:
                stdout = "HEAD is now at abc123\n"
            elif args == ["pm2", "jlist"]:
                stdout = "[]"
            elif args == ["npm", "run", "db:migrate:preflight"]:
                returncode = 1
                stdout = ""
                return subprocess.CompletedProcess(args, returncode, stdout, "no such column: project_id")
            return subprocess.CompletedProcess(args, returncode, stdout, "")

        deployer = NativeDevDeployer(runner)
        state_dir = source.parent / "state"

        with self.assertRaises(NativeDeployError) as caught:
            deployer.deploy(
                {
                    "id": "agent-hq-dev",
                    "repo_path": str(dev),
                    "metadata": {"canonical_root": str(canonical), "state_dir": str(state_dir)},
                },
                str(source),
                services="api",
                health_check=False,
                expected_commit="abc123",
            )

        self.assertEqual(caught.exception.error, "database_migration_failed")
        self.assertNotIn(["pm2", "delete", "agent-hq-dev-api"], commands)


if __name__ == "__main__":
    unittest.main()
