from __future__ import annotations

import json
from pathlib import Path
import subprocess
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

    def fake_runner(self, source: Path, dev: Path, commands: list[list[str]]):
        source_path = str(source.resolve())
        dev_path = str(dev.resolve())

        def run(args, cwd=None, env=None, text=True, capture_output=True, timeout=None):
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
        deployer = NativeDevDeployer(self.fake_runner(source, dev, commands))
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


if __name__ == "__main__":
    unittest.main()
