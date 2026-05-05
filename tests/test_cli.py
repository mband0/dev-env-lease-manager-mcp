from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class CliTests(unittest.TestCase):
    def test_health_command_starts_and_reads_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "envs.json"
            config_path.write_text(json.dumps({
                "data_path": str(Path(tmp) / "state.sqlite3"),
                "environments": [{"id": "agent-hq-dev", "label": "Agent HQ Dev"}],
            }), encoding="utf-8")

            completed = subprocess.run(
                [sys.executable, "-m", "dev_env_lease_manager.cli", "--config", str(config_path), "health"],
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        body = json.loads(completed.stdout)
        self.assertTrue(body["ok"])
        self.assertEqual(body["environment_count"], 1)


if __name__ == "__main__":
    unittest.main()

