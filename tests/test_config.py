from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from dev_env_lease_manager.config import load_config


class ConfigTests(unittest.TestCase):
    def write_config(self, payload: dict) -> str:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "envs.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return str(path)

    def test_loads_environment_definitions(self) -> None:
        path = self.write_config({
            "data_path": "./state.sqlite3",
            "environments": [
                {
                    "id": "agent-hq-dev",
                    "label": "Agent HQ Dev",
                    "base_url": "http://127.0.0.1:3510",
                    "stale_after_seconds": 60,
                }
            ],
        })
        config = load_config(path)
        self.assertEqual(config.environments[0].id, "agent-hq-dev")
        self.assertEqual(config.environments[0].label, "Agent HQ Dev")
        self.assertEqual(config.environments[0].stale_after_seconds, 60)

    def test_rejects_duplicate_environment_ids(self) -> None:
        path = self.write_config({
            "environments": [
                {"id": "dev", "label": "Dev"},
                {"id": "dev", "label": "Duplicate"},
            ]
        })
        with self.assertRaisesRegex(ValueError, "duplicate environment id"):
            load_config(path)

    def test_requires_at_least_one_environment(self) -> None:
        path = self.write_config({"environments": []})
        with self.assertRaisesRegex(ValueError, "at least one environment"):
            load_config(path)


if __name__ == "__main__":
    unittest.main()

