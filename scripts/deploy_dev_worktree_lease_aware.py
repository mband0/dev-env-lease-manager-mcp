#!/usr/bin/env python3
from __future__ import annotations

from dev_env_lease_manager.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["deploy", *(__import__("sys").argv[1:])]))

