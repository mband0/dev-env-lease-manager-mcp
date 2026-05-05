#!/usr/bin/env python3
from __future__ import annotations

import sys

from dev_env_lease_manager.cli import main


def deploy_argv(argv: list[str]) -> list[str]:
    prefix: list[str] = []
    rest = list(argv)
    while rest:
        head = rest[0]
        if head == "--config" and len(rest) >= 2:
            prefix.extend(rest[:2])
            rest = rest[2:]
            continue
        if head.startswith("--config="):
            prefix.append(head)
            rest = rest[1:]
            continue
        break
    return [*prefix, "deploy", *rest]


if __name__ == "__main__":
    raise SystemExit(main(deploy_argv(sys.argv[1:])))
