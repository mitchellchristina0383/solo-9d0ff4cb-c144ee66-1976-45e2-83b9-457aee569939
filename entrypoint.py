#!/usr/bin/env python3
"""Container entrypoint: make the data volume writable, then drop privileges.

The image runs as root only long enough to take ownership of ``$DRILL_DATA_DIR``
(a fresh named volume is root-owned), then execs the real command as an
unprivileged user.  Running outside Docker as a non-root user this is a no-op.
"""

from __future__ import annotations

import os
import sys

DRILL_UID = 10001
DRILL_GID = 10001


def take_ownership(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    for root, dirs, files in os.walk(path):
        os.chown(root, DRILL_UID, DRILL_GID)
        for name in dirs:
            os.chown(os.path.join(root, name), DRILL_UID, DRILL_GID)
        for name in files:
            os.chown(os.path.join(root, name), DRILL_UID, DRILL_GID)


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("usage: entrypoint.py <command> [args...]")

    data_dir = os.environ.get("DRILL_DATA_DIR", "/data")
    if os.geteuid() == 0:
        take_ownership(data_dir)
        os.setgroups([])
        os.setgid(DRILL_GID)
        os.setuid(DRILL_UID)
    else:
        os.makedirs(data_dir, exist_ok=True)

    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == "__main__":
    main()
