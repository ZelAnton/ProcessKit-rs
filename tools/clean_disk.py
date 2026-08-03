#!/usr/bin/env python3
"""Remove disposable local build caches without deleting target roots."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import stat
import sys


REPOSITORY = Path(__file__).resolve().parent.parent


def is_link(path: Path) -> bool:
    """Return whether deleting the path could traverse outside its parent."""

    is_junction = getattr(path, "is_junction", lambda: False)
    return path.is_symlink() or is_junction()


def incremental_directories(target: Path) -> list[Path]:
    """Find incremental roots without following nested links or junctions."""

    found: list[Path] = []
    for current, directories, _ in os.walk(target, followlinks=False):
        current_path = Path(current)
        retained: list[str] = []
        for name in directories:
            candidate = current_path / name
            if is_link(candidate):
                continue
            if name == "incremental":
                found.append(candidate)
            else:
                retained.append(name)
        directories[:] = retained
    return found


def target_roots() -> list[Path]:
    roots = [REPOSITORY / "target"]
    worktrees = REPOSITORY / ".work" / "worktrees"
    if worktrees.is_dir():
        roots.extend(path / "target" for path in worktrees.iterdir() if path.is_dir())
    return [root for root in roots if root.is_dir()]


def _retry_readonly_operation(func, path: str) -> None:
    """Retry an rmtree operation after making its path writable."""

    os.chmod(path, stat.S_IWRITE)
    func(path)


def _rmtree_onexc(func, path: str, _exc: BaseException) -> None:
    _retry_readonly_operation(func, path)


def _rmtree_onerror(func, path: str, _exc_info) -> None:
    _retry_readonly_operation(func, path)


def _rmtree(path: Path) -> None:
    """Remove a tree, retrying read-only paths on platforms that support it."""

    if sys.version_info >= (3, 12):
        shutil.rmtree(path, onexc=_rmtree_onexc)
    else:
        shutil.rmtree(path, onerror=_rmtree_onerror)


def remove_tree(path: Path) -> bool:
    if is_link(path):
        print(f"skip linked path: {path}", file=sys.stderr)
        return False
    try:
        _rmtree(path)
    except OSError as error:
        print(f"skip tree after error: {path}: {error}", file=sys.stderr)
        return False
    print(f"removed {path}")
    return True


def main() -> int:
    removed = 0
    for target in target_roots():
        for incremental in incremental_directories(target):
            removed += remove_tree(incremental)

        checkout = target.parent
        for name in ("mutants.out", "mutants.out.old"):
            report = checkout / name
            if report.is_dir():
                removed += remove_tree(report)

    suffix = "y" if removed == 1 else "ies"
    print(f"clean-disk removed {removed} disposable director{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
