#!/usr/bin/env python3
"""Report large user cache and macOS System Data candidate directories.

This script is read-only. It does not delete files; it only prints sizes and
optional cleanup hints for well-known cache locations.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ROOTS = [
    "~/.cache",
    "~/Library/Caches",
]

COMMON_SYSTEM_DATA_ROOTS = [
    "~/Library/Application Support",
    "~/Library/Containers",
    "~/Library/Group Containers",
    "~/Library/Logs",
    "~/Library/Developer",
    "~/.Trash",
    "/private/var/folders",
]

PURGE_HINTS = {
    "pip": "python -m pip cache purge",
    "huggingface": "rm -rf ~/.cache/huggingface",
    "npm": "npm cache clean --force",
    "yarn": "yarn cache clean",
    "pnpm": "pnpm store prune",
    "uv": "uv cache clean",
    "poetry": "poetry cache clear --all pypi",
    "matplotlib": "rm -rf ~/.matplotlib ~/Library/Caches/matplotlib",
}


@dataclass(frozen=True)
class SizeRow:
    path: Path
    size_kib: int
    note: str = ""

    @property
    def size_bytes(self) -> int:
        return self.size_kib * 1024


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit user cache directories and macOS System Data candidates."
    )
    parser.add_argument(
        "roots",
        nargs="*",
        help="Optional directories to scan. Defaults to ~/.cache and ~/Library/Caches.",
    )
    parser.add_argument(
        "--include-common",
        action="store_true",
        help="Also scan common macOS System Data locations such as Application Support and /private/var/folders.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=25,
        help="Number of largest child directories/files to print per root.",
    )
    parser.add_argument(
        "--nested-top",
        type=int,
        default=8,
        help="For each root, also expand this many largest child directories one level deeper.",
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Write all measured rows to a CSV file.",
    )
    parser.add_argument(
        "--cleanup-hints",
        action="store_true",
        help="Print possible cleanup commands for recognized cache directories. Commands are not executed.",
    )
    return parser.parse_args()


def expand_path(path_text: str) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(path_text))).resolve()


def format_size(size_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def du_kib(path: Path) -> tuple[int | None, str]:
    try:
        result = subprocess.run(
            ["du", "-sk", os.fspath(path)],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        return None, str(exc)

    if result.returncode != 0 and not result.stdout:
        return None, result.stderr.strip()

    first_line = result.stdout.splitlines()[0] if result.stdout else ""
    try:
        return int(first_line.split()[0]), result.stderr.strip()
    except (IndexError, ValueError):
        return None, result.stderr.strip() or result.stdout.strip()


def measure(path: Path) -> SizeRow:
    size_kib, warning = du_kib(path)
    if size_kib is None:
        return SizeRow(path=path, size_kib=0, note=warning or "unreadable")
    return SizeRow(path=path, size_kib=size_kib, note=warning)


def iter_children(path: Path) -> list[Path]:
    try:
        return sorted(path.iterdir(), key=lambda child: child.name.lower())
    except (FileNotFoundError, NotADirectoryError, PermissionError):
        return []


def largest_children(path: Path, limit: int) -> list[SizeRow]:
    rows = [measure(child) for child in iter_children(path)]
    return sorted(rows, key=lambda row: row.size_kib, reverse=True)[:limit]


def cleanup_hint_for(path: Path) -> str:
    path_text = os.fspath(path)
    name = path.name.lower()
    for marker, command in PURGE_HINTS.items():
        if marker in name or f"/{marker}/" in path_text.lower():
            return command
    return ""


def print_rows(title: str, rows: list[SizeRow], cleanup_hints: bool) -> None:
    print(title)
    if not rows:
        print("  No entries found.")
        return

    width = max(len(format_size(row.size_bytes)) for row in rows)
    for row in rows:
        hint = cleanup_hint_for(row.path) if cleanup_hints else ""
        details = []
        if row.note:
            details.append("warning: " + row.note.replace("\n", " | "))
        if hint:
            details.append("hint: " + hint)
        suffix = f"  ({'; '.join(details)})" if details else ""
        print(f"  {format_size(row.size_bytes):>{width}}  {row.path}{suffix}")


def write_csv(path: Path, rows: list[SizeRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["size_bytes", "size_human", "path", "note"])
        for row in sorted(rows, key=lambda item: item.size_kib, reverse=True):
            writer.writerow([row.size_bytes, format_size(row.size_bytes), row.path, row.note])


def main() -> int:
    args = parse_args()
    root_texts = args.roots or list(DEFAULT_ROOTS)
    if args.include_common:
        root_texts.extend(COMMON_SYSTEM_DATA_ROOTS)

    roots = []
    for root_text in root_texts:
        root = expand_path(root_text)
        if root not in roots:
            roots.append(root)

    all_rows: list[SizeRow] = []
    root_rows = [measure(root) for root in roots]
    all_rows.extend(root_rows)

    print_rows("Root totals:", sorted(root_rows, key=lambda row: row.size_kib, reverse=True), args.cleanup_hints)
    print()

    for root in roots:
        if not root.exists():
            print(f"{root}: not found")
            print()
            continue

        children = largest_children(root, args.top)
        all_rows.extend(children)
        print_rows(f"Largest entries under {root}:", children, args.cleanup_hints)

        nested_parents = [row.path for row in children if row.path.is_dir()][: args.nested_top]
        for parent in nested_parents:
            nested = largest_children(parent, min(args.top, 15))
            all_rows.extend(nested)
            if nested:
                print_rows(f"  Largest entries under {parent}:", nested, args.cleanup_hints)
        print()

    if args.csv_path:
        csv_path = expand_path(args.csv_path)
        write_csv(csv_path, all_rows)
        print(f"Wrote CSV report to {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
