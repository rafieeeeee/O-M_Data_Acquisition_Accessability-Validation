#!/usr/bin/env python3
"""Run the repository context and governance documentation sweep."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from om_pipeline.analysis.context_sweep import (  # noqa: E402
    format_context_sweep_result,
    run_context_sweep,
)


def main() -> int:
    issues = run_context_sweep(PROJECT_ROOT)
    print(format_context_sweep_result(issues))
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
