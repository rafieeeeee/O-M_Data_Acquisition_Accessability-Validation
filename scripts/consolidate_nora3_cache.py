"""Consolidate completed NORA3 raw cache CSV pairs into parquet batches."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from om_pipeline.metocean.nora3_cache_consolidator import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    consolidate_nora3_cache,
)
from om_pipeline.ingestion.nora3 import NORA3_CACHE_DIR  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read stable NORA3 wave/wind cache CSV pairs and write checkpointed "
            "joined parquet batches without touching the active downloader."
        )
    )
    parser.add_argument("--cache-dir", default=NORA3_CACHE_DIR)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--stable-seconds", type=int, default=120)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = consolidate_nora3_cache(
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        stable_seconds=args.stable_seconds,
        max_batches=args.max_batches,
        dry_run=args.dry_run,
    )

    print("NORA3 cache consolidation summary")
    print(f"  eligible_pairs:             {summary.eligible_pairs}")
    print(f"  already_processed_pairs:    {summary.already_processed_pairs}")
    print(f"  written_pairs:              {summary.written_pairs}")
    print(f"  written_batches:            {summary.written_batches}")
    print(f"  skipped_incomplete_pairs:   {summary.skipped_incomplete_pairs}")
    print(f"  skipped_fresh_pairs:        {summary.skipped_fresh_pairs}")
    print(f"  output_dir:                 {summary.output_dir}")
    print(f"  manifest:                   {summary.manifest_path}")


if __name__ == "__main__":
    main()
