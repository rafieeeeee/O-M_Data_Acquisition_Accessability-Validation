#!/usr/bin/env python
"""Run controlled NWS hourly true u/v current batches."""

from __future__ import annotations

import argparse
from pathlib import Path

from om_pipeline.metocean.nws_current_batch import (
    DEFAULT_ELIGIBILITY,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_RAW_CACHE_ROOT,
    DEFAULT_REPORT_DIR,
    NWS_CURRENT_DATASET_ID,
    NWS_CURRENT_PRODUCT_ID,
    run_nws_current_batch,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eligibility", type=Path, default=DEFAULT_ELIGIBILITY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--raw-cache-root", type=Path, default=DEFAULT_RAW_CACHE_ROOT)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--remaining-recommended", action="store_true")
    parser.add_argument("--exclude-existing", action="store_true")
    parser.add_argument("--exclude-stress-test", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-overwrite", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--pilot-product-id", default=NWS_CURRENT_PRODUCT_ID)
    parser.add_argument("--pilot-dataset-id", default=NWS_CURRENT_DATASET_ID)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    overwrite = bool(args.overwrite and not args.no_overwrite)
    result = run_nws_current_batch(
        eligibility_path=args.eligibility,
        output_root=args.output_root,
        raw_cache_root=args.raw_cache_root,
        report_dir=args.report_dir,
        top_n=args.top_n,
        remaining_recommended=args.remaining_recommended,
        exclude_existing=args.exclude_existing,
        exclude_stress_test=args.exclude_stress_test,
        dry_run=args.dry_run,
        overwrite=overwrite,
        product_id=args.pilot_product_id,
        dataset_id=args.pilot_dataset_id,
    )

    manifest = result.manifest
    validated = manifest[manifest["status"].eq("validated")]
    skipped = manifest[manifest["status"].eq("skipped_existing")]
    accepted = manifest[
        manifest["status"].isin(["validated", "skipped_existing"])
        & manifest["qa_status"].eq("passed")
    ]
    failed = manifest[manifest["status"].eq("failed")]
    print(f"Selected farm-years: {len(result.selected)}")
    print(f"Output root: {result.output_root}")
    print(f"Raw cache root: {result.raw_cache_root}")
    print(f"Manifest path: {result.manifest_path}")
    print(f"Dry-run report path: {result.dry_run_report_path}")
    if result.validation_report_path is not None:
        print(f"Validation report path: {result.validation_report_path}")
    print(f"Processed farm-years: {len(validated)}")
    print(f"Skipped existing farm-years: {len(skipped)}")
    print(f"Accepted farm-years: {len(accepted)}")
    print(f"Failed farm-years: {len(failed)}")
    print(f"Final row count: {int(accepted['row_count'].sum()) if not accepted.empty else 0:,}")


if __name__ == "__main__":
    main()
