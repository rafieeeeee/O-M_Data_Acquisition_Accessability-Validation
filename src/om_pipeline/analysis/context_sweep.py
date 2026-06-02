"""Lightweight context-documentation sweep for evidence-readiness handoffs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ContextCheck:
    """Required phrases for one context-bearing file."""

    relative_path: str
    required_phrases: tuple[str, ...]


EVIDENCE_READINESS_CONTEXT_CHECKS = (
    ContextCheck(
        "CONTEXT.md",
        (
            "Evidence Readiness Foundation",
            "`success` and `success_no_ais_in_bbox` are observed AIS source coverage",
            "`skipped_missing_source` is missing source evidence",
            "RQ6 as ready only for source-aware metocean sensitivity/readiness work",
            "RQ2/RQ3/RQ5/RQ7/RQ8/RQ9/RQ10 as blocked",
            "within country/sea-basin/year-month strata",
            "evidence consistent with geographical AIS observability bias",
        ),
    ),
    ContextCheck(
        "analysis/00_data_foundation/EVIDENCE_READINESS_METHODOLOGY.md",
        (
            "Reproducibility",
            "/opt/anaconda3/bin/python scripts/build_evidence_readiness.py",
            "`success_no_ais_in_bbox` as observed coverage",
            "`skipped_missing_source` as missing source evidence",
            "not calibrated access-probability claims",
            "Confirmed failure inference requires SCADA",
        ),
    ),
    ContextCheck(
        "docs/adr/0031-evidence-readiness-data-integration.md",
        (
            "source of truth for the generated report and ignored",
            "`success` and `success_no_ais_in_bbox` as observed AIS source coverage",
            "`skipped_missing_source` as missing source evidence",
            "RQ6) only",
            "RQ9 remains blocked for failure claims",
        ),
    ),
    ContextCheck(
        "reports/evidence_readiness/data_limitations_and_observability_report.md",
        (
            "Reproducibility Notes",
            "`success_no_ais_in_bbox` is observed zero-event evidence",
            "`skipped_missing_source` is missing source evidence",
            "RQ6 is ready only for source-aware metocean sensitivity/readiness work",
            "RQ9 remains blocked for failure claims",
            "Validation is localized to CARE Wind Farm B/C mappings",
        ),
    ),
    ContextCheck(
        "analysis/00_data_foundation/AIS_RECEIVER_DISTANCE_OBSERVABILITY_AUDIT.md",
        (
            "/opt/anaconda3/bin/python scripts/build_ais_observability_bias.py",
            "Only per-message receiver assignment can directly test receiver-distance bias",
            "`skipped_missing_source` is missing source evidence",
            "Observed AIS base-station geometry is reported separately",
            "Matched Base-Station Distance Diagnostic",
            "`insufficient_matched_strata`",
        ),
    ),
    ContextCheck(
        "reports/evidence_readiness/ais_receiver_distance_observability_report.md",
        (
            "Only per-message receiver assignment can directly test receiver-distance bias",
            "`Data source type` is source-channel evidence",
            "`Type of mobile = Base Station` rows are direct AIS base-station geometry reference",
            "`skipped_missing_source` is missing source evidence",
            "Do not infer receiver locations from vessel positions",
            "Do not claim the nearest observed base station received a vessel ping",
            "Matched Base-Station Distance Diagnostic",
            "observed-source farm-months only",
            "Nearest observed AIS base station remains a source-geometry control only",
            "RQ9 remains blocked for failure claims",
        ),
    ),
    ContextCheck(
        "docs/adr/0032-ais-observability-bias-audit.md",
        (
            "Tier 1a per-message receiver assignment",
            "Tier 1b direct AIS source-geometry reference",
            "`Data source type`",
            "Require explicit provenance",
            "country/sea-basin/year-month strata",
            "evidence consistent with geographical AIS observability bias",
            "not failure rate",
        ),
    ),
)


def run_context_sweep(
    project_root: Path,
    checks: tuple[ContextCheck, ...] = EVIDENCE_READINESS_CONTEXT_CHECKS,
) -> list[str]:
    """Return context documentation issues found under the project root."""
    issues: list[str] = []
    for check in checks:
        path = project_root / check.relative_path
        if not path.exists():
            issues.append(f"Missing context file: {check.relative_path}")
            continue
        text = path.read_text(encoding="utf-8")
        folded = text.casefold()
        for phrase in check.required_phrases:
            if phrase.casefold() not in folded:
                issues.append(f"{check.relative_path} is missing phrase: {phrase}")
    return issues


def format_context_sweep_result(issues: list[str]) -> str:
    """Render a concise CLI result for context sweep checks."""
    if not issues:
        return "Context sweep passed: evidence-readiness docs preserve sign-off semantics."
    lines = ["Context sweep failed:"]
    lines.extend(f"- {issue}" for issue in issues)
    return "\n".join(lines)
