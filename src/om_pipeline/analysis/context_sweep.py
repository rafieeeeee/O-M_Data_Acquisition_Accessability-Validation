"""Lightweight documentation governance sweep."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class ContextCheck:
    """Required phrases for one context-bearing file."""

    relative_path: str
    required_phrases: tuple[str, ...]


GOVERNANCE_CONTEXT_CHECKS = (
    ContextCheck(
        "AGENTS.md",
        (
            "Spec-Driven Increments",
            "Do not work directly on `main`",
            "`main` is the stable integration baseline",
            "meaningful increment",
            "Plan / Design / Spec",
            "Acceptance / Sign-off",
            "remove the completed topic branch",
            "Research-Question Control",
            "docs/research-questions/rq-register.md",
            "docs/research-questions/README.md",
            "docs/context-authority-map.md",
            "docs/branch-exit-checklist.md",
            "docs/adr/0031-stable-mainline-spec-driven-governance.md",
        ),
    ),
    ContextCheck(
        "docs/governance.md",
        (
            "ADR 0031: Stable Mainline Spec-Driven Governance",
            "context authority map",
            "branch exit checklist",
            "`main` is the stable integration baseline",
            "Create one topic branch per meaningful increment",
            "Merge back to `main` only after the increment is validated, reviewed, and accepted",
            "Delete the completed topic branch after merge",
            "branch=rq6-metocean-resolution-sensitivity",
            "ahead_of_main=5",
            "upstream=none",
            "A meaningful increment is any change",
            "Plan / Design / Spec",
            "Validation Plan",
            "Definition Of Done",
        ),
    ),
    ContextCheck(
        "docs/adr/0031-stable-mainline-spec-driven-governance.md",
        (
            "Stable Mainline Spec-Driven Governance",
            "context rot",
            "handoffs",
            "unsupported research claims",
            "Treat `main` as the stable integration baseline",
            "Use one topic branch per meaningful increment",
            "Require validation evidence before merge",
            "review/sign-off note before merge",
            "Delete the completed topic branch after merge",
        ),
    ),
    ContextCheck(
        "docs/context-authority-map.md",
        (
            "Context Authority Map",
            "which file owns a project truth",
            "Governance policy",
            "Current project handoff",
            "Roadmap status",
            "Research-question planning",
            "Fusion v2 status",
            "Stage 2 modelling boundary",
            "FINO status",
            "Wind evidence guardrails",
            "Current evidence guardrails",
            "RQ9 claim boundaries",
            "Do not duplicate volatile row counts",
        ),
    ),
    ContextCheck(
        "docs/research-questions/README.md",
        (
            "Research Questions",
            "RQ Lifecycle",
            "Folder Convention",
            "Generated Outputs",
            "Independence Rules",
            "Targeted Unblockers",
            "Claim Boundary Expectations",
            "docs/research-questions/templates/",
        ),
    ),
    ContextCheck(
        "docs/research-questions/rq-register.md",
        (
            "RQ Register",
            "RQ01 Stage 2 Fusion v2 workability sensitivity",
            "RQ09 covariate and observability follow-up",
            "FINO validation planning",
            "AIS receiver geometry/proxy unblocker",
            "Status Vocabulary",
            "Update Rule",
        ),
    ),
    ContextCheck(
        "docs/research-questions/templates/rq-analysis-plan-template.md",
        (
            "RQ Analysis Plan Template",
            "Research Question",
            "Hypothesis",
            "Accepted Inputs",
            "Excluded Inputs",
            "Claim Boundary",
            "Method",
            "Validation Checks",
            "Outputs",
            "Caveats",
            "Decision Criteria",
            "Context Files To Update",
        ),
    ),
    ContextCheck(
        "docs/research-questions/templates/rq-exit-report-template.md",
        (
            "RQ Exit Report Template",
            "RQ And Branch",
            "Scope Completed",
            "Files Changed",
            "Inputs Used",
            "Outputs Produced",
            "Validation Commands",
            "Findings",
            "Claim Boundary",
            "Caveats",
            "Known Blockers",
            "Context Updates",
            "Decision And Next Steps",
            "Merge Readiness",
        ),
    ),
    ContextCheck(
        "docs/branch-exit-checklist.md",
        (
            "Branch Exit Checklist",
            "Branch name",
            "Base branch",
            "Spec or plan link",
            "Data products touched",
            "Generated reports touched",
            "Context sweep command/result",
            "Unresolved caveats",
            "Known blockers",
            "Ready to merge to `main`",
            "Safe to delete topic branch after merge",
        ),
    ),
    ContextCheck(
        "docs/README.md",
        (
            "[Governance](governance.md)",
            "[Context authority map](context-authority-map.md)",
            "[Branch exit checklist](branch-exit-checklist.md)",
            "[Research-question register](research-questions/README.md)",
            "[ADR 0031: Stable mainline spec-driven governance](adr/0031-stable-mainline-spec-driven-governance.md)",
            "`main` is the stable integration baseline",
            "Meaningful increments require a short plan/spec",
            "Use the context authority map before changing repeated status claims",
            "Use the research-question register before starting analysis work",
        ),
    ),
    ContextCheck(
        "start_here/00_start_here.md",
        (
            "[docs/governance.md](../docs/governance.md)",
            "[docs/context-authority-map.md](../docs/context-authority-map.md)",
            "[docs/branch-exit-checklist.md](../docs/branch-exit-checklist.md)",
            "[docs/research-questions/README.md](../docs/research-questions/README.md)",
            "[docs/adr/0031-stable-mainline-spec-driven-governance.md](../docs/adr/0031-stable-mainline-spec-driven-governance.md)",
            "`main` is the stable integration baseline",
            "plan/spec first",
            "acceptance/sign-off before merge",
            "branch deletion after merge",
            "Use the authority map before editing repeated status claims",
            "Research-question control",
        ),
    ),
    ContextCheck(
        "start_here/01_project_state_summary.md",
        (
            "Governance handoff",
            "stable-mainline workflow",
            "docs/context-authority-map.md",
            "docs/branch-exit-checklist.md",
        ),
    ),
)


STATUS_GUARDRAIL_CHECKS = (
    ContextCheck(
        "CONTEXT.md",
        (
            "Stage 2 is the next planned modelling step and has not started",
            "Fusion v2 is an accepted/provisional event feature layer",
            "missing or non-covered current remains null and is never treated as zero",
            "No FINO bulk download or time-series import has been run",
            "wind direction nullable or sensitivity-only",
            "RQ9 uses AIS dwell behaviour as maintenance intervention-intensity evidence, not confirmed failure-rate evidence",
        ),
    ),
    ContextCheck(
        "docs/metocean-acquisition.md",
        (
            "Stage 2 status",
            "not started. The next modelling branch",
            "Fusion v2 does not download data",
            "Wind direction remains nullable",
            "missing current remains missing/null and must not be treated as zero current",
            "no usable local FINO1/2/3 time-series archive exists yet",
        ),
    ),
    ContextCheck(
        "docs/adr/0029-metocean-fusion-v2-multiparameter-event-features.md",
        (
            "Fusion v2 is modelling-ready for sensitivity analysis, not a final probability model",
            "missing or non-covered current remains null and is not converted to zero",
            "Wind direction may be used only where",
            "import FINO observations",
            "calibrated `P(operation | weather)` claims",
        ),
    ),
    ContextCheck(
        "docs/roadmap.md",
        (
            "Stage 2 Fusion v2 Sensitivity",
            "Not started",
            "wind direction is currently too sparse for broad modelling",
            "RQ9 Maintenance Intervention Intensity Feasibility",
        ),
    ),
)


CORE_MARKDOWN_LINK_FILES = (
    "README.md",
    "AGENTS.md",
    "docs/README.md",
    "docs/governance.md",
    "docs/context-authority-map.md",
    "docs/branch-exit-checklist.md",
    "docs/research-questions/README.md",
    "docs/research-questions/rq-register.md",
    "docs/research-questions/templates/rq-analysis-plan-template.md",
    "docs/research-questions/templates/rq-exit-report-template.md",
    "docs/adr/0031-stable-mainline-spec-driven-governance.md",
    "start_here/00_start_here.md",
    "start_here/01_project_state_summary.md",
)


MARKDOWN_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")


def run_phrase_checks(
    project_root: Path,
    checks: tuple[ContextCheck, ...] = GOVERNANCE_CONTEXT_CHECKS,
) -> list[str]:
    """Return required-phrase issues found under the project root."""
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


def check_markdown_links(
    project_root: Path,
    relative_paths: tuple[str, ...] = CORE_MARKDOWN_LINK_FILES,
) -> list[str]:
    """Return missing local Markdown link targets for core docs."""
    issues: list[str] = []
    for relative_path in relative_paths:
        source = project_root / relative_path
        if not source.exists():
            issues.append(f"Missing Markdown source: {relative_path}")
            continue
        text = source.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK_RE.findall(text):
            target = raw_target.strip()
            if _is_external_or_anchor(target):
                continue
            target_path = _local_markdown_target(source.parent, target)
            if not target_path.exists():
                issues.append(f"{relative_path} has broken link: {target}")
    return issues


def run_context_sweep(project_root: Path) -> list[str]:
    """Return governance and Markdown-link issues found under the project root."""
    return (
        run_phrase_checks(project_root)
        + run_phrase_checks(project_root, STATUS_GUARDRAIL_CHECKS)
        + check_markdown_links(project_root)
    )


def format_context_sweep_result(issues: list[str]) -> str:
    """Render a concise CLI result for context sweep checks."""
    if not issues:
        return "Context sweep passed: governance docs and core links are intact."
    lines = ["Context sweep failed:"]
    lines.extend(f"- {issue}" for issue in issues)
    return "\n".join(lines)


def _is_external_or_anchor(target: str) -> bool:
    parsed = urlparse(target)
    return bool(parsed.scheme or target.startswith("#"))


def _local_markdown_target(source_dir: Path, target: str) -> Path:
    path_part = target.split("#", 1)[0]
    decoded = unquote(path_part)
    return (source_dir / decoded).resolve()
