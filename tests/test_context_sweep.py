from pathlib import Path

from om_pipeline.analysis.context_sweep import (
    ContextCheck,
    STATUS_GUARDRAIL_CHECKS,
    check_markdown_links,
    format_context_sweep_result,
    run_context_sweep,
    run_phrase_checks,
)


def test_phrase_checks_pass_when_required_phrases_exist(tmp_path: Path):
    context_file = tmp_path / "AGENTS.md"
    context_file.write_text(
        "Spec-Driven Increments\nDo not work directly on `main`\n",
        encoding="utf-8",
    )

    issues = run_phrase_checks(
        tmp_path,
        (
            ContextCheck(
                "AGENTS.md",
                (
                    "Spec-Driven Increments",
                    "Do not work directly on `main`",
                ),
            ),
        ),
    )

    assert issues == []
    assert format_context_sweep_result(issues).startswith("Context sweep passed")


def test_phrase_checks_report_missing_files_and_phrases(tmp_path: Path):
    context_file = tmp_path / "AGENTS.md"
    context_file.write_text("Spec-Driven Increments\n", encoding="utf-8")

    issues = run_phrase_checks(
        tmp_path,
        (
            ContextCheck("AGENTS.md", ("missing phrase",)),
            ContextCheck("docs/governance.md", ("Branch Policy",)),
        ),
    )

    assert "AGENTS.md is missing phrase: missing phrase" in issues
    assert "Missing context file: docs/governance.md" in issues
    assert format_context_sweep_result(issues).startswith("Context sweep failed")


def test_markdown_link_check_reports_missing_local_targets(tmp_path: Path):
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    readme = docs_dir / "README.md"
    readme.write_text(
        "- [Exists](governance.md)\n- [Missing](missing.md)\n- [External](https://example.com)\n",
        encoding="utf-8",
    )
    (docs_dir / "governance.md").write_text("# Governance\n", encoding="utf-8")

    issues = check_markdown_links(tmp_path, ("docs/README.md",))

    assert issues == ["docs/README.md has broken link: missing.md"]


def test_context_sweep_requires_governance_support_docs(tmp_path: Path):
    (tmp_path / "AGENTS.md").write_text("", encoding="utf-8")
    (tmp_path / "docs").mkdir()
    (tmp_path / "start_here").mkdir()
    (tmp_path / "docs" / "README.md").write_text("", encoding="utf-8")
    (tmp_path / "docs" / "governance.md").write_text("", encoding="utf-8")
    (tmp_path / "start_here" / "00_start_here.md").write_text("", encoding="utf-8")
    (tmp_path / "start_here" / "01_project_state_summary.md").write_text("", encoding="utf-8")

    issues = run_context_sweep(tmp_path)

    assert "Missing context file: docs/context-authority-map.md" in issues
    assert "Missing context file: docs/branch-exit-checklist.md" in issues
    assert "Missing context file: docs/adr/0031-stable-mainline-spec-driven-governance.md" in issues


def test_status_guardrail_checks_report_missing_authoritative_phrases(tmp_path: Path):
    context_file = tmp_path / "CONTEXT.md"
    context_file.write_text("Stage 2 is the next planned modelling step and has not started\n", encoding="utf-8")

    issues = run_phrase_checks(
        tmp_path,
        (
            ContextCheck(
                "CONTEXT.md",
                (
                    "Stage 2 is the next planned modelling step and has not started",
                    "missing or non-covered current remains null and is never treated as zero",
                ),
            ),
        ),
    )

    assert issues == [
        "CONTEXT.md is missing phrase: missing or non-covered current remains null and is never treated as zero"
    ]
    assert any(check.relative_path == "CONTEXT.md" for check in STATUS_GUARDRAIL_CHECKS)
