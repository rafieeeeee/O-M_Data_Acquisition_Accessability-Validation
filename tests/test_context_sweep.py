from pathlib import Path

from om_pipeline.analysis.context_sweep import (
    ContextCheck,
    format_context_sweep_result,
    run_context_sweep,
)


def test_context_sweep_passes_when_required_phrases_exist(tmp_path: Path):
    context_file = tmp_path / "CONTEXT.md"
    context_file.write_text(
        "Evidence Readiness Foundation\n"
        "`success` and `success_no_ais_in_bbox` are observed AIS source coverage\n",
        encoding="utf-8",
    )

    issues = run_context_sweep(
        tmp_path,
        checks=(
            ContextCheck(
                "CONTEXT.md",
                (
                    "Evidence Readiness Foundation",
                    "`success` and `success_no_ais_in_bbox` are observed AIS source coverage",
                ),
            ),
        ),
    )

    assert issues == []
    assert format_context_sweep_result(issues).startswith("Context sweep passed")


def test_context_sweep_reports_missing_files_and_phrases(tmp_path: Path):
    context_file = tmp_path / "CONTEXT.md"
    context_file.write_text("Evidence Readiness Foundation\n", encoding="utf-8")

    issues = run_context_sweep(
        tmp_path,
        checks=(
            ContextCheck("CONTEXT.md", ("missing phrase",)),
            ContextCheck("docs/adr/missing.md", ("anything",)),
        ),
    )

    assert "CONTEXT.md is missing phrase: missing phrase" in issues
    assert "Missing context file: docs/adr/missing.md" in issues
    assert format_context_sweep_result(issues).startswith("Context sweep failed")
