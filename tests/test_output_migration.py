from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path

from hypercap_cc_nlp.output_migration import (
    build_output_migration_plan,
    execute_migration_plan,
)
from hypercap_cc_nlp.workflow_contracts import (
    analysis_qa_dir,
    baselines_qa_dir,
    cohort_qa_dir,
    rater_qa_dir,
    results_dir,
)


RESULTS_DATE = "2026-04-18"


def _set_mtime(path: Path, timestamp: int) -> None:
    os.utime(path, (timestamp, timestamp))


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def test_build_output_migration_plan_is_dry_run_and_never_moves_from_drafts(
    tmp_path: Path,
) -> None:
    draft_asset = tmp_path / "Drafts" / "Apr 16 2026" / "Figure 1.pdf"
    _write_file(draft_asset, "draft-figure")
    _write_file(
        tmp_path / "Drafts" / "BL-Edit Apr 16 2026" / "keep.txt",
        "leave me alone",
    )
    _write_file(
        tmp_path / "artifacts" / "reports" / "20260418_074310_full_pipeline_check" / "MIMICIV_hypercap_EXT_cohort.pdf",
        "report-pdf",
    )
    _write_file(tmp_path / "ICD_vs_Gas_Performance.xlsx", "analysis")

    operations = build_output_migration_plan(
        tmp_path,
        results_date=RESULTS_DATE,
    )

    assert any(
        operation.action == "copy" and operation.source == draft_asset.resolve()
        for operation in operations
    )
    assert all(
        not (operation.action == "move" and "Drafts" in operation.source.parts)
        for operation in operations
    )
    assert draft_asset.exists()
    assert not results_dir(tmp_path, results_date=RESULTS_DATE).exists()
    assert not (tmp_path / "Legacy Code" / "generated-output-archive" / RESULTS_DATE).exists()


def test_execute_output_migration_plan_moves_and_archives_expected_outputs(
    tmp_path: Path,
) -> None:
    results_path = results_dir(tmp_path, results_date=RESULTS_DATE)
    archive_root = tmp_path / "Legacy Code" / "generated-output-archive" / RESULTS_DATE

    draft_asset = tmp_path / "Drafts" / "Apr 16 2026" / "Figure 1.pdf"
    _write_file(draft_asset, "draft-figure")
    _write_file(
        tmp_path / "Drafts" / "BL-Edit Apr 16 2026" / "working_draft.docx",
        "keep",
    )

    current_report_dir = tmp_path / "artifacts" / "reports" / "20260418_074310_full_pipeline_check"
    stale_report_dir = tmp_path / "artifacts" / "reports" / "20260331_190538"
    _write_file(current_report_dir / "MIMICIV_hypercap_EXT_cohort.pdf", "latest-report-pdf")
    _write_file(stale_report_dir / "old.pdf", "stale-report-pdf")

    _write_file(tmp_path / "ICD_vs_Gas_Performance.xlsx", "analysis-xlsx")
    _write_file(tmp_path / "Chart Review Sample Calc.html", "chart-html")
    chart_bundle = tmp_path / "Chart Review Sample Calc_files"
    _write_file(chart_bundle / "bundle.txt", "bundle")
    _write_file(tmp_path / "qa_summary.json", "qa-summary")
    _write_file(tmp_path / "analysis_qc_checks.json", "analysis-qc")
    _write_file(tmp_path / "MIMICIV_hypercap_EXT_cohort.pdf", "root-stage-pdf")
    _write_file(tmp_path / "Hypercap CC NLP Reyan Figures.pdf", "stale-root-pdf")
    _write_file(
        tmp_path / "artifacts" / "gas_source_diagnostics_by_ed_stay.csv",
        "gas-source-diag",
    )
    _write_file(
        tmp_path / "artifacts" / "baselines" / "jupyter" / "baseline_a" / "baseline_manifest.json",
        "baseline-manifest",
    )
    _write_file(
        tmp_path / "artifacts" / "consort" / "CONSORT_Enrollment_Flow.png",
        "consort-png",
    )
    _write_file(
        tmp_path / "artifacts" / "Reyan Run Mar 31.zip",
        "reyan-zip",
    )
    _write_file(
        tmp_path / "Results" / RESULTS_DATE / "artifacts" / "consort" / "CONSORT_Enrollment_Flow_Counts.csv",
        "nested-consort-counts",
    )

    _write_file(
        tmp_path / "annotation_agreement_outputs_nlp" / "R3_vs_NLP_summary.txt",
        "rater-summary",
    )

    reyan_dir = tmp_path / "artifacts" / "Reyan Run Mar 31"
    current_reyan = reyan_dir / "Ascertainment_Flow_Diagram.pdf"
    stale_reyan = reyan_dir / "Figure1_Ascertainment_Flow_Diagram_Final.pdf"
    _write_file(current_reyan, "current-reyan")
    _write_file(stale_reyan, "stale-reyan")
    _set_mtime(current_reyan, int(datetime(2026, 4, 18, 12, 0, 0).timestamp()))
    _set_mtime(stale_reyan, int(datetime(2026, 4, 17, 12, 0, 0).timestamp()))

    _write_file(tmp_path / "tmp" / "verification" / "old.txt", "tmp-stale")
    _write_file(tmp_path / "debug" / "tmp_pdfcheck" / "old.txt", "pdfcheck-stale")
    _write_file(
        tmp_path / "MIMIC tabular data" / "2025-10-14 MIMICIV all with CC.xlsx",
        "dated-workbook",
    )

    operations = build_output_migration_plan(
        tmp_path,
        results_date=RESULTS_DATE,
        report_dir=current_report_dir,
    )
    execute_migration_plan(operations)

    assert draft_asset.exists()
    assert draft_asset.read_text() == "draft-figure"
    assert (results_path / "Figure 1.pdf").read_text() == "draft-figure"
    assert (results_path / "MIMICIV_hypercap_EXT_cohort.pdf").read_text() == "latest-report-pdf"
    assert (results_path / "ICD_vs_Gas_Performance.xlsx").read_text() == "analysis-xlsx"
    assert (results_path / "Chart Review Sample Calc.html").read_text() == "chart-html"
    assert (results_path / "Chart Review Sample Calc_files" / "bundle.txt").read_text() == "bundle"
    assert (results_path / "Ascertainment_Flow_Diagram.pdf").read_text() == "current-reyan"

    assert (cohort_qa_dir(tmp_path) / "qa_summary.json").read_text() == "qa-summary"
    assert (
        cohort_qa_dir(tmp_path) / "gas_source_diagnostics_by_ed_stay.csv"
    ).read_text() == "gas-source-diag"
    assert (analysis_qa_dir(tmp_path) / "analysis_qc_checks.json").read_text() == "analysis-qc"
    assert (rater_qa_dir(tmp_path) / "R3_vs_NLP_summary.txt").read_text() == "rater-summary"
    assert (
        baselines_qa_dir(tmp_path) / "jupyter" / "baseline_a" / "baseline_manifest.json"
    ).read_text() == "baseline-manifest"

    assert not (tmp_path / "annotation_agreement_outputs_nlp" / "R3_vs_NLP_summary.txt").exists()
    assert not (tmp_path / "Chart Review Sample Calc.html").exists()
    assert not (tmp_path / "tmp").exists()
    assert not (tmp_path / "debug" / "tmp_pdfcheck").exists()
    assert (
        archive_root
        / "artifacts"
        / "reports"
        / "20260418_074310_full_pipeline_check"
    ).exists()

    assert (archive_root / "MIMICIV_hypercap_EXT_cohort.pdf").read_text() == "root-stage-pdf"
    assert (archive_root / "Hypercap CC NLP Reyan Figures.pdf").read_text() == "stale-root-pdf"
    assert (
        archive_root
        / "artifacts"
        / "reports"
        / "20260331_190538"
        / "old.pdf"
    ).read_text() == "stale-report-pdf"
    assert (
        archive_root
        / "artifacts"
        / "consort"
        / "CONSORT_Enrollment_Flow.png"
    ).read_text() == "consort-png"
    assert (
        archive_root
        / "artifacts"
        / "Reyan Run Mar 31.zip"
    ).read_text() == "reyan-zip"
    assert (
        archive_root
        / "artifacts"
        / "Reyan Run Mar 31"
        / "Figure1_Ascertainment_Flow_Diagram_Final.pdf"
    ).read_text() == "stale-reyan"
    assert (
        archive_root
        / "Results"
        / RESULTS_DATE
        / "artifacts"
        / "consort"
        / "CONSORT_Enrollment_Flow_Counts.csv"
    ).read_text() == "nested-consort-counts"
    assert (archive_root / "tmp" / "verification" / "old.txt").read_text() == "tmp-stale"
    assert (archive_root / "debug" / "tmp_pdfcheck" / "old.txt").read_text() == "pdfcheck-stale"
    assert (
        archive_root
        / "MIMIC tabular data"
        / "2025-10-14 MIMICIV all with CC.xlsx"
    ).read_text() == "dated-workbook"
