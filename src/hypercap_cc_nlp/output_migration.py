"""Helpers for migrating generated outputs to the Results/ QA/ archive contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil

from .workflow_contracts import (
    analysis_qa_dir,
    baselines_qa_dir,
    cohort_qa_dir,
    rater_qa_dir,
    results_dir,
)

DRAFT_RESULTS_PATTERNS: tuple[str, ...] = (
    "Figure *.pdf",
    "Figure *.png",
    "Figure *.xlsx",
    "Table *.pdf",
    "Table *.xlsx",
)

STAGE_PDF_FILENAMES: tuple[str, ...] = (
    "MIMICIV_hypercap_EXT_cohort.pdf",
    "Hypercap CC NLP Classifier.pdf",
    "Rater Agreement Analysis.pdf",
    "Hypercap CC NLP Analysis.pdf",
)

ROOT_RESULTS_FILENAMES: tuple[str, ...] = (
    "Ascertainment_Overlap_Intersections.xlsx",
    "Ascertainment_Overlap_UpSet.png",
    "RFV_Category_Overlap_Intersections.xlsx",
    "RFV_Category_Overlap_UpSet.png",
    "RFV_Labels_Per_Encounter_Summary.xlsx",
    "RFV_Uncodable_Diagnostics.xlsx",
    "Symptom_Composition_Pivot_ChartReady.xlsx",
    "Symptom_Composition_by_ABG_VBG_Overlap.xlsx",
    "Symptom_Composition_by_Gas_Source_Overlap.xlsx",
    "Symptom_Composition_by_Gas_Timing.xlsx",
    "Symptom_Composition_by_Gas_Timing_Stacked.png",
    "Symptom_Composition_by_Hypercapnia_Definition.xlsx",
    "Symptom_Composition_by_ICD_Gas_Overlap.xlsx",
    "Top_Symptom_Group_Percentages_by_Gas_Timing_95CI.png",
    "ICD_Positive_Subset_Breakdown.xlsx",
    "ICD_vs_Gas_Performance.xlsx",
    "Chart Review Sample Calc.html",
    "Chart Review Sample Calc_files",
)

ROOT_COHORT_QA_FILENAMES: tuple[str, ...] = (
    "qa_summary.json",
    "lab_item_map.json",
    "lab_unit_audit.csv",
    "current_columns.json",
    "ed_columns.json",
)

LEGACY_ROOT_COHORT_QA_FILENAMES: tuple[str, ...] = (
    "gas_source_diagnostics_by_ed_stay.csv",
)

ROOT_ANALYSIS_QA_FILENAMES: tuple[str, ...] = (
    "analysis_qc_checks.json",
    "analysis_qc_checks.csv",
)

DEBUG_QA_FILENAMES: tuple[str, ...] = (
    "qa_summary_ed_cc.json",
    "qa_summary_ed_spine.json",
)

STALE_ROOT_GENERATED_FILENAMES: tuple[str, ...] = (
    *STAGE_PDF_FILENAMES,
    "Hypercap CC NLP Reyan Figures.pdf",
    "Hypercap-CC-NLP-Reyan-Figures.pdf",
    "Hypercap-CC-NLP-Analysis.pdf",
)


@dataclass(frozen=True)
class MigrationOperation:
    """A planned filesystem move or copy."""

    action: str
    source: Path
    destination: Path
    reason: str


def archive_root(work_dir: Path, *, results_date: str) -> Path:
    """Return the generated-output archive root for ``results_date``."""
    return (
        work_dir / "Legacy Code" / "generated-output-archive" / results_date
    ).expanduser().resolve()


def latest_report_dir(work_dir: Path) -> Path | None:
    """Return the latest non-Reyan report directory under ``artifacts/reports``."""
    reports_root = work_dir / "artifacts" / "reports"
    if not reports_root.exists():
        return None
    candidates = [
        path
        for path in reports_root.iterdir()
        if path.is_dir() and not path.name.startswith("reyan-")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def build_output_migration_plan(
    work_dir: Path,
    *,
    results_date: str,
    report_dir: Path | None = None,
) -> list[MigrationOperation]:
    """Plan migration operations for the flat results/qa/archive contract."""
    resolved_work_dir = work_dir.expanduser().resolve()
    resolved_results_dir = results_dir(resolved_work_dir, results_date=results_date)
    resolved_archive_root = archive_root(resolved_work_dir, results_date=results_date)
    resolved_cohort_qa_dir = cohort_qa_dir(resolved_work_dir)
    resolved_rater_qa_dir = rater_qa_dir(resolved_work_dir)
    resolved_analysis_qa_dir = analysis_qa_dir(resolved_work_dir)
    resolved_baselines_qa_dir = baselines_qa_dir(resolved_work_dir)
    draft_results_dir = resolved_work_dir / "Drafts" / "Apr 16 2026"
    legacy_rater_dir = resolved_work_dir / "annotation_agreement_outputs_nlp"
    debug_qa_dir = resolved_work_dir / "debug" / "qa"
    reyan_dir = resolved_work_dir / "artifacts" / "Reyan Run Mar 31"
    reyan_zip_path = resolved_work_dir / "artifacts" / "Reyan Run Mar 31.zip"
    legacy_consort_dir = resolved_work_dir / "artifacts" / "consort"
    legacy_baselines_dir = resolved_work_dir / "artifacts" / "baselines"
    reports_root = resolved_work_dir / "artifacts" / "reports"
    nested_results_artifacts_dir = resolved_results_dir / "artifacts"
    selected_report_dir = (
        report_dir.expanduser().resolve() if report_dir else latest_report_dir(resolved_work_dir)
    )

    operations: list[MigrationOperation] = []
    seen_sources: set[Path] = set()

    def add_operation(action: str, source: Path, destination: Path, reason: str) -> None:
        resolved_source = source.expanduser().resolve()
        resolved_destination = destination.expanduser().resolve()
        if not resolved_source.exists():
            return
        if resolved_source in seen_sources:
            return
        if action == "move" and "Drafts" in resolved_source.parts:
            raise ValueError(f"Refusing to move from Drafts: {resolved_source}")
        operations.append(
            MigrationOperation(
                action=action,
                source=resolved_source,
                destination=resolved_destination,
                reason=reason,
            )
        )
        seen_sources.add(resolved_source)

    if draft_results_dir.exists():
        for pattern in DRAFT_RESULTS_PATTERNS:
            for source in sorted(draft_results_dir.glob(pattern)):
                if source.is_file():
                    add_operation(
                        "copy",
                        source,
                        resolved_results_dir / source.name,
                        "copy_generated_draft_asset",
                    )

    if selected_report_dir and selected_report_dir.exists():
        for filename in STAGE_PDF_FILENAMES:
            add_operation(
                "move",
                selected_report_dir / filename,
                resolved_results_dir / filename,
                "move_latest_stage_pdf",
            )
        add_operation(
            "move",
            selected_report_dir,
            resolved_archive_root / selected_report_dir.relative_to(resolved_work_dir),
            "archive_migrated_report_directory",
        )

    for filename in ROOT_RESULTS_FILENAMES:
        add_operation(
            "move",
            resolved_work_dir / filename,
            resolved_results_dir / filename,
            "move_root_results_output",
        )

    for filename in ROOT_COHORT_QA_FILENAMES:
        add_operation(
            "move",
            resolved_work_dir / filename,
            resolved_cohort_qa_dir / filename,
            "move_root_cohort_qa_output",
        )

    for filename in LEGACY_ROOT_COHORT_QA_FILENAMES:
        add_operation(
            "move",
            resolved_work_dir / "artifacts" / filename,
            resolved_cohort_qa_dir / filename,
            "move_legacy_cohort_qa_output",
        )

    for filename in ROOT_ANALYSIS_QA_FILENAMES:
        add_operation(
            "move",
            resolved_work_dir / filename,
            resolved_analysis_qa_dir / filename,
            "move_root_analysis_qa_output",
        )

    if debug_qa_dir.exists():
        for filename in DEBUG_QA_FILENAMES:
            add_operation(
                "move",
                debug_qa_dir / filename,
                resolved_cohort_qa_dir / filename,
                "move_debug_cohort_qa_output",
            )

    if legacy_rater_dir.exists():
        for source in sorted(legacy_rater_dir.iterdir()):
            if source.name.startswith("."):
                continue
            add_operation(
                "move",
                source,
                resolved_rater_qa_dir / source.name,
                "move_legacy_rater_output",
            )

    if legacy_baselines_dir.exists():
        add_operation(
            "move",
            legacy_baselines_dir,
            resolved_baselines_qa_dir,
            "move_legacy_baseline_directory",
        )

    if reyan_dir.exists():
        for source in sorted(reyan_dir.iterdir()):
            if source.name.startswith(".") or not source.is_file():
                continue
            if _mtime_date(source) == results_date:
                add_operation(
                    "move",
                    source,
                    resolved_results_dir / source.name,
                    "move_current_reyan_output",
                )

    if reyan_zip_path.exists():
        add_operation(
            "move",
            reyan_zip_path,
            resolved_archive_root / reyan_zip_path.relative_to(resolved_work_dir),
            "archive_stale_reyan_zip",
        )

    if legacy_consort_dir.exists():
        add_operation(
            "move",
            legacy_consort_dir,
            resolved_archive_root / legacy_consort_dir.relative_to(resolved_work_dir),
            "archive_legacy_consort_directory",
        )

    if nested_results_artifacts_dir.exists():
        add_operation(
            "move",
            nested_results_artifacts_dir,
            resolved_archive_root / nested_results_artifacts_dir.relative_to(resolved_work_dir),
            "archive_nested_results_artifacts_directory",
        )

    for filename in STALE_ROOT_GENERATED_FILENAMES:
        add_operation(
            "move",
            resolved_work_dir / filename,
            resolved_archive_root / filename,
            "archive_stale_root_generated_output",
        )

    if reports_root.exists():
        for source in sorted(reports_root.iterdir()):
            if not source.is_dir():
                continue
            if selected_report_dir and source.resolve() == selected_report_dir:
                continue
            add_operation(
                "move",
                source,
                resolved_archive_root / source.relative_to(resolved_work_dir),
                "archive_stale_report_directory",
            )

    if reyan_dir.exists():
        for source in sorted(reyan_dir.iterdir()):
            if source.name.startswith(".") or not source.is_file():
                continue
            if _mtime_date(source) != results_date:
                add_operation(
                    "move",
                    source,
                    resolved_archive_root / source.relative_to(resolved_work_dir),
                    "archive_stale_reyan_output",
                )

    for relative_dir in (Path("tmp"), Path("debug") / "tmp_pdfcheck"):
        source = resolved_work_dir / relative_dir
        if source.exists():
            add_operation(
                "move",
                source,
                resolved_archive_root / relative_dir,
                "archive_stale_debug_directory",
            )

    mimic_dir = resolved_work_dir / "MIMIC tabular data"
    if mimic_dir.exists():
        for source in sorted(mimic_dir.iterdir()):
            if not source.is_file() or source.name.startswith("."):
                continue
            if source.name.endswith(".xlsx") and _looks_like_dated_top_level_export(source.name):
                add_operation(
                    "move",
                    source,
                    resolved_archive_root / source.relative_to(resolved_work_dir),
                    "archive_dated_top_level_workbook",
                )

    return operations


def execute_migration_plan(operations: list[MigrationOperation]) -> None:
    """Execute a migration plan in order."""
    for operation in operations:
        if not operation.source.exists():
            continue
        operation.destination.parent.mkdir(parents=True, exist_ok=True)
        _remove_existing_path(operation.destination)
        if operation.action == "copy":
            _copy_path(operation.source, operation.destination)
        elif operation.action == "move":
            shutil.move(str(operation.source), str(operation.destination))
        else:
            raise ValueError(f"Unsupported action: {operation.action}")


def _copy_path(source: Path, destination: Path) -> None:
    if source.is_dir():
        shutil.copytree(source, destination)
        return
    shutil.copy2(source, destination)


def _remove_existing_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
        return
    path.unlink()


def _mtime_date(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d")


def _looks_like_dated_top_level_export(filename: str) -> bool:
    if len(filename) < 12:
        return False
    try:
        datetime.strptime(filename[:10], "%Y-%m-%d")
    except ValueError:
        return False
    return filename[10:11] == " "
