"""Private semantic parity checks for the IMV timing implementation ticket."""

from __future__ import annotations

import hashlib
import json
import math
import shutil
import subprocess
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .workflow_contracts import CANONICAL_COHORT_FILENAME, CANONICAL_NLP_FILENAME


MEMBERSHIP_COLUMNS = ("hadm_id", "subject_id", "ed_stay_id")
RFV_ASSIGNMENT_COLUMNS = (
    "hadm_id",
    "RFV1",
    "RFV2",
    "RFV3",
    "RFV4",
    "RFV5",
    "RFV1_name",
    "RFV2_name",
    "RFV3_name",
    "RFV4_name",
    "RFV5_name",
)
BASELINE_RESULT_WORKBOOKS = (
    "Cohort_Construction_and_Definitions.xlsx",
    "Table 1.xlsx",
    "Table 2.xlsx",
    "Figure 2.xlsx",
    "Figure 3.xlsx",
    "Figure 4.xlsx",
    "Figure S1.xlsx",
    "Figure S6.xlsx",
    "Figure S7.xlsx",
    "Figure S8.xlsx",
    "Figure S9.xlsx",
    "Supplementary_Table_Acid_Base_Source_Missingness.xlsx",
    "Candidate_Definition_Yield_Composition.xlsx",
    "Sensitivity_Analysis_Suite.xlsx",
)
NOTE_SHEET_TOKENS = ("note", "definition", "readme")
NUMERIC_ATOL = 1e-9


def _resolve_git_commit(work_dir: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=work_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _normalize_text(value: object) -> str:
    return unicodedata.normalize("NFC", str(value)).strip()


def _canonical_value(value: object) -> list[object]:
    if pd.isna(value):
        return ["missing", None]
    if isinstance(value, (bool, np.bool_)):
        return ["boolean", bool(value)]
    if isinstance(value, (int, float, np.integer, np.floating)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return ["number", str(numeric)]
        return ["number", numeric]
    if isinstance(value, (pd.Timestamp, datetime, np.datetime64)):
        return ["datetime", pd.Timestamp(value).isoformat()]
    return ["text", _normalize_text(value)]


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...], *, label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise KeyError(f"{label} is missing required columns: {missing}")


def _semantic_hash(
    frame: pd.DataFrame,
    *,
    columns: tuple[str, ...],
    sort_by: tuple[str, ...],
    label: str,
) -> str:
    _require_columns(frame, columns, label=label)
    selected = frame.loc[:, list(columns)].sort_values(
        list(sort_by), kind="mergesort", na_position="last"
    )
    payload = {
        "columns": list(columns),
        "rows": [
            [_canonical_value(value) for value in row]
            for row in selected.itertuples(index=False, name=None)
        ],
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _validate_unique_admissions(frame: pd.DataFrame, *, label: str) -> None:
    _require_columns(frame, ("hadm_id",), label=label)
    if frame["hadm_id"].isna().any():
        raise ValueError(f"{label} contains missing hadm_id values")
    duplicate_count = int(frame["hadm_id"].duplicated(keep=False).sum())
    if duplicate_count:
        raise ValueError(f"{label} contains {duplicate_count} duplicate hadm_id rows")


def _read_first_sheet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_excel(path, sheet_name=0, engine="openpyxl")


def _sheet_is_notes(sheet_name: str) -> bool:
    normalized = sheet_name.casefold()
    return any(token in normalized for token in NOTE_SHEET_TOKENS)


def _frame_signature(frame: pd.DataFrame) -> dict[str, object]:
    payload = {
        "columns": [_normalize_text(column) for column in frame.columns],
        "rows": [
            [_canonical_value(value) for value in row]
            for row in frame.itertuples(index=False, name=None)
        ],
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return {
        "rows": int(len(frame)),
        "columns": list(payload["columns"]),
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _workbook_signature(path: Path) -> dict[str, object]:
    sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    return {
        "file": path.name,
        "sheets": {
            sheet_name: _frame_signature(frame)
            for sheet_name, frame in sheets.items()
        },
    }


def _values_equal(baseline: object, current: object) -> tuple[bool, float | None]:
    if pd.isna(baseline) and pd.isna(current):
        return True, None
    baseline_is_number = isinstance(
        baseline, (int, float, np.integer, np.floating)
    ) and not isinstance(baseline, (bool, np.bool_))
    current_is_number = isinstance(
        current, (int, float, np.integer, np.floating)
    ) and not isinstance(current, (bool, np.bool_))
    if baseline_is_number and current_is_number:
        baseline_number = float(baseline)
        current_number = float(current)
        if math.isfinite(baseline_number) and math.isfinite(current_number):
            delta = abs(baseline_number - current_number)
            return math.isclose(
                baseline_number,
                current_number,
                rel_tol=0.0,
                abs_tol=NUMERIC_ATOL,
            ), delta
        return baseline_number == current_number, None
    return _canonical_value(baseline) == _canonical_value(current), None


def _compare_frames(baseline: pd.DataFrame, current: pd.DataFrame) -> dict[str, object]:
    baseline_columns = [_normalize_text(column) for column in baseline.columns]
    current_columns = [_normalize_text(column) for column in current.columns]
    if baseline_columns != current_columns or baseline.shape != current.shape:
        return {
            "equal": False,
            "schema_equal": False,
            "baseline_shape": list(baseline.shape),
            "current_shape": list(current.shape),
            "changed_cells": None,
            "max_numeric_delta": None,
        }

    changed_cells = 0
    max_numeric_delta = 0.0
    for baseline_row, current_row in zip(
        baseline.itertuples(index=False, name=None),
        current.itertuples(index=False, name=None),
        strict=True,
    ):
        for baseline_value, current_value in zip(
            baseline_row, current_row, strict=True
        ):
            equal, numeric_delta = _values_equal(baseline_value, current_value)
            if not equal:
                changed_cells += 1
            if numeric_delta is not None:
                max_numeric_delta = max(max_numeric_delta, numeric_delta)
    return {
        "equal": changed_cells == 0,
        "schema_equal": True,
        "baseline_shape": list(baseline.shape),
        "current_shape": list(current.shape),
        "changed_cells": changed_cells,
        "max_numeric_delta": max_numeric_delta,
    }


def _compare_workbooks(baseline_path: Path, current_path: Path) -> dict[str, object]:
    if not current_path.exists():
        return {
            "status": "fail",
            "missing_current": True,
            "sheet_results": {},
        }
    baseline_sheets = pd.read_excel(
        baseline_path, sheet_name=None, engine="openpyxl"
    )
    current_sheets = pd.read_excel(current_path, sheet_name=None, engine="openpyxl")
    baseline_names = set(baseline_sheets)
    current_names = set(current_sheets)
    missing_sheets = sorted(baseline_names - current_names)
    extra_sheets = sorted(current_names - baseline_names)
    sheet_results: dict[str, dict[str, object]] = {}
    failures = 0
    warnings = 0

    for sheet_name in sorted(baseline_names & current_names):
        comparison = _compare_frames(
            baseline_sheets[sheet_name], current_sheets[sheet_name]
        )
        notes_sheet = _sheet_is_notes(sheet_name)
        comparison["notes_sheet"] = notes_sheet
        if not comparison["equal"]:
            if notes_sheet:
                warnings += 1
            else:
                failures += 1
        sheet_results[sheet_name] = comparison

    for sheet_name in missing_sheets + extra_sheets:
        if _sheet_is_notes(sheet_name):
            warnings += 1
        else:
            failures += 1

    status = "fail" if failures else "warning" if warnings else "pass"
    return {
        "status": status,
        "missing_current": False,
        "missing_sheets": missing_sheets,
        "extra_sheets": extra_sheets,
        "sheet_results": sheet_results,
    }


def capture_imv_ticket_baseline(
    work_dir: Path,
    *,
    baseline_dir: Path,
    results_date: str,
    source_commit: str | None = None,
) -> dict[str, object]:
    """Capture private semantic inputs and signatures under an ignored baseline."""
    work_dir = work_dir.resolve()
    baseline_dir = baseline_dir.resolve()
    expected_root = (work_dir / "artifacts" / "qa" / "baselines").resolve()
    if not baseline_dir.is_relative_to(expected_root):
        raise ValueError(f"Baseline must be under {expected_root}: {baseline_dir}")
    if not baseline_dir.exists():
        raise FileNotFoundError(baseline_dir)

    semantic_dir = baseline_dir / "imv_ticket_semantic"
    manifest_path = semantic_dir / "semantic_manifest.json"
    if manifest_path.exists():
        raise FileExistsError(f"Semantic baseline already exists: {manifest_path}")

    data_dir = work_dir / "MIMIC tabular data"
    current_results_dir = work_dir / "Results" / results_date
    baseline_data_dir = semantic_dir / "MIMIC tabular data"
    baseline_results_dir = semantic_dir / "Results" / results_date
    baseline_data_dir.mkdir(parents=True, exist_ok=True)
    baseline_results_dir.mkdir(parents=True, exist_ok=True)

    cohort_path = data_dir / CANONICAL_COHORT_FILENAME
    nlp_path = data_dir / CANONICAL_NLP_FILENAME
    cohort = _read_first_sheet(cohort_path)
    nlp = _read_first_sheet(nlp_path)
    _validate_unique_admissions(cohort, label="cohort handoff")
    _validate_unique_admissions(nlp, label="NLP handoff")

    shutil.copy2(cohort_path, baseline_data_dir / cohort_path.name)
    shutil.copy2(nlp_path, baseline_data_dir / nlp_path.name)

    result_signatures: dict[str, object] = {}
    for filename in BASELINE_RESULT_WORKBOOKS:
        source = current_results_dir / filename
        if not source.exists():
            raise FileNotFoundError(source)
        destination = baseline_results_dir / filename
        shutil.copy2(source, destination)
        result_signatures[filename] = _workbook_signature(destination)

    manifest = {
        "schema_version": 1,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_commit": source_commit or _resolve_git_commit(work_dir),
        "results_date": results_date,
        "cohort": {
            "rows": int(len(cohort)),
            "membership_sha256": _semantic_hash(
                cohort,
                columns=MEMBERSHIP_COLUMNS,
                sort_by=("hadm_id",),
                label="cohort handoff",
            ),
        },
        "classifier": {
            "rows": int(len(nlp)),
            "rfv_assignment_sha256": _semantic_hash(
                nlp,
                columns=RFV_ASSIGNMENT_COLUMNS,
                sort_by=("hadm_id",),
                label="NLP handoff",
            ),
        },
        "result_workbooks": result_signatures,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"manifest_path": str(manifest_path), **manifest}


def compare_imv_ticket_baseline(
    work_dir: Path,
    *,
    baseline_dir: Path,
    results_date: str,
) -> dict[str, object]:
    """Compare current private outputs with an IMV-ticket semantic baseline."""
    work_dir = work_dir.resolve()
    semantic_dir = baseline_dir.resolve() / "imv_ticket_semantic"
    manifest_path = semantic_dir / "semantic_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(manifest_path)
    manifest = json.loads(manifest_path.read_text())

    current_data_dir = work_dir / "MIMIC tabular data"
    baseline_data_dir = semantic_dir / "MIMIC tabular data"
    current_results_dir = work_dir / "Results" / results_date
    baseline_results_dir = semantic_dir / "Results" / manifest["results_date"]

    baseline_cohort = _read_first_sheet(
        baseline_data_dir / CANONICAL_COHORT_FILENAME
    )
    current_cohort = _read_first_sheet(
        current_data_dir / CANONICAL_COHORT_FILENAME
    )
    baseline_nlp = _read_first_sheet(baseline_data_dir / CANONICAL_NLP_FILENAME)
    current_nlp = _read_first_sheet(current_data_dir / CANONICAL_NLP_FILENAME)
    for label, frame in (
        ("baseline cohort", baseline_cohort),
        ("current cohort", current_cohort),
        ("baseline NLP", baseline_nlp),
        ("current NLP", current_nlp),
    ):
        _validate_unique_admissions(frame, label=label)

    membership_baseline_hash = _semantic_hash(
        baseline_cohort,
        columns=MEMBERSHIP_COLUMNS,
        sort_by=("hadm_id",),
        label="baseline cohort",
    )
    membership_current_hash = _semantic_hash(
        current_cohort,
        columns=MEMBERSHIP_COLUMNS,
        sort_by=("hadm_id",),
        label="current cohort",
    )
    rfv_baseline_hash = _semantic_hash(
        baseline_nlp,
        columns=RFV_ASSIGNMENT_COLUMNS,
        sort_by=("hadm_id",),
        label="baseline NLP",
    )
    rfv_current_hash = _semantic_hash(
        current_nlp,
        columns=RFV_ASSIGNMENT_COLUMNS,
        sort_by=("hadm_id",),
        label="current NLP",
    )

    membership_equal = (
        len(baseline_cohort) == len(current_cohort)
        and membership_baseline_hash == membership_current_hash
    )
    rfv_equal = (
        len(baseline_nlp) == len(current_nlp)
        and rfv_baseline_hash == rfv_current_hash
    )
    workbook_results = {
        filename: _compare_workbooks(
            baseline_results_dir / filename,
            current_results_dir / filename,
        )
        for filename in BASELINE_RESULT_WORKBOOKS
    }
    workbook_failures = sum(
        result["status"] == "fail" for result in workbook_results.values()
    )
    workbook_warnings = sum(
        result["status"] == "warning" for result in workbook_results.values()
    )
    status = (
        "fail"
        if not membership_equal or not rfv_equal or workbook_failures
        else "warning"
        if workbook_warnings
        else "pass"
    )
    return {
        "status": status,
        "baseline_manifest_path": str(manifest_path),
        "baseline_results_date": manifest["results_date"],
        "current_results_date": results_date,
        "cohort_membership": {
            "equal": membership_equal,
            "baseline_rows": int(len(baseline_cohort)),
            "current_rows": int(len(current_cohort)),
            "baseline_sha256": membership_baseline_hash,
            "current_sha256": membership_current_hash,
        },
        "rfv_assignments": {
            "equal": rfv_equal,
            "baseline_rows": int(len(baseline_nlp)),
            "current_rows": int(len(current_nlp)),
            "baseline_sha256": rfv_baseline_hash,
            "current_sha256": rfv_current_hash,
        },
        "workbooks": workbook_results,
        "summary": {
            "workbook_failures": int(workbook_failures),
            "workbook_warnings": int(workbook_warnings),
        },
    }


def write_imv_ticket_parity_report(report: dict[str, Any], output_path: Path) -> Path:
    """Write an aggregate-only private parity report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return output_path
