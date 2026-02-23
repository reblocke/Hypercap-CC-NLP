"""Workflow contracts for canonical notebook handoffs and schema validation."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd

DATA_DIRNAME = "MIMIC tabular data"
PRIOR_RUNS_DIRNAME = "prior runs"

CANONICAL_COHORT_FILENAME = "MIMICIV all with CC.xlsx"
CANONICAL_NLP_FILENAME = "MIMICIV all with CC_with_NLP.xlsx"

CLASSIFIER_TRANSITIONAL_ALIASES: dict[str, tuple[str, ...]] = {
    "age": ("age_at_admit",),
    "hr": ("ed_first_hr_model", "ed_first_hr", "ed_triage_hr_model", "ed_triage_hr"),
    "rr": ("ed_first_rr_model", "ed_first_rr", "ed_triage_rr_model", "ed_triage_rr"),
    "sbp": ("ed_first_sbp_model", "ed_first_sbp", "ed_triage_sbp_model", "ed_triage_sbp"),
    "dbp": ("ed_first_dbp_model", "ed_first_dbp", "ed_triage_dbp_model", "ed_triage_dbp"),
    "temp": ("ed_first_temp_model", "ed_first_temp", "ed_triage_temp_model", "ed_triage_temp"),
    "spo2": (
        "ed_first_o2sat_model",
        "ed_first_o2sat",
        "ed_triage_o2sat_model",
        "ed_triage_o2sat",
    ),
    "race": ("ed_race", "race_ed_raw", "hosp_race", "race_hosp_raw"),
}


def data_dir(work_dir: Path) -> Path:
    """Return canonical data directory rooted at ``work_dir``."""
    return (work_dir / DATA_DIRNAME).expanduser().resolve()


def resolve_classifier_input_path(
    work_dir: Path, input_filename: str | None = None
) -> Path:
    """Resolve classifier input workbook under canonical data directory."""
    filename = input_filename or CANONICAL_COHORT_FILENAME
    input_path = data_dir(work_dir) / filename
    if not input_path.exists():
        raise FileNotFoundError(
            "Expected classifier input workbook was not found at "
            f"{input_path}. Run the cohort notebook first or set "
            "CLASSIFIER_INPUT_FILENAME."
        )
    return input_path


def resolve_analysis_input_path(work_dir: Path, input_filename: str | None = None) -> Path:
    """Resolve analysis input workbook under canonical data directory."""
    filename = input_filename or CANONICAL_NLP_FILENAME
    input_path = data_dir(work_dir) / filename
    if not input_path.exists():
        raise FileNotFoundError(
            "Expected analysis input workbook was not found at "
            f"{input_path}. Run the classifier notebook first or set "
            "ANALYSIS_INPUT_FILENAME."
        )
    return input_path


def resolve_rater_nlp_input_path(work_dir: Path, input_filename: str | None = None) -> Path:
    """Resolve rater notebook NLP input workbook under canonical data directory."""
    filename = input_filename or CANONICAL_NLP_FILENAME
    input_path = data_dir(work_dir) / filename
    if not input_path.exists():
        raise FileNotFoundError(
            "Expected rater NLP input workbook was not found at "
            f"{input_path}. Run the classifier notebook first or set "
            "RATER_NLP_INPUT_FILENAME."
        )
    return input_path


def resolve_classifier_output_paths(
    work_dir: Path,
    *,
    run_dt: datetime | None = None,
) -> tuple[Path, Path]:
    """Return canonical NLP output path and dated archive path."""
    base_data_dir = data_dir(work_dir)
    canonical_path = base_data_dir / CANONICAL_NLP_FILENAME

    run_date = (run_dt or datetime.now()).strftime("%Y-%m-%d")
    archive_dir = base_data_dir / PRIOR_RUNS_DIRNAME
    archive_path = archive_dir / f"{run_date} {CANONICAL_NLP_FILENAME}"

    return canonical_path, archive_path


def normalize_classifier_input_schema(
    df: pd.DataFrame,
    alias_map: Mapping[str, str | Sequence[str]] | None = None,
) -> pd.DataFrame:
    """Add transitional alias columns for classifier compatibility.

    Existing destination columns are preserved. Source columns are never removed.
    """
    resolved_alias_map = alias_map or CLASSIFIER_TRANSITIONAL_ALIASES
    normalized = df.copy()
    for destination, source_spec in resolved_alias_map.items():
        if destination in normalized.columns:
            continue
        if isinstance(source_spec, str):
            candidate_sources = (source_spec,)
        else:
            candidate_sources = tuple(source_spec)
        for source_name in candidate_sources:
            if source_name in normalized.columns:
                normalized[destination] = normalized[source_name]
                break
    return normalized


def ensure_required_columns(
    df: pd.DataFrame,
    required: list[str],
    *,
    context: str,
) -> None:
    """Validate required columns with context-aware error message."""
    missing = sorted(set(required).difference(df.columns))
    if missing:
        raise KeyError(f"{context}: missing required columns: {missing}")
