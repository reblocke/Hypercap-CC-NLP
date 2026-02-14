from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from hypercap_cc_nlp.workflow_contracts import (
    CANONICAL_COHORT_FILENAME,
    CANONICAL_NLP_FILENAME,
    CLASSIFIER_TRANSITIONAL_ALIASES,
    resolve_analysis_input_path,
    resolve_classifier_input_path,
    resolve_classifier_output_paths,
    resolve_rater_nlp_input_path,
    ensure_required_columns,
    normalize_classifier_input_schema,
)


def test_resolve_classifier_input_path_uses_canonical_default(tmp_path: Path) -> None:
    data_dir = tmp_path / "MIMIC tabular data"
    data_dir.mkdir()
    workbook = data_dir / CANONICAL_COHORT_FILENAME
    workbook.write_text("placeholder")

    resolved = resolve_classifier_input_path(tmp_path)
    assert resolved == workbook.resolve()


def test_resolve_classifier_input_path_accepts_override(tmp_path: Path) -> None:
    data_dir = tmp_path / "MIMIC tabular data"
    data_dir.mkdir()
    workbook = data_dir / "custom_input.xlsx"
    workbook.write_text("placeholder")

    resolved = resolve_classifier_input_path(tmp_path, "custom_input.xlsx")
    assert resolved == workbook.resolve()


def test_resolve_analysis_input_path_uses_canonical_default(tmp_path: Path) -> None:
    data_dir = tmp_path / "MIMIC tabular data"
    data_dir.mkdir()
    workbook = data_dir / CANONICAL_NLP_FILENAME
    workbook.write_text("placeholder")

    resolved = resolve_analysis_input_path(tmp_path)
    assert resolved == workbook.resolve()


def test_resolve_rater_nlp_input_path_uses_canonical_default(tmp_path: Path) -> None:
    data_dir = tmp_path / "MIMIC tabular data"
    data_dir.mkdir()
    workbook = data_dir / CANONICAL_NLP_FILENAME
    workbook.write_text("placeholder")

    resolved = resolve_rater_nlp_input_path(tmp_path)
    assert resolved == workbook.resolve()


def test_resolve_rater_nlp_input_path_accepts_override(tmp_path: Path) -> None:
    data_dir = tmp_path / "MIMIC tabular data"
    data_dir.mkdir()
    workbook = data_dir / "custom_rater_input.xlsx"
    workbook.write_text("placeholder")

    resolved = resolve_rater_nlp_input_path(tmp_path, "custom_rater_input.xlsx")
    assert resolved == workbook.resolve()


def test_resolve_classifier_output_paths_returns_canonical_and_archive(
    tmp_path: Path,
) -> None:
    canonical, archive = resolve_classifier_output_paths(
        tmp_path,
        run_dt=datetime(2026, 2, 8),
    )
    assert canonical == (
        tmp_path / "MIMIC tabular data" / CANONICAL_NLP_FILENAME
    ).resolve()
    assert archive == (
        tmp_path
        / "MIMIC tabular data"
        / "prior runs"
        / f"2026-02-08 {CANONICAL_NLP_FILENAME}"
    ).resolve()


def test_normalize_classifier_input_schema_adds_aliases_without_dropping_source() -> None:
    df = pd.DataFrame(
        {
            "age_at_admit": [65.0],
            "ed_first_hr": [90],
            "ed_first_rr": [18],
            "ed_first_sbp": [120],
            "ed_first_dbp": [70],
            "ed_first_temp": [37.0],
            "ed_first_o2sat": [98],
            "race_ed_raw": ["White"],
        }
    )
    normalized = normalize_classifier_input_schema(df)

    for destination, sources in CLASSIFIER_TRANSITIONAL_ALIASES.items():
        assert destination in normalized.columns
        assert any(source in normalized.columns for source in sources)


def test_normalize_classifier_input_schema_preserves_existing_destination() -> None:
    df = pd.DataFrame({"age_at_admit": [65.0], "age": [99.0]})
    normalized = normalize_classifier_input_schema(df)
    assert normalized["age"].tolist() == [99.0]


def test_normalize_classifier_input_schema_prefers_model_fields() -> None:
    df = pd.DataFrame(
        {
            "ed_first_hr": [150.0],
            "ed_first_hr_model": [88.0],
            "ed_triage_hr": [120.0],
        }
    )
    normalized = normalize_classifier_input_schema(df)
    assert normalized["hr"].tolist() == [88.0]


def test_ensure_required_columns_raises_informative_error() -> None:
    df = pd.DataFrame({"hadm_id": [1]})
    with pytest.raises(KeyError, match="classifier input: missing required columns"):
        ensure_required_columns(df, ["hadm_id", "subject_id"], context="classifier input")
