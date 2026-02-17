from pathlib import Path

import pandas as pd
import pytest

from hypercap_cc_nlp.analysis_core import (
    binary_crosstab_yes_no,
    classify_gas_source_overlap,
    classify_icd_category_vectorized,
    classify_inclusion_type_vectorized,
    ensure_required_columns,
    resolve_analysis_paths,
    symptom_distribution_by_overlap,
)


def test_resolve_analysis_paths_returns_canonical_workbook(tmp_path: Path) -> None:
    data_dir = tmp_path / "MIMIC tabular data"
    data_dir.mkdir()
    workbook = data_dir / "sample.xlsx"
    workbook.write_text("placeholder")

    resolved = resolve_analysis_paths(tmp_path, "sample.xlsx")
    assert resolved == workbook.resolve()


def test_resolve_analysis_paths_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Expected analysis input workbook"):
        resolve_analysis_paths(tmp_path, "missing.xlsx")


def test_ensure_required_columns_raises_informative_key_error() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    with pytest.raises(KeyError, match="Missing required columns"):
        ensure_required_columns(df, ["a", "c"])


def test_classify_icd_category_vectorized_precedence() -> None:
    df = pd.DataFrame(
        {
            "ICD10_J9602": [1, 0, 0, 0, 0, 0, 0],
            "ICD10_J9612": [1, 1, 0, 0, 0, 0, 0],
            "ICD10_J9622": [0, 0, 1, 0, 0, 0, 0],
            "ICD10_J9692": [0, 0, 0, 1, 0, 0, 0],
            "ICD10_E662": [0, 0, 0, 0, 1, 0, 0],
            "ICD9_27803": [0, 0, 0, 0, 0, 1, 0],
        }
    )

    out = classify_icd_category_vectorized(df)
    assert out.tolist() == [
        "Acute RF with hypoxia",
        "Acute RF with hypercapnia",
        "Acute RF with hypoxia & hypercapnia",
        "Respiratory failure, unspecified",
        "Obesity hypoventilation syndrome",
        "Obesity hypoventilation syndrome",
        "Other / None",
    ]


def test_classify_inclusion_type_vectorized_mapping() -> None:
    any_icd = pd.Series([1, 1, 0, 0])
    gas_any = pd.Series([1, 0, 1, 0])
    out = classify_inclusion_type_vectorized(any_icd, gas_any)
    assert out.tolist() == ["Both", "ICD_only", "Gas_only", "Neither"]


def test_binary_crosstab_yes_no_has_stable_columns() -> None:
    df = pd.DataFrame(
        {
            "RFV1_name": ["Resp", "Resp", "Cardiac"],
            "flag": [1, 1, 1],
        }
    )
    out = binary_crosstab_yes_no(df, "RFV1_name", "flag")

    assert list(out.columns) == ["No", "Yes", "Percent_yes"]
    assert out["No"].sum() == 0
    assert out.loc["Resp", "Yes"] == 2
    assert out.loc["Cardiac", "Percent_yes"] == 100.0


def test_symptom_distribution_by_overlap_percent_sums() -> None:
    df = pd.DataFrame(
        {
            "overlap": ["ABG-only", "ABG-only", "ABG-only", "VBG-only", "VBG-only"],
            "RFV1_name": ["Resp", "Resp", "Neuro", "Cardiac", "Resp"],
        }
    )

    counts, pivot = symptom_distribution_by_overlap(
        df,
        group_col="overlap",
        symptom_col="RFV1_name",
        top_k=1,
    )

    summed = counts.groupby("overlap")["Percent"].sum().round(1)
    assert summed.to_dict() == {"ABG-only": 100.0, "VBG-only": 100.0}
    assert set(pivot.columns.tolist()) == {"ABG-only", "VBG-only"}


def test_classify_gas_source_overlap_includes_other_strata() -> None:
    labels = classify_gas_source_overlap(
        pd.Series([1, 1, 0, 0, 0]),
        pd.Series([0, 1, 1, 0, 0]),
        pd.Series([0, 1, 1, 1, 0]),
    )
    assert labels.tolist() == [
        "ABG-only",
        "ABG+VBG+OTHER",
        "VBG+OTHER",
        "OTHER-only",
        "No-gas",
    ]
