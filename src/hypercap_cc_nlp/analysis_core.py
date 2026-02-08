"""Core analysis helpers for Hypercap CC NLP notebook workflows."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def resolve_analysis_paths(work_dir: Path, filename: str) -> Path:
    """Return the canonical analysis input path under ``MIMIC tabular data``.

    Args:
        work_dir: Repository working directory.
        filename: Workbook filename expected under ``MIMIC tabular data``.

    Returns:
        Resolved absolute file path.

    Raises:
        FileNotFoundError: If the expected workbook does not exist.
    """
    input_path = (work_dir / "MIMIC tabular data" / filename).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(
            "Expected analysis input workbook was not found at "
            f"{input_path}. Verify WORK_DIR and run the upstream notebook first."
        )
    return input_path


def ensure_required_columns(df: pd.DataFrame, required: list[str]) -> None:
    """Validate that all required columns are present in ``df``.

    Args:
        df: Input DataFrame.
        required: Required column names.

    Raises:
        KeyError: If one or more required columns are missing.
    """
    missing = sorted(set(required).difference(df.columns))
    if missing:
        raise KeyError(f"Missing required columns: {missing}")


def to_binary_flag(series: pd.Series) -> pd.Series:
    """Coerce any scalar series into a deterministic 0/1 indicator."""
    numeric = pd.to_numeric(series, errors="coerce").fillna(0)
    return (numeric > 0).astype(int)


def _binary_or_zero(df: pd.DataFrame, column: str) -> pd.Series:
    """Return a 0/1 series for ``column`` or zeros if missing."""
    if column in df.columns:
        return to_binary_flag(df[column])
    return pd.Series(0, index=df.index, dtype="int64")


def classify_icd_category_vectorized(df: pd.DataFrame) -> pd.Series:
    """Vectorized ICD category mapping with explicit precedence.

    Precedence order matches the legacy row-wise implementation.
    """
    j9602 = _binary_or_zero(df, "ICD10_J9602")
    j9612 = _binary_or_zero(df, "ICD10_J9612")
    j9622 = _binary_or_zero(df, "ICD10_J9622")
    j9692 = _binary_or_zero(df, "ICD10_J9692")
    e662 = _binary_or_zero(df, "ICD10_E662")
    icd9_27803 = _binary_or_zero(df, "ICD9_27803")

    category = np.select(
        [
            j9602.eq(1),
            j9612.eq(1),
            j9622.eq(1),
            j9692.eq(1),
            e662.eq(1) | icd9_27803.eq(1),
        ],
        [
            "Acute RF with hypoxia",
            "Acute RF with hypercapnia",
            "Acute RF with hypoxia & hypercapnia",
            "Respiratory failure, unspecified",
            "Obesity hypoventilation syndrome",
        ],
        default="Other / None",
    )

    return pd.Series(category, index=df.index, name="icd_category")


def classify_inclusion_type_vectorized(
    any_icd: pd.Series, gas_any: pd.Series
) -> pd.Series:
    """Vectorized inclusion source classification for ICD/gas overlap."""
    any_icd_bin = to_binary_flag(any_icd)
    gas_any_bin = to_binary_flag(gas_any)

    labels = np.select(
        [
            any_icd_bin.eq(1) & gas_any_bin.eq(1),
            any_icd_bin.eq(1) & gas_any_bin.eq(0),
            any_icd_bin.eq(0) & gas_any_bin.eq(1),
        ],
        ["Both", "ICD_only", "Gas_only"],
        default="Neither",
    )

    return pd.Series(labels, index=any_icd.index, name="inclusion_type")


def binary_crosstab_yes_no(
    df: pd.DataFrame, row_col: str, flag_col: str
) -> pd.DataFrame:
    """Build a stable ``No``/``Yes`` crosstab and row-level percent yes."""
    ensure_required_columns(df, [row_col, flag_col])

    tab = pd.crosstab(
        df[row_col], to_binary_flag(df[flag_col]), margins=False, dropna=False
    )
    tab = tab.reindex(columns=[0, 1], fill_value=0)
    tab.columns = ["No", "Yes"]

    row_totals = tab.sum(axis=1).replace(0, np.nan)
    tab["Percent_yes"] = (tab["Yes"] / row_totals * 100).round(1).fillna(0)
    return tab


def symptom_distribution_by_overlap(
    df: pd.DataFrame,
    group_col: str,
    symptom_col: str,
    top_k: int = 10,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return overlap-group symptom counts and a percent pivot table."""
    ensure_required_columns(df, [group_col, symptom_col])

    tmp = df.dropna(subset=[group_col, symptom_col]).copy()
    if tmp.empty:
        counts = pd.DataFrame(columns=[group_col, "symptom_group", "N", "Percent"])
        pivot = pd.DataFrame()
        return counts, pivot

    top_symptoms = tmp[symptom_col].value_counts(dropna=False).head(top_k).index
    tmp["symptom_group"] = tmp[symptom_col].where(
        tmp[symptom_col].isin(top_symptoms), "Other"
    )

    counts = (
        tmp.groupby([group_col, "symptom_group"], dropna=False)
        .size()
        .reset_index(name="N")
    )
    counts["Percent"] = (
        counts.groupby(group_col)["N"].transform(lambda x: x / x.sum() * 100).round(1)
    )

    pivot = counts.pivot_table(
        index="symptom_group",
        columns=group_col,
        values="Percent",
        fill_value=0,
    ).round(1)

    return counts, pivot
