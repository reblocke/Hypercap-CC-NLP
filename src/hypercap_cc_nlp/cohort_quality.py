"""Quality helpers for cohort notebook data contracts and diagnostics."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

OMR_RESULT_NAMES = ("bmi", "height", "weight")
OMR_OUTPUT_COLUMNS = (
    "bmi_closest_pre_ed",
    "height_closest_pre_ed",
    "weight_closest_pre_ed",
)

EXPECTED_STRUCTURAL_NULL_FIELDS = (
    "poc_abg_ph_uom",
    "poc_vbg_ph_uom",
    "poc_other_ph_uom",
)

PACO2_VALUE_UOM_PAIRS = (
    ("poc_abg_paco2", "poc_abg_paco2_uom"),
    ("poc_vbg_paco2", "poc_vbg_paco2_uom"),
    ("poc_other_paco2", "poc_other_paco2_uom"),
)

_NUMERIC_TOKEN_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)")


def _to_int64(series: pd.Series) -> pd.Series:
    """Return pandas nullable integer series for key columns."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def prepare_omr_records(omr_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize OMR records into a deterministic schema.

    Required source columns: ``subject_id``, ``chartdate``, ``result_name``,
    ``result_value``.

    Returns:
        DataFrame with columns:
        ``subject_id`` (Int64), ``chartdate_dt`` (datetime64),
        ``result_name`` (lowercase), and ``result_value_num`` (float).
    """
    required = {"subject_id", "chartdate", "result_name", "result_value"}
    missing = sorted(required.difference(omr_df.columns))
    if missing:
        raise KeyError(f"prepare_omr_records missing required columns: {missing}")

    prepared = omr_df.copy()
    prepared["subject_id"] = _to_int64(prepared["subject_id"])
    prepared["chartdate_dt"] = pd.to_datetime(prepared["chartdate"], errors="coerce")
    prepared["result_name"] = (
        prepared["result_name"].astype(str).str.strip().str.lower()
    )
    prepared["result_value_num"] = pd.to_numeric(
        prepared["result_value"]
        .astype(str)
        .str.extract(_NUMERIC_TOKEN_PATTERN, expand=False),
        errors="coerce",
    )

    prepared = prepared.loc[prepared["result_name"].isin(OMR_RESULT_NAMES)].copy()
    prepared = prepared.loc[
        prepared["subject_id"].notna() & prepared["chartdate_dt"].notna()
    ].copy()

    return prepared[["subject_id", "chartdate_dt", "result_name", "result_value_num"]]


def attach_closest_pre_ed_omr(
    ed_df: pd.DataFrame,
    omr_df: pd.DataFrame,
    window_days: int = 365,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Attach closest pre-ED anthropometrics and return diagnostics.

    Args:
        ed_df: ED-stay grain dataframe with ``ed_stay_id``, ``subject_id``,
            and ``ed_intime``.
        omr_df: Prepared OMR records from ``prepare_omr_records``.
        window_days: Inclusion window in days before ED arrival.
    """
    required_ed = {"ed_stay_id", "subject_id", "ed_intime"}
    missing_ed = sorted(required_ed.difference(ed_df.columns))
    if missing_ed:
        raise KeyError(f"attach_closest_pre_ed_omr missing ED columns: {missing_ed}")

    required_omr = {"subject_id", "chartdate_dt", "result_name", "result_value_num"}
    missing_omr = sorted(required_omr.difference(omr_df.columns))
    if missing_omr:
        raise KeyError(f"attach_closest_pre_ed_omr missing OMR columns: {missing_omr}")

    diagnostics: dict[str, Any] = {
        "window_days": int(window_days),
        "source_rows_prepared": int(len(omr_df)),
        "parsed_value_rows": int(omr_df["result_value_num"].notna().sum()),
        "ed_rows": int(len(ed_df)),
    }

    ed_norm = ed_df[["ed_stay_id", "subject_id", "ed_intime"]].copy()
    ed_norm["subject_id"] = _to_int64(ed_norm["subject_id"])
    ed_norm["ed_intime_dt"] = pd.to_datetime(ed_norm["ed_intime"], errors="coerce")
    ed_norm["ed_date_dt"] = ed_norm["ed_intime_dt"].dt.floor("D")
    ed_norm = ed_norm.loc[ed_norm["subject_id"].notna() & ed_norm["ed_date_dt"].notna()]

    if omr_df.empty or ed_norm.empty:
        updated = ed_df.copy()
        for column_name in OMR_OUTPUT_COLUMNS:
            if column_name not in updated.columns:
                updated[column_name] = pd.NA
        diagnostics.update(
            {
                "ed_rows_eligible_for_join": int(len(ed_norm)),
                "subject_overlap_count": 0,
                "candidate_rows_after_subject_join": 0,
                "within_window_candidate_rows": 0,
                "eligible_ed_stays_with_candidates": 0,
                "attached_non_null_counts": {
                    column_name: int(updated[column_name].notna().sum())
                    for column_name in OMR_OUTPUT_COLUMNS
                },
            }
        )
        return updated, diagnostics

    omr_pivot = (
        omr_df.pivot_table(
            index=["subject_id", "chartdate_dt"],
            columns="result_name",
            values="result_value_num",
            aggfunc="first",
        )
        .reset_index()
        .copy()
    )

    shared_subjects = set(ed_norm["subject_id"].dropna().astype(int)).intersection(
        set(omr_pivot["subject_id"].dropna().astype(int))
    )
    diagnostics["ed_rows_eligible_for_join"] = int(len(ed_norm))
    diagnostics["subject_overlap_count"] = int(len(shared_subjects))

    merged = ed_norm.merge(omr_pivot, on="subject_id", how="left")
    diagnostics["candidate_rows_after_subject_join"] = int(
        merged["chartdate_dt"].notna().sum()
    )

    merged["days_before"] = (
        merged["ed_date_dt"] - pd.to_datetime(merged["chartdate_dt"], errors="coerce")
    ).dt.days
    valid_days = merged["days_before"].dropna()
    diagnostics["days_before_min"] = (
        int(valid_days.min()) if not valid_days.empty else None
    )
    diagnostics["days_before_max"] = (
        int(valid_days.max()) if not valid_days.empty else None
    )
    diagnostics["nonnegative_candidate_rows"] = int((merged["days_before"] >= 0).sum())
    within_window = merged.loc[
        merged["days_before"].notna()
        & merged["days_before"].ge(0)
        & merged["days_before"].le(window_days)
    ].copy()

    diagnostics["within_window_candidate_rows"] = int(len(within_window))

    if within_window.empty:
        updated = ed_df.copy()
        for column_name in OMR_OUTPUT_COLUMNS:
            if column_name not in updated.columns:
                updated[column_name] = pd.NA
        diagnostics["eligible_ed_stays_with_candidates"] = 0
        diagnostics["attached_non_null_counts"] = {
            column_name: int(updated[column_name].notna().sum())
            for column_name in OMR_OUTPUT_COLUMNS
        }
        return updated, diagnostics

    closest = (
        within_window.sort_values(
            ["ed_stay_id", "days_before", "chartdate_dt"],
            ascending=[True, True, False],
        )
        .groupby("ed_stay_id", as_index=False)
        .first()
        .rename(
            columns={
                "bmi": "bmi_closest_pre_ed",
                "height": "height_closest_pre_ed",
                "weight": "weight_closest_pre_ed",
            }
        )
    )

    updates = closest[
        ["ed_stay_id", "bmi_closest_pre_ed", "height_closest_pre_ed", "weight_closest_pre_ed"]
    ]
    updated = ed_df.merge(updates, on="ed_stay_id", how="left")

    diagnostics["eligible_ed_stays_with_candidates"] = int(closest["ed_stay_id"].nunique())
    diagnostics["attached_non_null_counts"] = {
        column_name: int(updated[column_name].notna().sum())
        for column_name in OMR_OUTPUT_COLUMNS
    }

    return updated, diagnostics


def evaluate_uom_expectations(ed_df: pd.DataFrame) -> dict[str, Any]:
    """Evaluate expected-null and value/uom consistency rules."""
    structural_nulls: dict[str, dict[str, Any]] = {}
    for field_name in EXPECTED_STRUCTURAL_NULL_FIELDS:
        if field_name not in ed_df.columns:
            continue
        structural_nulls[field_name] = {
            "present": True,
            "missing_n": int(ed_df[field_name].isna().sum()),
            "missing_pct": float(ed_df[field_name].isna().mean()),
            "all_null": bool(ed_df[field_name].isna().all()),
        }

    paco2_checks: dict[str, dict[str, Any]] = {}
    for value_column, uom_column in PACO2_VALUE_UOM_PAIRS:
        if value_column not in ed_df.columns or uom_column not in ed_df.columns:
            paco2_checks[uom_column] = {
                "present": False,
                "reason": "missing_value_or_uom_column",
            }
            continue

        value_present = ed_df[value_column].notna()
        uom_lower = ed_df[uom_column].astype(str).str.strip().str.lower()
        missing_uom_with_value = int((value_present & ed_df[uom_column].isna()).sum())
        non_mmhg_uom_with_value = int(
            (value_present & ed_df[uom_column].notna() & uom_lower.ne("mmhg")).sum()
        )

        paco2_checks[uom_column] = {
            "present": True,
            "paired_value_column": value_column,
            "value_rows": int(value_present.sum()),
            "missing_uom_when_value_present": missing_uom_with_value,
            "non_mmhg_uom_when_value_present": non_mmhg_uom_with_value,
            "passes": bool(
                missing_uom_with_value == 0 and non_mmhg_uom_with_value == 0
            ),
        }

    return {
        "expected_structural_null_fields": list(EXPECTED_STRUCTURAL_NULL_FIELDS),
        "structural_null_checks": structural_nulls,
        "paco2_uom_checks": paco2_checks,
    }


def classify_missingness_expectations(
    ed_df: pd.DataFrame,
    target_fields: list[str],
) -> pd.DataFrame:
    """Classify field-level missingness into expected and unexpected categories."""
    rows: list[dict[str, Any]] = []
    total_rows = max(int(len(ed_df)), 1)
    expected_structural = set(EXPECTED_STRUCTURAL_NULL_FIELDS)

    for field_name in target_fields:
        if field_name not in ed_df.columns:
            rows.append(
                {
                    "field": field_name,
                    "missing_n": int(len(ed_df)),
                    "missing_pct": 1.0,
                    "expectation": "missing_column",
                }
            )
            continue

        missing_n = int(ed_df[field_name].isna().sum())
        missing_pct = float(missing_n / total_rows)

        if field_name in expected_structural:
            expectation = "expected_structural_null"
        elif missing_pct >= 1.0:
            expectation = "unexpected_full_null"
        elif missing_pct > 0.0:
            expectation = "conditional_sparse"
        else:
            expectation = "complete"

        rows.append(
            {
                "field": field_name,
                "missing_n": missing_n,
                "missing_pct": missing_pct,
                "expectation": expectation,
            }
        )

    return pd.DataFrame(rows)
