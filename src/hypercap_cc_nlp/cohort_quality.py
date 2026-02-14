"""Quality helpers for cohort notebook data contracts and diagnostics."""

from __future__ import annotations

import re
from typing import Any, Mapping

import pandas as pd

OMR_RESULT_NAMES = ("bmi", "height", "weight")
OMR_OUTPUT_COLUMNS = (
    "bmi_closest_pre_ed",
    "height_closest_pre_ed",
    "weight_closest_pre_ed",
)

OMR_PROVENANCE_COLUMNS = (
    "anthro_timing_tier",
    "anthro_days_offset",
    "anthro_chartdate",
    "anthro_timing_uncertain",
)

OMR_TIMING_TIERS = ("pre_ed_365", "post_ed_365", "missing")

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

DEFAULT_VITALS_MODEL_RANGES: dict[str, tuple[float, float]] = {
    "ed_triage_hr": (20.0, 250.0),
    "ed_first_hr": (20.0, 250.0),
    "ed_triage_rr": (4.0, 80.0),
    "ed_first_rr": (4.0, 80.0),
    "ed_triage_sbp": (40.0, 300.0),
    "ed_first_sbp": (40.0, 300.0),
    "ed_triage_dbp": (20.0, 200.0),
    "ed_first_dbp": (20.0, 200.0),
    "ed_triage_o2sat": (40.0, 100.0),
    "ed_first_o2sat": (40.0, 100.0),
    "ed_triage_temp": (90.0, 110.0),
    "ed_first_temp": (90.0, 110.0),
}

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
    """Attach closest anthropometrics with tiered timing fallback.

    Args:
        ed_df: ED-stay grain dataframe with ``ed_stay_id``, ``subject_id``,
            and ``ed_intime``.
        omr_df: Prepared OMR records from ``prepare_omr_records``.
        window_days: Inclusion window in days before/after ED arrival.
    """

    def _with_default_anthro_columns(frame: pd.DataFrame) -> pd.DataFrame:
        updated = frame.copy()
        for column_name in OMR_OUTPUT_COLUMNS:
            if column_name not in updated.columns:
                updated[column_name] = pd.NA
        if "anthro_timing_tier" not in updated.columns:
            updated["anthro_timing_tier"] = "missing"
        else:
            updated["anthro_timing_tier"] = updated["anthro_timing_tier"].fillna("missing")
        if "anthro_days_offset" not in updated.columns:
            updated["anthro_days_offset"] = pd.Series([pd.NA] * len(updated), dtype="Int64")
        else:
            updated["anthro_days_offset"] = pd.to_numeric(
                updated["anthro_days_offset"], errors="coerce"
            ).astype("Int64")
        if "anthro_chartdate" not in updated.columns:
            updated["anthro_chartdate"] = pd.NaT
        else:
            updated["anthro_chartdate"] = pd.to_datetime(
                updated["anthro_chartdate"], errors="coerce"
            )
        if "anthro_timing_uncertain" not in updated.columns:
            updated["anthro_timing_uncertain"] = pd.Series(
                [pd.NA] * len(updated), dtype="boolean"
            )
        else:
            updated["anthro_timing_uncertain"] = updated[
                "anthro_timing_uncertain"
            ].astype("boolean")

        missing_mask = updated["anthro_timing_tier"].eq("missing")
        pre_mask = updated["anthro_timing_tier"].eq("pre_ed_365")
        post_mask = updated["anthro_timing_tier"].eq("post_ed_365")
        updated.loc[pre_mask, "anthro_timing_uncertain"] = False
        updated.loc[post_mask, "anthro_timing_uncertain"] = True
        updated.loc[missing_mask, "anthro_timing_uncertain"] = pd.NA

        return updated

    def _tier_counts(frame: pd.DataFrame) -> dict[str, int]:
        tier_series = frame["anthro_timing_tier"].astype(str).fillna("missing")
        return {
            "pre_ed_365": int((tier_series == "pre_ed_365").sum()),
            "post_ed_365": int((tier_series == "post_ed_365").sum()),
            "missing": int((tier_series == "missing").sum()),
        }

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

    empty_window_diagnostics = {
        "nonnegative_candidate_rows": 0,
        "pre_window_candidate_rows": 0,
        "post_window_candidate_rows": 0,
        "closest_absolute_candidate_rows": 0,
        "within_window_candidate_rows": 0,
    }

    ed_norm = ed_df[["ed_stay_id", "subject_id", "ed_intime"]].copy()
    ed_norm["subject_id"] = _to_int64(ed_norm["subject_id"])
    ed_norm["ed_intime_dt"] = pd.to_datetime(ed_norm["ed_intime"], errors="coerce")
    ed_norm["ed_date_dt"] = ed_norm["ed_intime_dt"].dt.floor("D")
    ed_norm = ed_norm.loc[ed_norm["subject_id"].notna() & ed_norm["ed_date_dt"].notna()]

    if omr_df.empty or ed_norm.empty:
        updated = _with_default_anthro_columns(ed_df)
        tier_counts = _tier_counts(updated)
        total_rows = max(int(len(updated)), 1)
        diagnostics.update(
            {
                "ed_rows_eligible_for_join": int(len(ed_norm)),
                "subject_overlap_count": 0,
                "candidate_rows_after_subject_join": 0,
                **empty_window_diagnostics,
                "within_window_candidate_rows": 0,
                "eligible_ed_stays_with_candidates": 0,
                "attached_non_null_counts": {
                    column_name: int(updated[column_name].notna().sum())
                    for column_name in OMR_OUTPUT_COLUMNS
                },
                "attached_any_non_null_rows": int(
                    updated[list(OMR_OUTPUT_COLUMNS)].notna().any(axis=1).sum()
                ),
                "selected_tier_counts": tier_counts,
                "selected_tier_rates": {
                    key: float(value / total_rows) for key, value in tier_counts.items()
                },
                "timing_uncertain_count": int(
                    updated["anthro_timing_uncertain"].fillna(False).sum()
                ),
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
    pre_window_mask = (
        merged["days_before"].notna()
        & merged["days_before"].ge(0)
        & merged["days_before"].le(window_days)
    )
    post_window_mask = (
        merged["days_before"].notna()
        & merged["days_before"].lt(0)
        & merged["days_before"].ge(-window_days)
    )
    abs_window_mask = (
        merged["days_before"].notna()
        & merged["days_before"].abs().le(window_days)
    )

    diagnostics["nonnegative_candidate_rows"] = int((merged["days_before"] >= 0).sum())
    diagnostics["pre_window_candidate_rows"] = int(pre_window_mask.sum())
    diagnostics["post_window_candidate_rows"] = int(post_window_mask.sum())
    diagnostics["closest_absolute_candidate_rows"] = int(abs_window_mask.sum())

    pre_candidates = merged.loc[pre_window_mask].copy()
    post_candidates = merged.loc[post_window_mask].copy()
    diagnostics["within_window_candidate_rows"] = int(len(pre_candidates))

    selected_parts: list[pd.DataFrame] = []
    selected_stays: set[Any] = set()

    if not pre_candidates.empty:
        pre_selected = (
            pre_candidates.sort_values(
                ["ed_stay_id", "days_before", "chartdate_dt"],
                ascending=[True, True, False],
            )
            .groupby("ed_stay_id", as_index=False)
            .first()
        )
        pre_selected["anthro_timing_tier"] = "pre_ed_365"
        selected_parts.append(pre_selected)
        selected_stays.update(pre_selected["ed_stay_id"].tolist())

    if not post_candidates.empty:
        post_candidates["days_after_abs"] = post_candidates["days_before"].abs()
        post_selected = (
            post_candidates.sort_values(
                ["ed_stay_id", "days_after_abs", "chartdate_dt"],
                ascending=[True, True, True],
            )
            .groupby("ed_stay_id", as_index=False)
            .first()
        )
        post_selected = post_selected.loc[
            ~post_selected["ed_stay_id"].isin(selected_stays)
        ].copy()
        if not post_selected.empty:
            post_selected["anthro_timing_tier"] = "post_ed_365"
            selected_parts.append(post_selected)

    if selected_parts:
        selected = pd.concat(selected_parts, ignore_index=True)
        selected = selected.rename(
            columns={
                "bmi": "bmi_closest_pre_ed",
                "height": "height_closest_pre_ed",
                "weight": "weight_closest_pre_ed",
                "chartdate_dt": "anthro_chartdate",
                "days_before": "anthro_days_offset",
            }
        )
        selected["anthro_chartdate"] = pd.to_datetime(
            selected["anthro_chartdate"], errors="coerce"
        )
        selected["anthro_days_offset"] = pd.to_numeric(
            selected["anthro_days_offset"], errors="coerce"
        ).astype("Int64")
        selected["anthro_timing_uncertain"] = selected["anthro_timing_tier"].eq(
            "post_ed_365"
        )

        updates = selected[
            [
                "ed_stay_id",
                *OMR_OUTPUT_COLUMNS,
                *OMR_PROVENANCE_COLUMNS,
            ]
        ].copy()
        updated = ed_df.merge(updates, on="ed_stay_id", how="left")
    else:
        updated = ed_df.copy()

    updated = _with_default_anthro_columns(updated)

    diagnostics["eligible_ed_stays_with_candidates"] = int(
        updated["anthro_timing_tier"].isin({"pre_ed_365", "post_ed_365"}).sum()
    )
    diagnostics["attached_non_null_counts"] = {
        column_name: int(updated[column_name].notna().sum())
        for column_name in OMR_OUTPUT_COLUMNS
    }
    diagnostics["attached_any_non_null_rows"] = int(
        updated[list(OMR_OUTPUT_COLUMNS)].notna().any(axis=1).sum()
    )
    tier_counts = _tier_counts(updated)
    diagnostics["selected_tier_counts"] = tier_counts
    total_rows = max(int(len(updated)), 1)
    diagnostics["selected_tier_rates"] = {
        key: float(value / total_rows) for key, value in tier_counts.items()
    }
    diagnostics["timing_uncertain_count"] = int(
        updated["anthro_timing_uncertain"].fillna(False).sum()
    )

    return updated, diagnostics


def summarize_gas_source(panel_df: pd.DataFrame) -> dict[str, Any]:
    """Summarize gas source composition from panel-level records."""
    total_rows = int(len(panel_df))
    if total_rows == 0:
        return {
            "panel_rows": 0,
            "source_present": bool("source" in panel_df.columns),
            "source_counts": {},
            "source_rates": {},
            "all_other_or_unknown": False,
        }

    if "source" not in panel_df.columns:
        return {
            "panel_rows": total_rows,
            "source_present": False,
            "source_counts": {},
            "source_rates": {},
            "all_other_or_unknown": True,
        }

    source = (
        panel_df["source"]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": "other", "nan": "other", "none": "other"})
        .fillna("other")
    )
    source_counts = {str(key): int(value) for key, value in source.value_counts(dropna=False).items()}
    source_rates = {key: float(value / total_rows) for key, value in source_counts.items()}
    all_other_or_unknown = bool(
        total_rows > 0 and set(source_counts.keys()).issubset({"other", "unknown"})
    )

    return {
        "panel_rows": total_rows,
        "source_present": True,
        "source_counts": source_counts,
        "source_rates": source_rates,
        "all_other_or_unknown": all_other_or_unknown,
    }


def assert_gas_source_coverage(
    gas_source_audit: Mapping[str, Any],
    *,
    fail_on_all_other_source: bool = True,
) -> None:
    """Fail fast when source attribution collapses to all other/unknown."""
    if not fail_on_all_other_source:
        return
    if int(gas_source_audit.get("panel_rows", 0)) <= 0:
        return
    if bool(gas_source_audit.get("all_other_or_unknown", False)):
        raise ValueError(
            "Gas source attribution classified all panel rows as other/unknown. "
            "Set COHORT_FAIL_ON_ALL_OTHER_SOURCE=0 to bypass this guard."
        )


def build_first_other_pco2_audit(ed_df: pd.DataFrame) -> pd.DataFrame:
    """Build route-stratified audit summary for first_other_pco2 values.

    This helper is intentionally tolerant for notebook QA execution: if the
    required fields are not present, it returns a sentinel audit row instead of
    raising.
    """
    columns = [
        "source",
        "count_nonnull",
        "mean",
        "median",
        "q25",
        "q75",
        "p95",
        "max",
        "pct_ge_80",
        "pct_ge_100",
        "pct_ge_150",
        "pct_eq_160",
        "top_values",
        "status",
        "missing_columns",
    ]
    required = {"first_other_pco2", "first_other_src"}
    missing = sorted(required.difference(ed_df.columns))
    if missing:
        return pd.DataFrame(
            [
                {
                    "source": "UNAVAILABLE",
                    "count_nonnull": 0,
                    "mean": None,
                    "median": None,
                    "q25": None,
                    "q75": None,
                    "p95": None,
                    "max": None,
                    "pct_ge_80": 0.0,
                    "pct_ge_100": 0.0,
                    "pct_ge_150": 0.0,
                    "pct_eq_160": 0.0,
                    "top_values": {},
                    "status": "missing_columns",
                    "missing_columns": ",".join(missing),
                }
            ],
            columns=columns,
        )

    frame = ed_df[["first_other_src", "first_other_pco2"]].copy()
    frame["first_other_src"] = (
        frame["first_other_src"].astype(str).str.strip().str.upper().replace({"": "UNKNOWN", "NAN": "UNKNOWN"})
    )
    frame["first_other_pco2"] = pd.to_numeric(frame["first_other_pco2"], errors="coerce")
    frame = frame.loc[frame["first_other_pco2"].notna()].copy()

    if frame.empty:
        return pd.DataFrame(
            [
                {
                    "source": "UNAVAILABLE",
                    "count_nonnull": 0,
                    "mean": None,
                    "median": None,
                    "q25": None,
                    "q75": None,
                    "p95": None,
                    "max": None,
                    "pct_ge_80": 0.0,
                    "pct_ge_100": 0.0,
                    "pct_ge_150": 0.0,
                    "pct_eq_160": 0.0,
                    "top_values": {},
                    "status": "no_nonnull_values",
                    "missing_columns": "",
                }
            ],
            columns=columns,
        )

    rows: list[dict[str, Any]] = []
    for source_name, group in frame.groupby("first_other_src"):
        values = group["first_other_pco2"]
        rows.append(
            {
                "source": source_name,
                "count_nonnull": int(values.shape[0]),
                "mean": float(values.mean()),
                "median": float(values.median()),
                "q25": float(values.quantile(0.25)),
                "q75": float(values.quantile(0.75)),
                "p95": float(values.quantile(0.95)),
                "max": float(values.max()),
                "pct_ge_80": float((values >= 80).mean()),
                "pct_ge_100": float((values >= 100).mean()),
                "pct_ge_150": float((values >= 150).mean()),
                "pct_eq_160": float((values == 160).mean()),
                "top_values": {
                    str(key): int(value)
                    for key, value in values.value_counts().head(10).items()
                },
                "status": "ok",
                "missing_columns": "",
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values("source").reset_index(drop=True)


def add_vitals_model_fields(
    ed_df: pd.DataFrame,
    *,
    ranges: Mapping[str, tuple[float, float]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create cleaned vitals model fields and return field-level outlier audit.

    The function preserves original raw columns and writes cleaned values to
    ``<raw_column>_model``. Out-of-range values are nulled in model fields.
    """
    resolved_ranges = dict(ranges or DEFAULT_VITALS_MODEL_RANGES)
    updated = ed_df.copy()
    audit_rows: list[dict[str, Any]] = []

    for raw_column, (lower_bound, upper_bound) in resolved_ranges.items():
        if raw_column not in updated.columns:
            continue
        numeric = pd.to_numeric(updated[raw_column], errors="coerce")
        out_of_range = numeric.notna() & (
            (numeric < lower_bound) | (numeric > upper_bound)
        )
        model_column = f"{raw_column}_model"
        updated[model_column] = numeric.where(~out_of_range)
        nonnull_n = int(numeric.notna().sum())
        outlier_n = int(out_of_range.sum())
        audit_rows.append(
            {
                "raw_column": raw_column,
                "model_column": model_column,
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound),
                "nonnull_n": nonnull_n,
                "out_of_range_n": outlier_n,
                "out_of_range_pct": float(outlier_n / nonnull_n) if nonnull_n else 0.0,
            }
        )

    audit = pd.DataFrame(audit_rows).sort_values("raw_column").reset_index(drop=True)
    return updated, audit


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
    *,
    expected_sparse_fields: set[str] | None = None,
) -> pd.DataFrame:
    """Classify field-level missingness into expected and unexpected categories."""
    rows: list[dict[str, Any]] = []
    total_rows = max(int(len(ed_df)), 1)
    expected_structural = set(EXPECTED_STRUCTURAL_NULL_FIELDS)
    expected_sparse = set(expected_sparse_fields or set())

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
        elif field_name in expected_sparse and missing_pct >= 1.0:
            expectation = "expected_sparse"
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
