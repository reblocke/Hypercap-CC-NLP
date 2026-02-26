"""Pipeline contract checks for cohort/classifier handoff artifacts."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .classifier_quality import validate_classifier_contract
from .workflow_contracts import (
    CANONICAL_COHORT_FILENAME,
    CANONICAL_NLP_FILENAME,
    DATA_DIRNAME,
    PRIOR_RUNS_DIRNAME,
)

COHORT_POC_PCO2_MEDIAN_MIN = 45.0
COHORT_POC_PCO2_MEDIAN_MAX = 80.0
COHORT_POC_PCO2_FAIL_ENABLED = True
COHORT_HCO3_GAS_WARN_MIN_COVERAGE = 0.80
COHORT_HCO3_GAS_FAIL_MIN_COVERAGE = 0.60
COHORT_HCO3_ICD_ONLY_WARN_MIN_COVERAGE = 0.50
COHORT_PO2_TRIPLET_WARN_MIN_COVERAGE = 0.20
COHORT_FIRST_GAS_ANCHOR_TOLERANCE = 0
COHORT_REQUIRED_AUDIT_SUFFIXES = (
    "blood_gas_itemid_manifest_audit.csv",
    "pco2_itemid_qc_audit.csv",
    "hco3_itemid_qc_audit.csv",
    "pco2_source_distribution_audit.csv",
    "pco2_window_max_contributor_audit.csv",
    "blood_gas_triplet_completeness_audit.csv",
    "qualifying_pco2_distribution_by_type_audit.csv",
    "other_route_quarantine_audit.csv",
    "first_gas_anchor_audit.csv",
    "hco3_integrity_audit.csv",
    "hco3_coverage_audit.csv",
    "timing_integrity_audit.csv",
    "ventilation_timing_audit.csv",
    "anthropometric_cleaning_audit.csv",
)

COHORT_REQUIRED_ED_VITALS_CLEAN_COLUMNS: dict[str, tuple[str, ...]] = {
    "ed_triage_temp": (
        "ed_triage_temp_f_clean",
        "ed_triage_temp_was_celsius_like",
        "ed_triage_temp_out_of_range",
    ),
    "ed_first_temp": (
        "ed_first_temp_f_clean",
        "ed_first_temp_was_celsius_like",
        "ed_first_temp_out_of_range",
    ),
    "ed_triage_pain": (
        "ed_triage_pain_clean",
        "ed_triage_pain_is_sentinel_13",
        "ed_triage_pain_out_of_range",
    ),
    "ed_first_pain": (
        "ed_first_pain_clean",
        "ed_first_pain_is_sentinel_13",
        "ed_first_pain_out_of_range",
    ),
    "ed_triage_sbp": ("ed_triage_sbp_clean", "ed_triage_sbp_out_of_range"),
    "ed_triage_dbp": ("ed_triage_dbp_clean", "ed_triage_dbp_out_of_range"),
    "ed_first_sbp": ("ed_first_sbp_clean", "ed_first_sbp_out_of_range"),
    "ed_first_dbp": ("ed_first_dbp_clean", "ed_first_dbp_out_of_range"),
    "ed_triage_o2sat": (
        "ed_triage_o2sat_clean",
        "ed_triage_o2sat_gt_100",
        "ed_triage_o2sat_out_of_range",
        "ed_triage_o2sat_zero",
    ),
    "ed_first_o2sat": (
        "ed_first_o2sat_clean",
        "ed_first_o2sat_gt_100",
        "ed_first_o2sat_out_of_range",
        "ed_first_o2sat_zero",
    ),
}

COHORT_ED_VITALS_CLEAN_BOUNDS: dict[str, tuple[float, float]] = {
    "ed_triage_temp_f_clean": (50.0, 120.0),
    "ed_first_temp_f_clean": (50.0, 120.0),
    "ed_triage_pain_clean": (0.0, 10.0),
    "ed_first_pain_clean": (0.0, 10.0),
    "ed_triage_sbp_clean": (20.0, 300.0),
    "ed_first_sbp_clean": (20.0, 300.0),
    "ed_triage_dbp_clean": (10.0, 200.0),
    "ed_first_dbp_clean": (10.0, 200.0),
    "ed_triage_o2sat_clean": (0.0, 100.0),
    "ed_first_o2sat_clean": (0.0, 100.0),
}

COHORT_ANTHRO_MODEL_BOUNDS: dict[str, tuple[float, float]] = {
    "bmi_closest_pre_ed_model": (10.0, 100.0),
    "height_closest_pre_ed_model": (100.0, 230.0),
    "weight_closest_pre_ed_model": (25.0, 400.0),
}

COHORT_ANTHRO_ALLOWED_SOURCES = {"ED", "ICU", "HOSPITAL", "missing"}
COHORT_ANTHRO_CANONICAL_UOMS: dict[str, str] = {
    "bmi_closest_pre_ed_uom": "kg/m2",
    "height_closest_pre_ed_uom": "cm",
    "weight_closest_pre_ed_uom": "kg",
}
COHORT_ANTHRO_REQUIRED_UNIT_TIME_COLUMNS: dict[str, tuple[str, str]] = {
    "bmi_closest_pre_ed": ("bmi_closest_pre_ed_uom", "bmi_closest_pre_ed_time"),
    "height_closest_pre_ed": ("height_closest_pre_ed_uom", "height_closest_pre_ed_time"),
    "weight_closest_pre_ed": ("weight_closest_pre_ed_uom", "weight_closest_pre_ed_time"),
}


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _status_from_findings(findings: list[dict[str, str]]) -> str:
    has_error = any(item["severity"] == "error" for item in findings)
    has_warning = any(item["severity"] == "warning" for item in findings)
    if has_error:
        return "fail"
    if has_warning:
        return "warning"
    return "pass"


def validate_cohort_contract(
    df: pd.DataFrame,
    *,
    gas_source_other_warn_threshold: float = 0.50,
    gas_source_other_fail_threshold: float | None = None,
    min_bmi_coverage: float = 0.30,
) -> dict[str, Any]:
    """Validate deterministic cohort output contracts."""
    findings: list[dict[str, str]] = []
    hospital_los_negative_model_n = 0
    dt_first_imv_model_negative_n = 0
    dt_first_niv_model_negative_n = 0
    max_pco2_0_24h_lt_qualifying_n = 0
    max_pco2_0_6h_lt_qualifying_n = 0
    anthro_model_invalid_counts: dict[str, int] = {}
    po2_triplet_coverage_by_site: dict[str, float] = {}

    if "hadm_id" in df.columns:
        hadm_dup = int(df.duplicated(subset=["hadm_id"]).sum())
        if hadm_dup:
            findings.append(
                {
                    "severity": "error",
                    "code": "hadm_id_not_unique",
                    "message": f"Found {hadm_dup} duplicate hadm_id rows.",
                }
            )

    if "ed_stay_id" in df.columns:
        ed_dup = int(df.duplicated(subset=["ed_stay_id"]).sum())
        if ed_dup:
            findings.append(
                {
                    "severity": "error",
                    "code": "ed_stay_id_not_unique",
                    "message": f"Found {ed_dup} duplicate ed_stay_id rows.",
                }
            )

    required = {
        "abg_hypercap_threshold",
        "vbg_hypercap_threshold",
        "unknown_hypercap_threshold",
    }
    threshold_union_col: str | None = None
    if "pco2_threshold_0_24h" in df.columns:
        threshold_union_col = "pco2_threshold_0_24h"
    elif "pco2_threshold_any" in df.columns:
        threshold_union_col = "pco2_threshold_any"
        findings.append(
            {
                "severity": "warning",
                "code": "pco2_threshold_any_deprecated_alias",
                "message": (
                    "Using deprecated alias column pco2_threshold_any; "
                    "prefer pco2_threshold_0_24h."
                ),
            }
        )

    if required.issubset(df.columns) and threshold_union_col is not None:
        abg = pd.to_numeric(df["abg_hypercap_threshold"], errors="coerce").fillna(0).astype(int)
        vbg = pd.to_numeric(df["vbg_hypercap_threshold"], errors="coerce").fillna(0).astype(int)
        unknown = pd.to_numeric(df["unknown_hypercap_threshold"], errors="coerce").fillna(0).astype(int)
        any_reported = pd.to_numeric(df[threshold_union_col], errors="coerce").fillna(0).astype(int)
        any_expected = (abg | vbg | unknown).astype(int)
        mismatch = int((any_expected != any_reported).sum())
        if mismatch:
            findings.append(
                {
                    "severity": "error",
                    "code": f"{threshold_union_col}_mismatch",
                    "message": f"{threshold_union_col} mismatch in {mismatch} rows.",
                }
            )
    elif required.issubset(df.columns) and threshold_union_col is None:
        findings.append(
            {
                "severity": "error",
                "code": "pco2_threshold_0_24h_missing",
                "message": "Missing pco2 threshold union column (expected pco2_threshold_0_24h).",
            }
        )

    if {"pco2_threshold_0_24h", "qualifying_pco2_mmhg", "max_pco2_0_24h"}.issubset(df.columns):
        gas_positive = (
            pd.to_numeric(df["pco2_threshold_0_24h"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )
        qualifying_values = pd.to_numeric(df["qualifying_pco2_mmhg"], errors="coerce")
        max_values_24h = pd.to_numeric(df["max_pco2_0_24h"], errors="coerce")
        max_pco2_0_24h_lt_qualifying_n = int(
            (
                gas_positive
                & (
                    qualifying_values.isna()
                    | max_values_24h.isna()
                    | max_values_24h.lt(qualifying_values)
                )
            ).sum()
        )
        if max_pco2_0_24h_lt_qualifying_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "max_pco2_0_24h_below_qualifying",
                    "message": (
                        "For gas-positive rows, max_pco2_0_24h must be present and >= qualifying_pco2_mmhg. "
                        f"Violations: {max_pco2_0_24h_lt_qualifying_n}."
                    ),
                }
            )

    if {
        "pco2_threshold_0_24h",
        "dt_qualifying_hypercapnia_hours",
        "qualifying_pco2_mmhg",
        "max_pco2_0_6h",
    }.issubset(df.columns):
        gas_positive = (
            pd.to_numeric(df["pco2_threshold_0_24h"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )
        dt_hours = pd.to_numeric(df["dt_qualifying_hypercapnia_hours"], errors="coerce")
        qualifying_values = pd.to_numeric(df["qualifying_pco2_mmhg"], errors="coerce")
        max_values_6h = pd.to_numeric(df["max_pco2_0_6h"], errors="coerce")
        qualifies_6h_mask = gas_positive & dt_hours.notna() & dt_hours.le(6.0)
        max_pco2_0_6h_lt_qualifying_n = int(
            (
                qualifies_6h_mask
                & (
                    qualifying_values.isna()
                    | max_values_6h.isna()
                    | max_values_6h.lt(qualifying_values)
                )
            ).sum()
        )
        if max_pco2_0_6h_lt_qualifying_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "max_pco2_0_6h_below_qualifying_when_dt_le_6h",
                    "message": (
                        "For gas-positive rows with dt_qualifying_hypercapnia_hours <= 6, "
                        "max_pco2_0_6h must be present and >= qualifying_pco2_mmhg. "
                        f"Violations: {max_pco2_0_6h_lt_qualifying_n}."
                    ),
                }
            )

    required_source_diag_columns = {
        "gas_source_inference_primary_tier",
        "gas_source_hint_conflict_rate",
        "gas_source_resolved_rate",
    }
    missing_source_diag_columns = sorted(
        required_source_diag_columns.difference(df.columns)
    )
    if missing_source_diag_columns:
        findings.append(
            {
                "severity": "warning",
                "code": "missing_gas_source_diagnostic_columns_export",
                "message": (
                    "Missing gas-source diagnostic columns in export workbook (expected in QA artifacts): "
                    f"{missing_source_diag_columns}"
                ),
            }
        )

    if "first_gas_time" in df.columns:
        missing_anchor_columns = sorted(
            {
                "first_gas_anchor_has_pco2",
                "first_gas_anchor_source_validated",
                "first_gas_specimen_type",
                "first_gas_specimen_present",
                "first_gas_pco2_itemid",
                "first_gas_pco2_fluid",
                "co2_other_is_blood_asserted",
            }.difference(df.columns)
        )
        if missing_anchor_columns:
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_first_gas_anchor_columns",
                    "message": (
                        "first_gas_time is present but required anchor columns are missing: "
                        f"{missing_anchor_columns}"
                    ),
                }
            )
        else:
            first_gas_present = pd.to_datetime(
                df["first_gas_time"], errors="coerce"
            ).notna()
            anchor_has_pco2 = (
                df["first_gas_anchor_has_pco2"].fillna(False).astype(bool)
            )
            source_validated = (
                df["first_gas_anchor_source_validated"].fillna(False).astype(bool)
            )
            without_pco2_anchor_n = int((first_gas_present & ~anchor_has_pco2).sum())
            without_validated_source_n = int((first_gas_present & ~source_validated).sum())
            if without_pco2_anchor_n > COHORT_FIRST_GAS_ANCHOR_TOLERANCE:
                findings.append(
                    {
                        "severity": "error",
                        "code": "first_gas_anchor_missing_pco2",
                        "message": (
                            "first_gas_time contains rows without pCO2-qualified anchors: "
                            f"{without_pco2_anchor_n}"
                        ),
                    }
                )
            if without_validated_source_n > COHORT_FIRST_GAS_ANCHOR_TOLERANCE:
                findings.append(
                    {
                        "severity": "error",
                        "code": "first_gas_anchor_unvalidated_source",
                        "message": (
                            "first_gas_time contains rows without validated source anchors: "
                            f"{without_validated_source_n}"
                        ),
                    }
                )

    if "hypercap_timing_class" in df.columns:
        timing_class = (
            df["hypercap_timing_class"]
            .astype("string")
            .fillna("")
            .str.strip()
        )
        allowed_timing_classes = {
            "presenting_0_6h",
            "late_6_24h",
            "none_within_24h",
            "icd_only_or_no_qualifying_gas",
        }
        invalid_class_n = int(
            (~timing_class.isin(allowed_timing_classes)).sum()
        )
        if invalid_class_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "timing_class_invalid_values",
                    "message": (
                        "hypercap_timing_class contains unsupported values: "
                        f"{invalid_class_n} rows."
                    ),
                }
            )

    if "hospital_los_hours_model" in df.columns:
        hospital_los_numeric = pd.to_numeric(
            df["hospital_los_hours_model"], errors="coerce"
        )
        hospital_los_negative_model_n = int(
            (hospital_los_numeric.notna() & hospital_los_numeric.lt(0)).sum()
        )
        if hospital_los_negative_model_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "hospital_los_hours_model_negative",
                    "message": (
                        "hospital_los_hours_model contains negative non-null values: "
                        f"{hospital_los_negative_model_n} rows."
                    ),
                }
            )

    integrity_flag_columns = {
        "time_integrity_any",
        "hospital_los_negative_flag",
        "admittime_before_ed_intime_flag",
        "dischtime_before_admittime_flag",
    }
    missing_integrity_flags = sorted(integrity_flag_columns.difference(df.columns))
    if missing_integrity_flags:
        findings.append(
            {
                "severity": "warning",
                "code": "missing_time_integrity_flags",
                "message": (
                    "Missing time-integrity diagnostic flags: "
                    f"{missing_integrity_flags}"
                ),
            }
        )
    if "time_integrity_any" in df.columns:
        if "timing_usable_for_model" not in df.columns:
            findings.append(
                {
                    "severity": "warning",
                    "code": "missing_timing_usable_for_model",
                    "message": "time_integrity_any is present but timing_usable_for_model is missing.",
                }
            )
        else:
            time_integrity_any = (
                df["time_integrity_any"].fillna(False).astype(bool)
            )
            timing_usable = (
                pd.to_numeric(df["timing_usable_for_model"], errors="coerce")
                .fillna(0)
                .astype(int)
                .eq(1)
            )
            mismatch_n = int((timing_usable != (~time_integrity_any)).sum())
            if mismatch_n > 0:
                findings.append(
                    {
                        "severity": "error",
                        "code": "timing_usable_for_model_inconsistent",
                        "message": (
                            "timing_usable_for_model is inconsistent with time_integrity_any in "
                            f"{mismatch_n} rows."
                        ),
                    }
                )

    for model_col, raw_col, flag_col, code in (
        (
            "dt_first_imv_hours_model",
            "dt_first_imv_hours",
            "imv_time_outside_window_flag",
            "dt_first_imv_hours_model_invalid",
        ),
        (
            "dt_first_niv_hours_model",
            "dt_first_niv_hours",
            "niv_time_outside_window_flag",
            "dt_first_niv_hours_model_invalid",
        ),
    ):
        if raw_col in df.columns and (model_col not in df.columns or flag_col not in df.columns):
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_vent_timing_model_columns",
                    "message": (
                        f"{raw_col} is present but {model_col} or {flag_col} is missing."
                    ),
                }
            )
            continue
        if model_col not in df.columns:
            continue
        model_values = pd.to_numeric(df[model_col], errors="coerce")
        negative_n = int((model_values.notna() & model_values.lt(0)).sum())
        if model_col == "dt_first_imv_hours_model":
            dt_first_imv_model_negative_n = negative_n
        if model_col == "dt_first_niv_hours_model":
            dt_first_niv_model_negative_n = negative_n
        if negative_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": code,
                    "message": f"{model_col} contains negative non-null values: {negative_n} rows.",
                }
            )

    anthro_flag_map = {
        "bmi_closest_pre_ed_model": "bmi_outlier_flag",
        "height_closest_pre_ed_model": "height_outlier_flag",
        "weight_closest_pre_ed_model": "weight_outlier_flag",
    }
    for model_col, bounds in COHORT_ANTHRO_MODEL_BOUNDS.items():
        if model_col not in df.columns:
            continue
        lower_bound, upper_bound = bounds
        numeric = pd.to_numeric(df[model_col], errors="coerce")
        if model_col == "height_closest_pre_ed_model":
            lower_invalid = numeric.lt(lower_bound)
            bounds_label = f"[{lower_bound}, {upper_bound}]"
        else:
            lower_invalid = numeric.le(lower_bound)
            bounds_label = f"({lower_bound}, {upper_bound}]"
        invalid_n = int(
            (
                numeric.notna()
                & (lower_invalid | numeric.gt(upper_bound))
            ).sum()
        )
        anthro_model_invalid_counts[model_col] = invalid_n
        if invalid_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "anthro_model_out_of_bounds",
                    "message": (
                        f"{model_col} contains {invalid_n} non-null values outside "
                        f"{bounds_label}."
                    ),
                }
            )
        flag_col = anthro_flag_map.get(model_col)
        if flag_col and flag_col not in df.columns:
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_anthro_outlier_flag_column",
                    "message": (
                        f"{model_col} is present but required outlier flag column {flag_col} is missing."
                    ),
                }
            )

    for raw_column, required_clean_columns in COHORT_REQUIRED_ED_VITALS_CLEAN_COLUMNS.items():
        if raw_column not in df.columns:
            continue
        missing_clean_columns = [
            clean_column for clean_column in required_clean_columns if clean_column not in df.columns
        ]
        if missing_clean_columns:
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_ed_vitals_clean_columns",
                    "message": (
                        f"Raw ED vitals column '{raw_column}' is present but required cleaned columns are missing: "
                        f"{missing_clean_columns}"
                    ),
                }
            )

    for clean_column, (lower_bound, upper_bound) in COHORT_ED_VITALS_CLEAN_BOUNDS.items():
        if clean_column not in df.columns:
            continue
        numeric = pd.to_numeric(df[clean_column], errors="coerce")
        invalid_n = int((numeric.notna() & ((numeric < lower_bound) | (numeric > upper_bound))).sum())
        if invalid_n:
            findings.append(
                {
                    "severity": "error",
                    "code": "invalid_ed_vitals_clean_range",
                    "message": (
                        f"{clean_column} has {invalid_n} non-null values outside "
                        f"[{lower_bound}, {upper_bound}]."
                    ),
                }
            )
        if clean_column.endswith("_temp_f_clean"):
            celsius_band_n = int((numeric.notna() & numeric.between(20.0, 50.0)).sum())
            if celsius_band_n:
                findings.append(
                    {
                        "severity": "error",
                        "code": "ed_temp_clean_contains_celsius_band_values",
                        "message": (
                            f"{clean_column} has {celsius_band_n} non-null values in the 20-50 band after cleaning."
                        ),
                    }
                )

    gas_source_other_rate = None
    if any(
        column_name in df.columns
        for column_name in ("hadm_other_rate_0_24h", "gas_source_other_rate", "gas_source_unknown_rate")
    ):
        if "hadm_other_rate_0_24h" in df.columns:
            rate_col = "hadm_other_rate_0_24h"
        elif "gas_source_other_rate" in df.columns:
            rate_col = "gas_source_other_rate"
        else:
            rate_col = "gas_source_unknown_rate"
        gas_source_other_rate = float(
            pd.to_numeric(df[rate_col], errors="coerce").mean()
        )
        if (
            gas_source_other_fail_threshold is not None
            and gas_source_other_rate >= gas_source_other_fail_threshold
        ):
            findings.append(
                {
                    "severity": "error",
                    "code": "gas_source_other_rate_fail_threshold_exceeded",
                    "message": (
                        f"Mean {rate_col} exceeded fail threshold "
                        f"{gas_source_other_fail_threshold:.2f}: {gas_source_other_rate:.4f}"
                    ),
                }
            )
        if gas_source_other_rate >= gas_source_other_warn_threshold:
            findings.append(
                {
                    "severity": "warning",
                    "code": "gas_source_other_rate_high",
                    "message": (
                        f"Mean {rate_col} exceeded warning threshold "
                        f"{gas_source_other_warn_threshold:.2f}: {gas_source_other_rate:.4f}"
                    ),
                }
            )

    poc_other_pco2_median = None
    poc_other_pco2_count = 0
    first_other_poc_rows = 0
    if {"first_other_src", "first_other_pco2"}.issubset(df.columns):
        source = df["first_other_src"].astype("string").str.strip().str.upper()
        source_is_poc = source.str.contains("POC", na=False)
        first_other_poc_rows = int(source_is_poc.sum())
        values = pd.to_numeric(df["first_other_pco2"], errors="coerce")
        poc_values = values.loc[source_is_poc & values.notna()]
        poc_other_pco2_count = int(poc_values.shape[0])
        if poc_other_pco2_count > 0:
            poc_other_pco2_median = float(poc_values.median())
            if (
                COHORT_POC_PCO2_FAIL_ENABLED
                and (
                    poc_other_pco2_median < COHORT_POC_PCO2_MEDIAN_MIN
                    or poc_other_pco2_median > COHORT_POC_PCO2_MEDIAN_MAX
                )
            ):
                findings.append(
                    {
                        "severity": "error",
                        "code": "poc_unknown_pco2_median_out_of_bounds",
                        "message": (
                            "POC UNKNOWN first_other_pco2 median outside hard-coded QA bounds "
                            f"[{COHORT_POC_PCO2_MEDIAN_MIN:.1f}, {COHORT_POC_PCO2_MEDIAN_MAX:.1f}] "
                            f"with median={poc_other_pco2_median:.1f} (n={poc_other_pco2_count})."
                        ),
                    }
                )

    poc_other_quarantine_leak_n = 0
    if {"unknown_hypercap_threshold", "first_other_src_detail"}.issubset(df.columns):
        detail = df["first_other_src_detail"].astype("string").str.lower()
        other_flag = (
            pd.to_numeric(df["unknown_hypercap_threshold"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )
        poc_other_quarantine_leak_n = int(
            (other_flag & detail.isin({"poc_bg_unknown", "poc_bg_unknown_available"})).sum()
        )

    anthro_source_counts: dict[str, int] = {}
    if "anthro_source" in df.columns:
        source_series = (
            df["anthro_source"].astype("string").fillna("missing").replace({"": "missing"})
        )
        anthro_source_counts = {
            str(key): int(value)
            for key, value in source_series
            .astype(str)
            .value_counts(dropna=False)
            .items()
        }
        invalid_source_values = sorted(
            set(source_series.unique()).difference(COHORT_ANTHRO_ALLOWED_SOURCES)
        )
        if invalid_source_values:
            findings.append(
                {
                    "severity": "error",
                    "code": "anthro_source_invalid_values",
                    "message": (
                        "anthro_source contains unsupported values: "
                        f"{invalid_source_values}. Allowed={sorted(COHORT_ANTHRO_ALLOWED_SOURCES)}."
                    ),
                }
            )

    anthro_unit_time_contract_enabled = any(
        col_name in df.columns
        for pair in COHORT_ANTHRO_REQUIRED_UNIT_TIME_COLUMNS.values()
        for col_name in pair
    )
    for value_column, (unit_column, time_column) in COHORT_ANTHRO_REQUIRED_UNIT_TIME_COLUMNS.items():
        if value_column not in df.columns or not anthro_unit_time_contract_enabled:
            continue
        missing_required_cols = [
            col_name
            for col_name in (unit_column, time_column)
            if col_name not in df.columns
        ]
        if missing_required_cols:
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_anthro_unit_time_columns",
                    "message": (
                        f"{value_column} present but missing required columns: {missing_required_cols}."
                    ),
                }
            )
            continue

        numeric = pd.to_numeric(df[value_column], errors="coerce")
        unit_series = df[unit_column].astype("string").str.strip().str.lower()
        time_series = pd.to_datetime(df[time_column], errors="coerce")
        nonnull_value_mask = numeric.notna()
        expected_unit = COHORT_ANTHRO_CANONICAL_UOMS[unit_column]
        invalid_unit_n = int(
            (
                nonnull_value_mask
                & unit_series.notna()
                & unit_series.ne("")
                & unit_series.ne(expected_unit)
            ).sum()
        )
        missing_unit_n = int(
            (nonnull_value_mask & (unit_series.isna() | unit_series.eq(""))).sum()
        )
        missing_time_n = int((nonnull_value_mask & time_series.isna()).sum())
        if invalid_unit_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "anthro_noncanonical_unit",
                    "message": (
                        f"{unit_column} contains {invalid_unit_n} non-null values not equal to "
                        f"'{expected_unit}' for non-null {value_column} rows."
                    ),
                }
            )
        if missing_unit_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "anthro_missing_unit_for_value",
                    "message": (
                        f"{unit_column} missing for {missing_unit_n} non-null {value_column} rows."
                    ),
                }
            )
        if missing_time_n > 0:
            findings.append(
                {
                    "severity": "warning",
                    "code": "anthro_missing_time_for_value",
                    "message": (
                        f"{time_column} missing for {missing_time_n} non-null {value_column} rows."
                    ),
                }
            )

    duplicate_alias_columns = [
        col_name
        for col_name in (
            "bmi_closest_pre_ed_unit",
            "height_closest_pre_ed_unit",
            "weight_closest_pre_ed_unit",
            "bmi_closest_pre_ed_datetime",
            "height_closest_pre_ed_datetime",
            "weight_closest_pre_ed_datetime",
        )
        if col_name in df.columns
    ]
    if duplicate_alias_columns:
        findings.append(
            {
                "severity": "error",
                "code": "anthro_duplicate_alias_columns_present",
                "message": (
                    "Duplicate anthropometric alias columns detected in export: "
                    f"{duplicate_alias_columns}."
                ),
            }
        )

    bmi_coverage_rate = None
    if "bmi_closest_pre_ed" in df.columns:
        bmi_coverage_rate = float(
            pd.to_numeric(df["bmi_closest_pre_ed"], errors="coerce").notna().mean()
        )
        if bmi_coverage_rate < min_bmi_coverage:
            findings.append(
                {
                    "severity": "error",
                    "code": "anthro_bmi_coverage_below_minimum",
                    "message": (
                        "BMI coverage fell below minimum threshold "
                        f"{min_bmi_coverage:.2f}: {bmi_coverage_rate:.4f}"
                    ),
                }
            )

    hco3_coverage_rate = None
    hco3_gas_positive_coverage_rate = None
    hco3_icd_only_coverage_rate = None
    hco3_source_value_mismatch_n = 0
    if "first_hco3" in df.columns:
        hco3_coverage_rate = float(
            pd.to_numeric(df["first_hco3"], errors="coerce").notna().mean()
        )
    if {"first_hco3", "pco2_threshold_0_24h"}.issubset(df.columns):
        hco3_values = pd.to_numeric(df["first_hco3"], errors="coerce")
        gas_positive_mask = (
            pd.to_numeric(df["pco2_threshold_0_24h"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )
        gas_positive_n = int(gas_positive_mask.sum())
        if gas_positive_n > 0:
            hco3_gas_positive_coverage_rate = float(
                hco3_values.loc[gas_positive_mask].notna().mean()
            )
            if hco3_gas_positive_coverage_rate < COHORT_HCO3_GAS_FAIL_MIN_COVERAGE:
                findings.append(
                    {
                        "severity": "error",
                        "code": "first_hco3_coverage_low",
                        "message": (
                            "first_hco3 gas-positive coverage below fail floor "
                            f"{COHORT_HCO3_GAS_FAIL_MIN_COVERAGE:.2f}: "
                            f"{hco3_gas_positive_coverage_rate:.4f}"
                        ),
                    }
                )
            elif hco3_gas_positive_coverage_rate < COHORT_HCO3_GAS_WARN_MIN_COVERAGE:
                findings.append(
                    {
                        "severity": "warning",
                        "code": "first_hco3_coverage_low",
                        "message": (
                            "first_hco3 gas-positive coverage below warning floor "
                            f"{COHORT_HCO3_GAS_WARN_MIN_COVERAGE:.2f}: "
                            f"{hco3_gas_positive_coverage_rate:.4f}"
                        ),
                    }
                )

    if {"first_hco3", "enrollment_route"}.issubset(df.columns):
        hco3_values = pd.to_numeric(df["first_hco3"], errors="coerce")
        icd_only_mask = (
            df["enrollment_route"]
            .astype("string")
            .fillna("NONE")
            .str.upper()
            .eq("ICD_ONLY")
        )
        icd_only_n = int(icd_only_mask.sum())
        if icd_only_n > 0:
            hco3_icd_only_coverage_rate = float(
                hco3_values.loc[icd_only_mask].notna().mean()
            )
            if hco3_icd_only_coverage_rate < COHORT_HCO3_ICD_ONLY_WARN_MIN_COVERAGE:
                findings.append(
                    {
                        "severity": "warning",
                        "code": "first_hco3_coverage_low",
                        "message": (
                            "first_hco3 ICD-only coverage below warning floor "
                            f"{COHORT_HCO3_ICD_ONLY_WARN_MIN_COVERAGE:.2f}: "
                            f"{hco3_icd_only_coverage_rate:.4f}"
                        ),
                    }
                )
    elif hco3_coverage_rate is not None and hco3_coverage_rate < COHORT_HCO3_GAS_WARN_MIN_COVERAGE:
        # Backward-compatibility fallback when route/threshold flags are unavailable.
        findings.append(
            {
                "severity": "warning",
                "code": "first_hco3_coverage_low",
                "message": (
                    "first_hco3 overall coverage below fallback warning floor "
                    f"{COHORT_HCO3_GAS_WARN_MIN_COVERAGE:.2f}: {hco3_coverage_rate:.4f}"
                ),
            }
        )
    if {"first_hco3", "first_hco3_source"}.issubset(df.columns):
        hco3_values = pd.to_numeric(df["first_hco3"], errors="coerce")
        hco3_source = df["first_hco3_source"].astype("string").fillna("missing")
        hco3_source_value_mismatch_n = int(
            ((hco3_source != "missing") & hco3_values.isna()).sum()
        )
        if hco3_source_value_mismatch_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "first_hco3_source_value_mismatch",
                    "message": (
                        "first_hco3_source implies a value but first_hco3 is null in "
                        f"{hco3_source_value_mismatch_n} rows."
                    ),
                }
            )

    ph_uom_pairs = (
        ("lab_abg_ph", "lab_abg_ph_uom"),
        ("lab_vbg_ph", "lab_vbg_ph_uom"),
        ("lab_other_ph", "lab_other_ph_uom"),
        ("poc_abg_ph", "poc_abg_ph_uom"),
        ("poc_vbg_ph", "poc_vbg_ph_uom"),
        ("poc_other_ph", "poc_other_ph_uom"),
    )
    for ph_column, ph_uom_column in ph_uom_pairs:
        if ph_column not in df.columns or ph_uom_column not in df.columns:
            continue
        ph_values = pd.to_numeric(df[ph_column], errors="coerce")
        ph_present = ph_values.notna()
        if not ph_present.any():
            continue
        uom_series = (
            df[ph_uom_column]
            .astype("string")
            .str.strip()
            .str.lower()
        )
        non_unitless_n = int(
            (ph_present & (uom_series.isna() | uom_series.ne("unitless"))).sum()
        )
        if non_unitless_n > 0:
            findings.append(
                {
                    "severity": "warning",
                    "code": "ph_uom_not_unitless",
                    "message": (
                        f"{ph_uom_column} has {non_unitless_n} non-unitless values "
                        f"for non-null {ph_column} rows."
                    ),
                }
            )

    for site_name in ("abg", "vbg", "other"):
        pco2_column = f"first_{site_name}_pco2"
        po2_column = f"first_{site_name}_po2"
        if pco2_column not in df.columns or po2_column not in df.columns:
            continue
        pco2_present = pd.to_numeric(df[pco2_column], errors="coerce").notna()
        pco2_n = int(pco2_present.sum())
        if pco2_n == 0:
            po2_triplet_coverage_by_site[site_name] = 1.0
            continue
        po2_present_given_pco2 = pd.to_numeric(df[po2_column], errors="coerce").notna() & pco2_present
        coverage = float(po2_present_given_pco2.sum() / pco2_n)
        po2_triplet_coverage_by_site[site_name] = coverage
        if coverage < COHORT_PO2_TRIPLET_WARN_MIN_COVERAGE:
            findings.append(
                {
                    "severity": "warning",
                    "code": "po2_triplet_coverage_low",
                    "message": (
                        f"{po2_column} coverage among non-null {pco2_column} rows is "
                        f"{coverage:.4f}, below warning floor {COHORT_PO2_TRIPLET_WARN_MIN_COVERAGE:.2f}."
                    ),
                }
            )

    status = _status_from_findings(findings)
    return {
        "status": status,
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "gas_source_other_rate_mean": gas_source_other_rate,
        "poc_other_pco2_median": poc_other_pco2_median,
        "poc_other_pco2_count": poc_other_pco2_count,
        "first_other_poc_rows": first_other_poc_rows,
        "poc_other_quarantine_leak_n": poc_other_quarantine_leak_n,
        "hospital_los_hours_model_negative_n": hospital_los_negative_model_n,
        "dt_first_imv_hours_model_negative_n": dt_first_imv_model_negative_n,
        "dt_first_niv_hours_model_negative_n": dt_first_niv_model_negative_n,
        "max_pco2_0_24h_lt_qualifying_n": max_pco2_0_24h_lt_qualifying_n,
        "max_pco2_0_6h_lt_qualifying_n": max_pco2_0_6h_lt_qualifying_n,
        "anthro_bmi_coverage_rate": bmi_coverage_rate,
        "anthro_model_invalid_counts": anthro_model_invalid_counts,
        "first_hco3_coverage_rate": hco3_coverage_rate,
        "first_hco3_gas_positive_coverage_rate": hco3_gas_positive_coverage_rate,
        "first_hco3_icd_only_coverage_rate": hco3_icd_only_coverage_rate,
        "first_hco3_source_value_mismatch_n": hco3_source_value_mismatch_n,
        "po2_triplet_coverage_by_site": po2_triplet_coverage_by_site,
        "anthro_source_counts": anthro_source_counts,
        "findings": findings,
    }


def build_pipeline_contract_report(work_dir: Path) -> dict[str, Any]:
    """Build contract report from canonical cohort/classifier outputs if present."""
    return build_pipeline_contract_report_for_stages(work_dir, stages=("cohort", "classifier"))


def build_pipeline_contract_report_for_stages(
    work_dir: Path,
    *,
    stages: tuple[str, ...],
) -> dict[str, Any]:
    """Build contract report from requested stage outputs."""
    requested = {stage.strip().lower() for stage in stages}
    valid = {"cohort", "classifier"}
    unsupported = sorted(requested.difference(valid))
    if unsupported:
        raise ValueError(f"Unsupported contract stages: {unsupported}")

    data_dir = (work_dir / DATA_DIRNAME).expanduser().resolve()
    cohort_path = data_dir / CANONICAL_COHORT_FILENAME
    classifier_path = data_dir / CANONICAL_NLP_FILENAME
    prior_runs_dir = data_dir / PRIOR_RUNS_DIRNAME

    findings: list[dict[str, str]] = []
    contracts: dict[str, Any] = {}

    if "cohort" in requested:
        if cohort_path.exists():
            cohort_df = pd.read_excel(cohort_path, engine="openpyxl")
            warn_threshold = float(os.getenv("COHORT_WARN_OTHER_RATE", "0.50"))
            fail_threshold_raw = os.getenv("COHORT_FAIL_OTHER_RATE", "").strip()
            fail_threshold = (
                float(fail_threshold_raw) if fail_threshold_raw else None
            )
            min_bmi_coverage = float(
                os.getenv("COHORT_ANTHRO_MIN_BMI_COVERAGE", "0.30")
            )
            cohort_report = validate_cohort_contract(
                cohort_df,
                gas_source_other_warn_threshold=warn_threshold,
                gas_source_other_fail_threshold=fail_threshold,
                min_bmi_coverage=min_bmi_coverage,
            )
            audit_artifacts: dict[str, str | None] = {}
            for suffix in COHORT_REQUIRED_AUDIT_SUFFIXES:
                candidates = (
                    list(prior_runs_dir.glob(f"* {suffix}"))
                    if prior_runs_dir.exists()
                    else []
                )
                latest_path = (
                    str(max(candidates, key=lambda path: path.stat().st_mtime))
                    if candidates
                    else None
                )
                audit_artifacts[suffix] = latest_path
                if latest_path is None:
                    finding = {
                        "severity": "error",
                        "code": "missing_cohort_audit_artifact",
                        "message": (
                            "Missing required cohort audit artifact matching "
                            f"'* {suffix}' in {prior_runs_dir}."
                        ),
                    }
                    cohort_report.setdefault("findings", []).append(finding)
                else:
                    latest_audit_path = Path(latest_path)
                    try:
                        if latest_audit_path.suffix.lower() == ".csv":
                            pd.read_csv(latest_audit_path, nrows=5)
                        elif latest_audit_path.suffix.lower() == ".json":
                            json.loads(latest_audit_path.read_text())
                    except Exception as exc:
                        cohort_report.setdefault("findings", []).append(
                            {
                                "severity": "error",
                                "code": "invalid_cohort_audit_artifact",
                                "message": (
                                    "Failed to parse required cohort audit artifact "
                                    f"{latest_audit_path}: {exc}"
                                ),
                            }
                        )
            cohort_report["audit_artifacts"] = audit_artifacts
            contracts["cohort"] = cohort_report
            findings.extend(cohort_report["findings"])
        else:
            contracts["cohort"] = {"status": "missing", "findings": []}
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_cohort_artifact",
                    "message": f"Missing cohort artifact: {cohort_path}",
                }
            )
    else:
        contracts["cohort"] = {"status": "skipped", "findings": []}

    if "classifier" in requested:
        if classifier_path.exists():
            classifier_df = pd.read_excel(classifier_path, engine="openpyxl")
            classifier_report = validate_classifier_contract(classifier_df)
            latest_cc_audit = None
            if prior_runs_dir.exists():
                candidates = list(prior_runs_dir.glob("* classifier_cc_missing_audit.csv"))
                if candidates:
                    latest_cc_audit = max(candidates, key=lambda path: path.stat().st_mtime)
            classifier_report["cc_missing_audit_path"] = (
                str(latest_cc_audit) if latest_cc_audit else None
            )
            if latest_cc_audit is None:
                classifier_report["status"] = "fail"
                classifier_report["error_count"] = int(
                    classifier_report.get("error_count", 0)
                ) + 1
                classifier_report.setdefault("findings", []).append(
                    {
                        "severity": "error",
                        "code": "missing_classifier_cc_missing_audit",
                        "message": (
                            "Missing classifier CC missingness audit artifact in "
                            f"{prior_runs_dir}."
                        ),
                    }
                )
            contracts["classifier"] = classifier_report
            findings.extend(classifier_report["findings"])
        else:
            contracts["classifier"] = {"status": "missing", "findings": []}
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_classifier_artifact",
                    "message": f"Missing classifier artifact: {classifier_path}",
                }
            )
    else:
        contracts["classifier"] = {"status": "skipped", "findings": []}

    overall_status = _status_from_findings(findings)
    return {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": overall_status,
        "contracts": contracts,
        "findings": findings,
        "paths": {
            "cohort": str(cohort_path),
            "classifier": str(classifier_path),
        },
        "stages": sorted(requested),
    }


def write_contract_report(
    report: dict[str, Any],
    *,
    work_dir: Path,
) -> dict[str, Path]:
    """Write contract report and optional FAILED_CONTRACT artifact paths."""
    run_id = _utc_timestamp()
    out_dir = (work_dir / "debug" / "contracts" / run_id).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "contract_report.json"
    report_path.write_text(json.dumps(report, indent=2))

    failed_path = out_dir / "FAILED_CONTRACT.json"
    if report["status"] == "fail":
        failed_path.write_text(json.dumps(report, indent=2))

    return {
        "out_dir": out_dir,
        "contract_report_path": report_path,
        "failed_contract_path": failed_path,
    }
