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
COHORT_HCO3_WARN_MIN_COVERAGE = 0.20
COHORT_FIRST_GAS_ANCHOR_TOLERANCE = 0
COHORT_REQUIRED_AUDIT_SUFFIXES = (
    "blood_gas_itemid_manifest_audit.csv",
    "pco2_itemid_qc_audit.csv",
    "pco2_source_distribution_audit.csv",
    "other_route_quarantine_audit.csv",
    "first_gas_anchor_audit.csv",
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
    "bmi_closest_pre_ed_model": (0.0, 100.0),
    "height_closest_pre_ed_model": (100.0, 250.0),
    "weight_closest_pre_ed_model": (0.0, 400.0),
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
    timing_tri_state_mismatch_n = 0
    timing_tri_state_invalid_value_n = 0
    hospital_los_negative_model_n = 0
    dt_first_imv_model_negative_n = 0
    dt_first_niv_model_negative_n = 0
    anthro_model_invalid_counts: dict[str, int] = {}

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
        "other_hypercap_threshold",
        "pco2_threshold_any",
    }
    if required.issubset(df.columns):
        abg = pd.to_numeric(df["abg_hypercap_threshold"], errors="coerce").fillna(0).astype(int)
        vbg = pd.to_numeric(df["vbg_hypercap_threshold"], errors="coerce").fillna(0).astype(int)
        other = pd.to_numeric(df["other_hypercap_threshold"], errors="coerce").fillna(0).astype(int)
        any_reported = pd.to_numeric(df["pco2_threshold_any"], errors="coerce").fillna(0).astype(int)
        any_expected = (abg | vbg | other).astype(int)
        mismatch = int((any_expected != any_reported).sum())
        if mismatch:
            findings.append(
                {
                    "severity": "error",
                    "code": "pco2_threshold_any_mismatch",
                    "message": f"pco2_threshold_any mismatch in {mismatch} rows.",
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
                "severity": "error",
                "code": "missing_gas_source_diagnostic_columns",
                "message": (
                    "Missing gas-source diagnostic columns: "
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

    tri_state_required = {
        "qualifying_gas_observed",
        "presenting_hypercapnia_tri",
        "late_hypercapnia_tri",
        "hypercap_timing_class",
    }
    if "dt_first_qualifying_gas_hours" in df.columns:
        missing_timing_cols = sorted(tri_state_required.difference(df.columns))
        if missing_timing_cols:
            findings.append(
                {
                    "severity": "error",
                    "code": "missing_timing_tri_state_columns",
                    "message": (
                        "Timing tri-state columns missing despite dt_first_qualifying_gas_hours presence: "
                        f"{missing_timing_cols}"
                    ),
                }
            )
    if tri_state_required.issubset(df.columns):
        qualifying_observed = (
            pd.to_numeric(df["qualifying_gas_observed"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )
        presenting_tri = pd.to_numeric(
            df["presenting_hypercapnia_tri"], errors="coerce"
        )
        late_tri = pd.to_numeric(df["late_hypercapnia_tri"], errors="coerce")
        invalid_presenting = int(
            (presenting_tri.notna() & ~presenting_tri.isin([0, 1])).sum()
        )
        invalid_late = int((late_tri.notna() & ~late_tri.isin([0, 1])).sum())
        timing_tri_state_invalid_value_n = invalid_presenting + invalid_late
        if timing_tri_state_invalid_value_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "timing_tri_state_invalid_values",
                    "message": (
                        "Timing tri-state columns contain values outside {0,1,NA}: "
                        f"{timing_tri_state_invalid_value_n} rows."
                    ),
                }
            )

        observed_missing_tri = int(
            (qualifying_observed & (presenting_tri.isna() | late_tri.isna())).sum()
        )
        unobserved_with_tri = int(
            ((~qualifying_observed) & (presenting_tri.notna() | late_tri.notna())).sum()
        )
        timing_tri_state_mismatch_n = observed_missing_tri + unobserved_with_tri
        if timing_tri_state_mismatch_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "timing_tri_state_inconsistent_with_observed_flag",
                    "message": (
                        "Tri-state timing fields are inconsistent with qualifying_gas_observed: "
                        f"observed_missing={observed_missing_tri}, unobserved_with_values={unobserved_with_tri}."
                    ),
                }
            )

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

    if "poc_inclusion_enabled" in df.columns:
        inclusion_values = (
            df["poc_inclusion_enabled"].fillna(False).astype(bool).value_counts()
        )
        if len(inclusion_values) > 1:
            findings.append(
                {
                    "severity": "warning",
                    "code": "poc_inclusion_enabled_not_constant",
                    "message": (
                        "poc_inclusion_enabled varies within a single cohort export; "
                        f"counts={inclusion_values.to_dict()}."
                    ),
                }
            )
    elif any(
        column_name in df.columns
        for column_name in ("poc_other_paco2", "poc_other_quarantined_hypercap_threshold")
    ):
        findings.append(
            {
                "severity": "error",
                "code": "missing_poc_inclusion_status_columns",
                "message": (
                    "POC-derived columns are present but poc_inclusion_enabled is missing."
                ),
            }
        )

    if "poc_inclusion_enabled" in df.columns and "poc_inclusion_reason" not in df.columns:
        findings.append(
            {
                "severity": "warning",
                "code": "missing_poc_inclusion_reason",
                "message": "Missing poc_inclusion_reason column in cohort export.",
            }
        )

    gas_source_other_rate = None
    if "gas_source_other_rate" in df.columns:
        gas_source_other_rate = float(
            pd.to_numeric(df["gas_source_other_rate"], errors="coerce").mean()
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
                        "Mean gas_source_other_rate exceeded fail threshold "
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
                        "Mean gas_source_other_rate exceeded warning threshold "
                        f"{gas_source_other_warn_threshold:.2f}: {gas_source_other_rate:.4f}"
                    ),
                }
            )

    poc_other_pco2_median = None
    poc_other_pco2_count = 0
    first_other_poc_rows = 0
    if {"first_other_src", "first_other_pco2"}.issubset(df.columns):
        source = df["first_other_src"].astype("string").str.strip().str.upper()
        first_other_poc_rows = int(source.eq("POC").sum())
        if first_other_poc_rows > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "first_other_src_contains_poc",
                    "message": (
                        "first_other_src contains POC rows under LAB-only OTHER policy: "
                        f"{first_other_poc_rows} rows."
                    ),
                }
            )
        values = pd.to_numeric(df["first_other_pco2"], errors="coerce")
        poc_values = values.loc[source.eq("POC") & values.notna()]
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
                        "code": "poc_other_pco2_median_out_of_bounds",
                        "message": (
                            "POC first_other_pco2 median outside hard-coded QA bounds "
                            f"[{COHORT_POC_PCO2_MEDIAN_MIN:.1f}, {COHORT_POC_PCO2_MEDIAN_MAX:.1f}] "
                            f"with median={poc_other_pco2_median:.1f} (n={poc_other_pco2_count})."
                        ),
                    }
                )

    poc_other_quarantine_leak_n = 0
    if {"other_hypercap_threshold", "first_other_src_detail"}.issubset(df.columns):
        detail = df["first_other_src_detail"].astype("string").str.lower()
        other_flag = (
            pd.to_numeric(df["other_hypercap_threshold"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )
        poc_other_quarantine_leak_n = int((other_flag & detail.eq("poc_quarantined")).sum())
        if poc_other_quarantine_leak_n > 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "poc_other_quarantine_leakage",
                    "message": (
                        "POC quarantined OTHER rows leaked into other_hypercap_threshold: "
                        f"{poc_other_quarantine_leak_n} rows."
                    ),
                }
            )

    anthro_source_counts: dict[str, int] = {}
    if "anthro_source" in df.columns:
        anthro_source_counts = {
            str(key): int(value)
            for key, value in df["anthro_source"]
            .fillna("missing")
            .astype(str)
            .value_counts(dropna=False)
            .items()
        }
        if anthro_source_counts.get("omr", 0) == 0 and anthro_source_counts.get(
            "icu_charted", 0
        ) == 0:
            findings.append(
                {
                    "severity": "error",
                    "code": "anthro_no_supported_sources",
                    "message": "anthro_source has no OMR or ICU charted rows.",
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
    if "first_hco3" in df.columns:
        hco3_coverage_rate = float(
            pd.to_numeric(df["first_hco3"], errors="coerce").notna().mean()
        )
        if hco3_coverage_rate < COHORT_HCO3_WARN_MIN_COVERAGE:
            findings.append(
                {
                    "severity": "warning",
                    "code": "first_hco3_coverage_low",
                    "message": (
                        "first_hco3 coverage below warning floor "
                        f"{COHORT_HCO3_WARN_MIN_COVERAGE:.2f}: {hco3_coverage_rate:.4f}"
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
        "timing_tri_state_mismatch_n": timing_tri_state_mismatch_n,
        "timing_tri_state_invalid_value_n": timing_tri_state_invalid_value_n,
        "hospital_los_hours_model_negative_n": hospital_los_negative_model_n,
        "dt_first_imv_hours_model_negative_n": dt_first_imv_model_negative_n,
        "dt_first_niv_hours_model_negative_n": dt_first_niv_model_negative_n,
        "anthro_bmi_coverage_rate": bmi_coverage_rate,
        "anthro_model_invalid_counts": anthro_model_invalid_counts,
        "first_hco3_coverage_rate": hco3_coverage_rate,
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
