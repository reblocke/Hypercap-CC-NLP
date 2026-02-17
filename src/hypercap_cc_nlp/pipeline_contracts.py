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
    if {"first_other_src", "first_other_pco2"}.issubset(df.columns):
        source = df["first_other_src"].astype("string").str.strip().str.upper()
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

    status = _status_from_findings(findings)
    return {
        "status": status,
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "gas_source_other_rate_mean": gas_source_other_rate,
        "poc_other_pco2_median": poc_other_pco2_median,
        "poc_other_pco2_count": poc_other_pco2_count,
        "anthro_bmi_coverage_rate": bmi_coverage_rate,
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
            prior_runs_dir = data_dir / PRIOR_RUNS_DIRNAME
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
