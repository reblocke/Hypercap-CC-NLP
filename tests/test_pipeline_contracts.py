from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hypercap_cc_nlp.pipeline_contracts import (
    COHORT_POC_PCO2_MEDIAN_MAX,
    COHORT_POC_PCO2_MEDIAN_MIN,
    build_pipeline_contract_report,
    validate_cohort_contract,
    write_contract_report,
)
from hypercap_cc_nlp.workflow_contracts import (
    CANONICAL_COHORT_FILENAME,
    CANONICAL_NLP_FILENAME,
    DATA_DIRNAME,
    PRIOR_RUNS_DIRNAME,
)


def test_validate_cohort_contract_detects_threshold_mismatch() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "ed_stay_id": [11, 22],
            "abg_hypercap_threshold": [1, 0],
            "vbg_hypercap_threshold": [0, 0],
            "other_hypercap_threshold": [0, 1],
            "pco2_threshold_any": [1, 0],
            "gas_source_other_rate": [0.9, 0.8],
            "gas_source_inference_primary_tier": ["fallback_other", "fallback_other"],
            "gas_source_hint_conflict_rate": [0.0, 0.0],
            "gas_source_resolved_rate": [0.0, 0.0],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}

    assert report["status"] == "fail"
    assert "pco2_threshold_any_mismatch" in codes
    assert "gas_source_other_rate_high" in codes


def test_validate_cohort_contract_requires_source_diagnostic_columns() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "other_hypercap_threshold": [0],
            "pco2_threshold_any": [1],
            "gas_source_other_rate": [0.1],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["omr"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "missing_gas_source_diagnostic_columns" in codes


def test_validate_cohort_contract_applies_other_fail_threshold() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [0],
            "vbg_hypercap_threshold": [1],
            "other_hypercap_threshold": [1],
            "pco2_threshold_any": [1],
            "gas_source_other_rate": [0.85],
            "gas_source_inference_primary_tier": ["label_fluid"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [32.0],
            "anthro_source": ["icu_charted"],
        }
    )
    report = validate_cohort_contract(
        df,
        gas_source_other_warn_threshold=0.50,
        gas_source_other_fail_threshold=0.80,
    )
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "gas_source_other_rate_fail_threshold_exceeded" in codes


def test_validate_cohort_contract_enforces_min_bmi_coverage() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "ed_stay_id": [11, 22, 33],
            "abg_hypercap_threshold": [0, 0, 0],
            "vbg_hypercap_threshold": [1, 1, 1],
            "other_hypercap_threshold": [0, 0, 0],
            "pco2_threshold_any": [1, 1, 1],
            "gas_source_other_rate": [0.2, 0.2, 0.2],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, pd.NA, pd.NA],
            "anthro_source": ["omr", "missing", "missing"],
        }
    )
    report = validate_cohort_contract(df, min_bmi_coverage=0.60)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "anthro_bmi_coverage_below_minimum" in codes


def test_validate_cohort_contract_fails_when_poc_other_pco2_median_out_of_bounds() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "ed_stay_id": [11, 22, 33],
            "abg_hypercap_threshold": [0, 0, 0],
            "vbg_hypercap_threshold": [1, 1, 1],
            "other_hypercap_threshold": [1, 1, 1],
            "pco2_threshold_any": [1, 1, 1],
            "gas_source_other_rate": [0.2, 0.2, 0.2],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 32.0, 34.0],
            "anthro_source": ["omr", "icu_charted", "omr"],
            "first_other_src": ["POC", "POC", "LAB"],
            "first_other_pco2": [110.0, 100.0, 40.0],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}

    assert report["status"] == "fail"
    assert "poc_other_pco2_median_out_of_bounds" in codes
    assert report["poc_other_pco2_median"] > COHORT_POC_PCO2_MEDIAN_MAX


def test_validate_cohort_contract_accepts_poc_other_pco2_median_within_bounds() -> None:
    midpoint = (COHORT_POC_PCO2_MEDIAN_MIN + COHORT_POC_PCO2_MEDIAN_MAX) / 2
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "ed_stay_id": [11, 22, 33],
            "abg_hypercap_threshold": [1, 0, 1],
            "vbg_hypercap_threshold": [0, 1, 0],
            "other_hypercap_threshold": [1, 1, 0],
            "pco2_threshold_any": [1, 1, 1],
            "gas_source_other_rate": [0.2, 0.2, 0.2],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 32.0, 34.0],
            "anthro_source": ["omr", "icu_charted", "omr"],
            "first_other_src": ["POC", "POC", "LAB"],
            "first_other_pco2": [midpoint, midpoint + 2, midpoint - 3],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}

    assert "poc_other_pco2_median_out_of_bounds" not in codes
    assert report["poc_other_pco2_count"] == 2


def test_build_pipeline_contract_report_reads_canonical_outputs(tmp_path: Path) -> None:
    data_dir = tmp_path / DATA_DIRNAME
    data_dir.mkdir(parents=True)

    cohort = pd.DataFrame(
        {
            "hadm_id": [1],
            "subject_id": [10],
            "ed_stay_id": [100],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "other_hypercap_threshold": [0],
            "pco2_threshold_any": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
        }
    )
    cohort.to_excel(data_dir / CANONICAL_COHORT_FILENAME, index=False)

    classifier = pd.DataFrame(
        {
            "hadm_id": [1],
            "subject_id": [10],
            "RFV1_name": ["Symptom – Respiratory"],
            "segment_preds": ["[]"],
            "cc_missing_reason": ["valid"],
            "cc_pseudomissing_flag": [False],
            "cc_missing_flag": [False],
            "hypercap_by_abg": [1],
            "hypercap_by_vbg": [0],
            "hypercap_by_bg": [1],
        }
    )
    classifier.to_excel(data_dir / CANONICAL_NLP_FILENAME, index=False)
    prior_runs_dir = data_dir / PRIOR_RUNS_DIRNAME
    prior_runs_dir.mkdir(parents=True)
    (prior_runs_dir / "2026-02-16 classifier_cc_missing_audit.csv").write_text(
        "cc_missing_reason,row_count,examples\nvalid,1,shortness of breath\n"
    )

    report = build_pipeline_contract_report(tmp_path)
    assert report["status"] == "pass"
    assert report["contracts"]["cohort"]["status"] == "pass"
    assert report["contracts"]["classifier"]["status"] == "pass"


def test_write_contract_report_writes_failed_contract_when_failing(tmp_path: Path) -> None:
    report = {
        "generated_utc": "2026-01-01T00:00:00Z",
        "status": "fail",
        "contracts": {},
        "findings": [{"severity": "error", "code": "test", "message": "boom"}],
    }
    paths = write_contract_report(report, work_dir=tmp_path)

    assert paths["contract_report_path"].exists()
    assert paths["failed_contract_path"].exists()
    payload = json.loads(paths["failed_contract_path"].read_text())
    assert payload["status"] == "fail"


def test_build_pipeline_contract_report_fails_when_cc_missing_audit_absent(tmp_path: Path) -> None:
    data_dir = tmp_path / DATA_DIRNAME
    data_dir.mkdir(parents=True)
    cohort = pd.DataFrame(
        {
            "hadm_id": [1],
            "subject_id": [10],
            "ed_stay_id": [100],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "other_hypercap_threshold": [0],
            "pco2_threshold_any": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["icu_charted"],
        }
    )
    cohort.to_excel(data_dir / CANONICAL_COHORT_FILENAME, index=False)
    classifier = pd.DataFrame(
        {
            "hadm_id": [1],
            "subject_id": [10],
            "RFV1_name": ["Symptom – Respiratory"],
            "segment_preds": ["[]"],
            "cc_missing_reason": ["valid"],
            "cc_pseudomissing_flag": [False],
            "cc_missing_flag": [False],
            "hypercap_by_abg": [1],
            "hypercap_by_vbg": [0],
            "hypercap_by_bg": [1],
        }
    )
    classifier.to_excel(data_dir / CANONICAL_NLP_FILENAME, index=False)

    report = build_pipeline_contract_report(tmp_path)
    assert report["status"] == "fail"
    classifier_codes = {
        finding["code"] for finding in report["contracts"]["classifier"]["findings"]
    }
    assert "missing_classifier_cc_missing_audit" in classifier_codes


def test_validate_cohort_contract_requires_cleaned_vitals_when_raw_present() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "other_hypercap_threshold": [0],
            "pco2_threshold_any": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["omr"],
            "ed_triage_temp": [98.6],
        }
    )

    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "missing_ed_vitals_clean_columns" in codes


def test_validate_cohort_contract_flags_invalid_cleaned_vitals_ranges() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "other_hypercap_threshold": [0],
            "pco2_threshold_any": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["omr"],
            "ed_triage_o2sat": [99.0],
            "ed_triage_o2sat_clean": [101.0],
            "ed_triage_o2sat_gt_100": [True],
            "ed_triage_o2sat_out_of_range": [False],
            "ed_triage_o2sat_zero": [False],
            "ed_triage_temp": [98.6],
            "ed_triage_temp_f_clean": [35.0],
            "ed_triage_temp_was_celsius_like": [False],
            "ed_triage_temp_out_of_range": [False],
        }
    )

    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "invalid_ed_vitals_clean_range" in codes
    assert "ed_temp_clean_contains_celsius_band_values" in codes
