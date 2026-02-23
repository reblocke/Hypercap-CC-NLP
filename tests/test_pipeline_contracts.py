from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hypercap_cc_nlp.pipeline_contracts import (
    COHORT_REQUIRED_AUDIT_SUFFIXES,
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
            "unknown_hypercap_threshold": [0, 1],
            "pco2_threshold_0_24h": [1, 0],
            "gas_source_other_rate": [0.9, 0.8],
            "gas_source_inference_primary_tier": ["fallback_other", "fallback_other"],
            "gas_source_hint_conflict_rate": [0.0, 0.0],
            "gas_source_resolved_rate": [0.0, 0.0],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}

    assert report["status"] == "fail"
    assert "pco2_threshold_0_24h_mismatch" in codes
    assert "gas_source_other_rate_high" in codes


def test_validate_cohort_contract_warns_on_deprecated_threshold_alias() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_any": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [29.0],
            "anthro_source": ["ED"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "warning"
    assert "pco2_threshold_any_deprecated_alias" in codes


def test_validate_cohort_contract_warns_when_source_diagnostic_columns_absent() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["HOSPITAL"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "warning"
    assert "missing_gas_source_diagnostic_columns_export" in codes


def test_validate_cohort_contract_applies_other_fail_threshold() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [0],
            "vbg_hypercap_threshold": [1],
            "unknown_hypercap_threshold": [1],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.85],
            "gas_source_inference_primary_tier": ["label_fluid"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [32.0],
            "anthro_source": ["ICU"],
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
            "unknown_hypercap_threshold": [0, 0, 0],
            "pco2_threshold_0_24h": [1, 1, 1],
            "gas_source_other_rate": [0.2, 0.2, 0.2],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, pd.NA, pd.NA],
            "anthro_source": ["HOSPITAL", "missing", "missing"],
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
            "unknown_hypercap_threshold": [1, 1, 1],
            "pco2_threshold_0_24h": [1, 1, 1],
            "gas_source_other_rate": [0.2, 0.2, 0.2],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 32.0, 34.0],
            "anthro_source": ["HOSPITAL", "ICU", "HOSPITAL"],
            "first_other_src": ["POC", "POC", "LAB"],
            "first_other_pco2": [110.0, 100.0, 40.0],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}

    assert report["status"] == "fail"
    assert "poc_unknown_pco2_median_out_of_bounds" in codes
    assert report["poc_other_pco2_median"] > COHORT_POC_PCO2_MEDIAN_MAX


def test_validate_cohort_contract_flags_poc_quarantine_leakage() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "ed_stay_id": [11, 22],
            "abg_hypercap_threshold": [0, 0],
            "vbg_hypercap_threshold": [1, 1],
            "unknown_hypercap_threshold": [1, 0],
            "pco2_threshold_0_24h": [1, 1],
            "gas_source_other_rate": [0.2, 0.2],
            "gas_source_inference_primary_tier": ["specimen_text", "specimen_text"],
            "gas_source_hint_conflict_rate": [0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0],
            "time_integrity_any": [False, False],
            "timing_usable_for_model": [1, 1],
            "hospital_los_negative_flag": [False, False],
            "admittime_before_ed_intime_flag": [False, False],
            "dischtime_before_admittime_flag": [False, False],
            "bmi_closest_pre_ed": [30.0, 32.0],
            "anthro_source": ["HOSPITAL", "ICU"],
            "first_other_src_detail": ["poc_bg_unknown", "lab_bg_unknown"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "pass"
    assert report["poc_other_quarantine_leak_n"] == 1
    assert "poc_other_quarantine_leakage" not in codes


def test_validate_cohort_contract_flags_first_gas_anchor_without_pco2() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["HOSPITAL"],
            "first_gas_time": [pd.Timestamp("2026-01-01")],
            "first_gas_anchor_has_pco2": [False],
            "first_gas_anchor_source_validated": [True],
            "first_gas_specimen_type": ["arterial blood"],
            "first_gas_specimen_present": [True],
            "first_gas_pco2_itemid": [50818],
            "first_gas_pco2_fluid": ["blood"],
            "co2_other_is_blood_asserted": [False],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "first_gas_anchor_missing_pco2" in codes


def test_validate_cohort_contract_accepts_poc_other_pco2_median_within_bounds() -> None:
    midpoint = (COHORT_POC_PCO2_MEDIAN_MIN + COHORT_POC_PCO2_MEDIAN_MAX) / 2
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "ed_stay_id": [11, 22, 33],
            "abg_hypercap_threshold": [1, 0, 1],
            "vbg_hypercap_threshold": [0, 1, 0],
            "unknown_hypercap_threshold": [1, 1, 0],
            "pco2_threshold_0_24h": [1, 1, 1],
            "gas_source_other_rate": [0.2, 0.2, 0.2],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 32.0, 34.0],
            "anthro_source": ["HOSPITAL", "ICU", "HOSPITAL"],
            "first_other_src": ["POC", "POC", "LAB"],
            "first_other_pco2": [midpoint, midpoint + 2, midpoint - 3],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}

    assert "poc_other_pco2_median_out_of_bounds" not in codes
    assert report["poc_other_pco2_count"] == 2


def test_validate_cohort_contract_flags_first_other_src_poc_rows() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "ed_stay_id": [11, 22],
            "abg_hypercap_threshold": [1, 0],
            "vbg_hypercap_threshold": [0, 1],
            "unknown_hypercap_threshold": [0, 1],
            "pco2_threshold_0_24h": [1, 1],
            "gas_source_other_rate": [0.2, 0.2],
            "gas_source_inference_primary_tier": ["specimen_text", "specimen_text"],
            "gas_source_hint_conflict_rate": [0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0],
            "time_integrity_any": [False, False],
            "timing_usable_for_model": [1, 1],
            "hospital_los_negative_flag": [False, False],
            "admittime_before_ed_intime_flag": [False, False],
            "dischtime_before_admittime_flag": [False, False],
            "bmi_closest_pre_ed": [30.0, 32.0],
            "anthro_source": ["HOSPITAL", "ICU"],
            "first_other_src": ["POC", "LAB_BG_UNKNOWN"],
            "first_other_pco2": [55.0, 50.0],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "pass"
    assert report["first_other_poc_rows"] == 1
    assert "first_other_src_contains_poc" not in codes


def test_validate_cohort_contract_warns_on_low_first_hco3_coverage() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3, 4, 5, 6],
            "ed_stay_id": [11, 22, 33, 44, 55, 66],
            "abg_hypercap_threshold": [1, 0, 1, 0, 1, 0],
            "vbg_hypercap_threshold": [0, 1, 0, 1, 0, 1],
            "unknown_hypercap_threshold": [0, 0, 1, 0, 0, 0],
            "pco2_threshold_0_24h": [1, 1, 1, 1, 1, 1],
            "gas_source_other_rate": [0.2, 0.2, 0.2, 0.2, 0.2, 0.2],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 32.0, 34.0, 35.0, 31.0, 33.0],
            "anthro_source": ["HOSPITAL", "ICU", "HOSPITAL", "ICU", "HOSPITAL", "ICU"],
            "first_hco3": [pd.NA, pd.NA, 24.0, pd.NA, pd.NA, pd.NA],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "first_hco3_coverage_low" in codes


def test_validate_cohort_contract_warns_on_low_first_po2_triplet_coverage() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "ed_stay_id": [11, 22, 33],
            "abg_hypercap_threshold": [1, 1, 1],
            "vbg_hypercap_threshold": [0, 0, 0],
            "unknown_hypercap_threshold": [0, 0, 0],
            "pco2_threshold_0_24h": [1, 1, 1],
            "gas_source_other_rate": [0.1, 0.1, 0.1],
            "gas_source_inference_primary_tier": [
                "specimen_text",
                "specimen_text",
                "specimen_text",
            ],
            "gas_source_hint_conflict_rate": [0.0, 0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0, 1.0],
            "first_abg_pco2": [60.0, 58.0, 62.0],
            "first_abg_po2": [pd.NA, pd.NA, pd.NA],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "po2_triplet_coverage_low" in codes


def test_validate_cohort_contract_fails_when_max_pco2_24h_below_qualifying() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "ed_stay_id": [11, 22],
            "abg_hypercap_threshold": [1, 0],
            "vbg_hypercap_threshold": [0, 1],
            "unknown_hypercap_threshold": [0, 0],
            "pco2_threshold_0_24h": [1, 1],
            "qualifying_pco2_mmhg": [60.0, 55.0],
            "max_pco2_0_24h": [59.0, 70.0],
            "dt_qualifying_hypercapnia_hours": [4.0, 12.0],
            "max_pco2_0_6h": [60.0, 56.0],
            "gas_source_other_rate": [0.1, 0.1],
            "gas_source_inference_primary_tier": ["specimen_text", "specimen_text"],
            "gas_source_hint_conflict_rate": [0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 31.0],
            "anthro_source": ["HOSPITAL", "ICU"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "max_pco2_0_24h_below_qualifying" in codes


def test_validate_cohort_contract_fails_when_max_pco2_6h_below_qualifying_for_early_rows() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "ed_stay_id": [11, 22],
            "abg_hypercap_threshold": [1, 0],
            "vbg_hypercap_threshold": [0, 1],
            "unknown_hypercap_threshold": [0, 0],
            "pco2_threshold_0_24h": [1, 1],
            "qualifying_pco2_mmhg": [60.0, 55.0],
            "max_pco2_0_24h": [65.0, 58.0],
            "dt_qualifying_hypercapnia_hours": [2.0, 8.0],
            "max_pco2_0_6h": [59.0, 54.0],
            "gas_source_other_rate": [0.1, 0.1],
            "gas_source_inference_primary_tier": ["specimen_text", "specimen_text"],
            "gas_source_hint_conflict_rate": [0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 31.0],
            "anthro_source": ["HOSPITAL", "ICU"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert report["status"] == "fail"
    assert "max_pco2_0_6h_below_qualifying_when_dt_le_6h" in codes


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
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "time_integrity_any": [False],
            "timing_usable_for_model": [1],
            "hospital_los_negative_flag": [False],
            "admittime_before_ed_intime_flag": [False],
            "dischtime_before_admittime_flag": [False],
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
        }
    )
    classifier.to_excel(data_dir / CANONICAL_NLP_FILENAME, index=False)
    prior_runs_dir = data_dir / PRIOR_RUNS_DIRNAME
    prior_runs_dir.mkdir(parents=True)
    (prior_runs_dir / "2026-02-16 classifier_cc_missing_audit.csv").write_text(
        "cc_missing_reason,row_count,examples\nvalid,1,shortness of breath\n"
    )
    for suffix in COHORT_REQUIRED_AUDIT_SUFFIXES:
        (prior_runs_dir / f"2026-02-16 {suffix}").write_text("col\nvalue\n")

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
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["ICU"],
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
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["HOSPITAL"],
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
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["HOSPITAL"],
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


def test_validate_cohort_contract_warns_when_timing_usable_missing() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["HOSPITAL"],
            "time_integrity_any": [False],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "missing_timing_usable_for_model" in codes


def test_validate_cohort_contract_flags_timing_usable_mismatch() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "ed_stay_id": [11, 22],
            "abg_hypercap_threshold": [1, 0],
            "vbg_hypercap_threshold": [0, 1],
            "unknown_hypercap_threshold": [0, 0],
            "pco2_threshold_0_24h": [1, 1],
            "gas_source_other_rate": [0.1, 0.1],
            "gas_source_inference_primary_tier": ["specimen_text", "specimen_text"],
            "gas_source_hint_conflict_rate": [0.0, 0.0],
            "gas_source_resolved_rate": [1.0, 1.0],
            "bmi_closest_pre_ed": [30.0, 31.0],
            "anthro_source": ["HOSPITAL", "ICU"],
            "time_integrity_any": [True, False],
            "timing_usable_for_model": [1, 0],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "timing_usable_for_model_inconsistent" in codes


def test_validate_cohort_contract_flags_negative_model_deltas() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["HOSPITAL"],
            "dt_first_imv_hours": [5.0],
            "dt_first_imv_hours_model": [-1.0],
            "imv_time_outside_window_flag": [True],
            "dt_first_niv_hours": [4.0],
            "dt_first_niv_hours_model": [-2.0],
            "niv_time_outside_window_flag": [True],
            "hospital_los_hours_model": [-5.0],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "hospital_los_hours_model_negative" in codes
    assert "dt_first_imv_hours_model_invalid" in codes
    assert "dt_first_niv_hours_model_invalid" in codes


def test_validate_cohort_contract_flags_anthro_model_out_of_bounds() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["HOSPITAL"],
            "bmi_closest_pre_ed_model": [150.0],
            "height_closest_pre_ed_model": [90.0],
            "weight_closest_pre_ed_model": [500.0],
            "bmi_outlier_flag": [True],
            "height_outlier_flag": [True],
            "weight_outlier_flag": [True],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "anthro_model_out_of_bounds" in codes


def test_validate_cohort_contract_flags_invalid_anthro_source_values() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "anthro_source": ["omr"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "anthro_source_invalid_values" in codes


def test_validate_cohort_contract_flags_noncanonical_anthro_units_when_enabled() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "height_closest_pre_ed": [170.0],
            "weight_closest_pre_ed": [75.0],
            "bmi_closest_pre_ed_uom": ["kg/m2"],
            "height_closest_pre_ed_uom": ["in"],
            "weight_closest_pre_ed_uom": ["kg"],
            "bmi_closest_pre_ed_time": [pd.Timestamp("2026-01-01")],
            "height_closest_pre_ed_time": [pd.Timestamp("2026-01-01")],
            "weight_closest_pre_ed_time": [pd.Timestamp("2026-01-01")],
            "anthro_source": ["HOSPITAL"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "anthro_noncanonical_unit" in codes


def test_validate_cohort_contract_flags_duplicate_anthro_alias_columns() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1],
            "ed_stay_id": [11],
            "abg_hypercap_threshold": [1],
            "vbg_hypercap_threshold": [0],
            "unknown_hypercap_threshold": [0],
            "pco2_threshold_0_24h": [1],
            "gas_source_other_rate": [0.1],
            "gas_source_inference_primary_tier": ["specimen_text"],
            "gas_source_hint_conflict_rate": [0.0],
            "gas_source_resolved_rate": [1.0],
            "bmi_closest_pre_ed": [30.0],
            "bmi_closest_pre_ed_uom": ["kg/m2"],
            "bmi_closest_pre_ed_time": [pd.Timestamp("2026-01-01")],
            "height_closest_pre_ed": [170.0],
            "height_closest_pre_ed_uom": ["cm"],
            "height_closest_pre_ed_time": [pd.Timestamp("2026-01-01")],
            "weight_closest_pre_ed": [75.0],
            "weight_closest_pre_ed_uom": ["kg"],
            "weight_closest_pre_ed_time": [pd.Timestamp("2026-01-01")],
            "bmi_closest_pre_ed_unit": ["kg/m2"],
            "anthro_source": ["HOSPITAL"],
        }
    )
    report = validate_cohort_contract(df)
    codes = {finding["code"] for finding in report["findings"]}
    assert "anthro_duplicate_alias_columns_present" in codes
