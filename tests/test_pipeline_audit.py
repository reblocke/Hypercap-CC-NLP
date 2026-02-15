from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from hypercap_cc_nlp.pipeline_audit import (
    ANALYSIS_EXPORT_FILENAMES,
    build_audit_report,
    compute_metric_drift,
    load_and_validate_artifacts,
    scan_logs_for_findings,
)
from hypercap_cc_nlp.workflow_contracts import (
    CANONICAL_COHORT_FILENAME,
    CANONICAL_NLP_FILENAME,
)


def _past_iso(minutes: int = 5) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _write_minimal_required_artifacts(tmp_path: Path) -> None:
    data_dir = tmp_path / "MIMIC tabular data"
    data_dir.mkdir(parents=True)
    cohort_df = pd.DataFrame(
        {
            "hadm_id": [1],
            "subject_id": [100],
            "ed_stay_id": [10],
            "anthro_timing_tier": ["pre_ed_365"],
            "anthro_days_offset": [1],
            "anthro_chartdate": ["2026-01-01"],
            "anthro_timing_uncertain": [False],
        }
    )
    cohort_df.to_excel(data_dir / CANONICAL_COHORT_FILENAME, index=False)

    classifier_df = pd.DataFrame(
        {
            "hadm_id": [1],
            "subject_id": [100],
            "RFV1": ["resp"],
            "RFV2": [""],
            "RFV3": [""],
            "RFV4": [""],
            "RFV5": [""],
        }
    )
    classifier_df.to_excel(data_dir / CANONICAL_NLP_FILENAME, index=False)

    qa_summary = {
        "gas_source_audit": {"all_other_or_unknown": False},
        "omr_diagnostics": {
            "pre_window_candidate_rows": 0,
            "post_window_candidate_rows": 0,
            "attached_non_null_counts": {
                "bmi_closest_pre_ed": 0,
                "height_closest_pre_ed": 0,
                "weight_closest_pre_ed": 0,
            },
        },
        "uom_expectation_checks": {
            "paco2_uom_checks": {
                "poc_other_paco2_uom": {"present": True, "passes": True},
            }
        },
        "vitals_outlier_audit": [
            {"raw_column": "ed_first_hr", "out_of_range_pct": 0.001}
        ],
        "first_other_pco2_audit": [
            {"source": "POC", "pct_eq_160": 0.05},
        ],
        "icu_link_rate": 0.77,
        "pct_any_gas_0_6h": 0.37,
        "pct_any_gas_0_24h": 0.94,
        "gas_source_other_rate": 0.75,
    }
    (tmp_path / "qa_summary.json").write_text(json.dumps(qa_summary))

    rater_dir = tmp_path / "annotation_agreement_outputs_nlp"
    rater_dir.mkdir(parents=True)
    (rater_dir / "R3_vs_NLP_summary.txt").write_text("ok")
    (rater_dir / "R3_vs_NLP_join_audit.json").write_text(
        json.dumps({"matched_rows": 1, "severity": "info"})
    )

    for filename in ANALYSIS_EXPORT_FILENAMES:
        pd.DataFrame({"x": [1]}).to_excel(tmp_path / filename, index=False)


def test_load_and_validate_artifacts_missing_artifacts_is_hard_fail(tmp_path: Path) -> None:
    result = load_and_validate_artifacts(tmp_path, run_started_at_utc=_past_iso())
    codes = {finding["code"] for finding in result["findings"]}
    severities = {finding["severity"] for finding in result["findings"]}

    assert "missing_artifact" in codes
    assert "P0" in severities


def test_load_and_validate_artifacts_invalid_json_is_hard_fail(tmp_path: Path) -> None:
    _write_minimal_required_artifacts(tmp_path)
    (tmp_path / "qa_summary.json").write_text("{not-json")

    result = load_and_validate_artifacts(tmp_path, run_started_at_utc=_past_iso())
    codes = {finding["code"] for finding in result["findings"]}
    assert "invalid_qa_summary_json" in codes


def test_load_and_validate_artifacts_infinite_values_is_hard_fail(tmp_path: Path) -> None:
    _write_minimal_required_artifacts(tmp_path)
    nlp_path = tmp_path / "MIMIC tabular data" / CANONICAL_NLP_FILENAME
    df = pd.read_excel(nlp_path, engine="openpyxl")
    df["numeric_bad"] = [np.inf]
    df.to_excel(nlp_path, index=False)

    result = load_and_validate_artifacts(tmp_path, run_started_at_utc=_past_iso())
    codes = {finding["code"] for finding in result["findings"]}
    assert "infinite_values_detected" in codes


def test_compute_metric_drift_emits_warning_and_fail_thresholds() -> None:
    current = {
        "cohort_rows": 111.0,
        "gas_source_other_rate": 0.92,
        "pct_any_gas_0_6h": 0.40,
    }
    baseline = {
        "cohort_rows": 100.0,
        "gas_source_other_rate": 0.80,
        "pct_any_gas_0_6h": 0.37,
    }
    drift = compute_metric_drift(current, baseline).set_index("metric")

    assert drift.loc["cohort_rows", "severity"] == "fail"
    assert drift.loc["gas_source_other_rate", "severity"] == "fail"
    assert drift.loc["pct_any_gas_0_6h", "severity"] == "ok"


def test_scan_logs_for_findings_classifies_traceback_as_p0(tmp_path: Path) -> None:
    log_path = tmp_path / "stage.log"
    log_path.write_text("hello\nTraceback (most recent call last):\nboom\n")
    findings = scan_logs_for_findings(
        [
            {
                "stage_id": "01_cohort",
                "log_path": str(log_path),
            }
        ]
    )

    assert not findings.empty
    assert findings.iloc[0]["severity"] == "P0"
    assert findings.iloc[0]["pattern"] == "traceback"


def test_build_audit_report_clean_warning_and_fail_statuses() -> None:
    manifest = {"run_id": "abc"}
    pipeline_ok = {
        "stages": [
            {"stage_id": "01_cohort", "returncode": 0, "status": "ok"},
        ]
    }
    artifact_clean = {
        "artifact_checks": {},
        "current_metrics": {},
        "findings": [],
    }
    empty_drift = pd.DataFrame(columns=["metric", "severity"])
    empty_logs = pd.DataFrame(columns=["severity"])

    pass_report = build_audit_report(
        run_id="abc",
        manifest=manifest,
        pipeline_run=pipeline_ok,
        preflight_findings=[],
        artifact_result=artifact_clean,
        drift_df=empty_drift,
        log_findings_df=empty_logs,
        strictness="fail_on_key_anomalies",
        baseline_info={"baseline_mode": "latest", "metrics": {}},
    )
    assert pass_report["status"] == "pass"

    warning_drift = pd.DataFrame(
        [
            {
                "metric": "icu_link_rate",
                "severity": "warning",
                "abs_delta": 0.05,
                "current_value": 0.8,
                "baseline_value": 0.75,
            }
        ]
    )
    warning_report = build_audit_report(
        run_id="abc",
        manifest=manifest,
        pipeline_run=pipeline_ok,
        preflight_findings=[],
        artifact_result=artifact_clean,
        drift_df=warning_drift,
        log_findings_df=empty_logs,
        strictness="fail_on_key_anomalies",
        baseline_info={"baseline_mode": "latest", "metrics": {}},
    )
    assert warning_report["status"] == "warning"

    fail_report = build_audit_report(
        run_id="abc",
        manifest=manifest,
        pipeline_run={
            "stages": [
                {"stage_id": "01_cohort", "returncode": 1, "status": "failed"},
            ]
        },
        preflight_findings=[],
        artifact_result=artifact_clean,
        drift_df=empty_drift,
        log_findings_df=empty_logs,
        strictness="fail_on_key_anomalies",
        baseline_info={"baseline_mode": "latest", "metrics": {}},
    )
    assert fail_report["status"] == "fail"
