from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hypercap_cc_nlp.pipeline_audit import ANALYSIS_EXPORT_FILENAMES
from hypercap_cc_nlp.pipeline_parity import (
    capture_jupyter_baseline,
    compare_current_to_baseline,
    resolve_baseline_dir,
)
from hypercap_cc_nlp.workflow_contracts import (
    CANONICAL_COHORT_FILENAME,
    CANONICAL_NLP_FILENAME,
)


def _write_required_workspace_artifacts(work_dir: Path) -> None:
    data_dir = work_dir / "MIMIC tabular data"
    data_dir.mkdir(parents=True, exist_ok=True)

    cohort = pd.DataFrame(
        {
            "hadm_id": [10, 11],
            "subject_id": [100, 101],
            "ed_stay_id": [1, 2],
            "anthro_timing_tier": ["pre_ed_365", "post_ed_365"],
            "anthro_days_offset": [7, -2],
            "anthro_chartdate": ["2026-01-01", "2026-01-02"],
            "anthro_timing_uncertain": [False, True],
        }
    )
    cohort.to_excel(data_dir / CANONICAL_COHORT_FILENAME, index=False)

    classifier = pd.DataFrame(
        {
            "hadm_id": [10, 11],
            "subject_id": [100, 101],
            "RFV1": ["A", "B"],
            "RFV2": ["", ""],
            "RFV3": ["", ""],
            "RFV4": ["", ""],
            "RFV5": ["", ""],
        }
    )
    classifier.to_excel(data_dir / CANONICAL_NLP_FILENAME, index=False)

    qa_summary = {
        "icu_link_rate": 0.7,
        "pct_any_gas_0_6h": 0.3,
        "pct_any_gas_0_24h": 0.9,
        "gas_source_other_rate": 0.75,
        "first_other_pco2_audit": [{"source": "POC", "pct_eq_160": 0.1}],
    }
    (work_dir / "qa_summary.json").write_text(json.dumps(qa_summary))

    rater_dir = work_dir / "annotation_agreement_outputs_nlp"
    rater_dir.mkdir(parents=True, exist_ok=True)
    (rater_dir / "R3_vs_NLP_join_audit.json").write_text(
        json.dumps({"matched_rows": 2, "severity": "info"})
    )
    (rater_dir / "R3_vs_NLP_summary.txt").write_text("ok")

    for name in ANALYSIS_EXPORT_FILENAMES:
        output_path = work_dir / name
        if output_path.suffix.lower() == ".xlsx":
            pd.DataFrame({"x": [1.0, 2.0]}).to_excel(output_path, index=False)
        else:
            output_path.write_bytes(b"placeholder")


def test_compare_missing_artifact_is_fail(tmp_path: Path) -> None:
    _write_required_workspace_artifacts(tmp_path)
    baseline = capture_jupyter_baseline(tmp_path, baseline_id="baseline1")

    (tmp_path / "qa_summary.json").unlink()
    report = compare_current_to_baseline(
        tmp_path,
        baseline_dir=Path(baseline["baseline_dir"]),
    )
    assert report["status"] == "fail"
    assert report["summary"]["p0_count"] == 1
    assert report["findings"][0]["code"] == "missing_artifact"


def test_compare_row_mismatch_is_fail(tmp_path: Path) -> None:
    _write_required_workspace_artifacts(tmp_path)
    baseline = capture_jupyter_baseline(tmp_path, baseline_id="baseline2")

    cohort_path = tmp_path / "MIMIC tabular data" / CANONICAL_COHORT_FILENAME
    cohort = pd.read_excel(cohort_path, engine="openpyxl")
    cohort = cohort.iloc[:1]
    cohort.to_excel(cohort_path, index=False)

    report = compare_current_to_baseline(
        tmp_path,
        baseline_dir=Path(baseline["baseline_dir"]),
    )
    assert report["status"] == "fail"
    assert any(item["code"] == "row_count_mismatch" for item in report["findings"])


def test_compare_metric_thresholds_warn_and_fail(tmp_path: Path) -> None:
    _write_required_workspace_artifacts(tmp_path)
    baseline = capture_jupyter_baseline(tmp_path, baseline_id="baseline3")

    qa_path = tmp_path / "qa_summary.json"
    qa_summary = json.loads(qa_path.read_text())
    qa_summary["icu_link_rate"] = 0.75  # warn delta 0.05
    qa_summary["gas_source_other_rate"] = 0.9  # fail delta 0.15
    qa_path.write_text(json.dumps(qa_summary))

    report = compare_current_to_baseline(
        tmp_path,
        baseline_dir=Path(baseline["baseline_dir"]),
    )
    assert report["status"] == "fail"
    codes = [item["code"] for item in report["findings"]]
    assert "metric_delta_warning" in codes
    assert "metric_delta_fail" in codes


def test_compare_analysis_sheet_shape_mismatch_is_fail(tmp_path: Path) -> None:
    _write_required_workspace_artifacts(tmp_path)
    baseline = capture_jupyter_baseline(tmp_path, baseline_id="baseline4")

    target_name = next(name for name in ANALYSIS_EXPORT_FILENAMES if name.endswith(".xlsx"))
    target = tmp_path / target_name
    pd.DataFrame({"x": [1.0]}).to_excel(target, index=False)
    report = compare_current_to_baseline(
        tmp_path,
        baseline_dir=Path(baseline["baseline_dir"]),
    )
    assert report["status"] == "warning"
    assert any(item["code"] == "analysis_shape_changed" for item in report["findings"])


def test_resolve_latest_baseline(tmp_path: Path) -> None:
    _write_required_workspace_artifacts(tmp_path)
    capture_jupyter_baseline(tmp_path, baseline_id="old")
    capture_jupyter_baseline(tmp_path, baseline_id="new")
    resolved = resolve_baseline_dir(tmp_path, "latest")
    assert resolved.name == "new"
