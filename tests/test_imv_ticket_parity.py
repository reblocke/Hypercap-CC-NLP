from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

from hypercap_cc_nlp.imv_ticket_parity import (
    BASELINE_RESULT_WORKBOOKS,
    capture_imv_ticket_baseline,
    compare_imv_ticket_baseline,
)
from hypercap_cc_nlp.workflow_contracts import (
    CANONICAL_COHORT_FILENAME,
    CANONICAL_NLP_FILENAME,
)


RESULTS_DATE = "2026-08-25"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
NOTES_WORKBOOK = "Table 1.xlsx"
CANDIDATE_WORKBOOK = "Candidate_Definition_Yield_Composition.xlsx"


def _build_workspace(work_dir: Path) -> Path:
    data_dir = work_dir / "MIMIC tabular data"
    results_dir = work_dir / "Results" / RESULTS_DATE
    baseline_dir = work_dir / "artifacts" / "qa" / "baselines" / "jupyter" / "base"
    data_dir.mkdir(parents=True)
    results_dir.mkdir(parents=True)
    baseline_dir.mkdir(parents=True)

    pd.DataFrame(
        {
            "hadm_id": [10, 11],
            "subject_id": [100, 101],
            "ed_stay_id": [1000, 1001],
        }
    ).to_excel(data_dir / CANONICAL_COHORT_FILENAME, index=False)
    nlp = {
        "hadm_id": [10, 11],
        "subject_id": [100, 101],
    }
    for slot in range(1, 6):
        nlp[f"RFV{slot}"] = [f"A{slot}", f"B{slot}"]
        nlp[f"RFV{slot}_name"] = [f"Alpha {slot}", f"Beta {slot}"]
    pd.DataFrame(nlp).to_excel(data_dir / CANONICAL_NLP_FILENAME, index=False)

    for filename in BASELINE_RESULT_WORKBOOKS:
        with pd.ExcelWriter(results_dir / filename, engine="openpyxl") as writer:
            pd.DataFrame({"group": ["A", "B"], "value": [1.0, 2.0]}).to_excel(
                writer, sheet_name="Data", index=False
            )
            if filename == CANDIDATE_WORKBOOK:
                pd.DataFrame(
                    {"definition": ["Primary", "Sensitivity"], "yield": [10, 20]}
                ).to_excel(writer, sheet_name="Candidate_Definitions", index=False)
            pd.DataFrame({"note": ["baseline"]}).to_excel(
                writer, sheet_name="Notes", index=False
            )
    return baseline_dir


def _semantic_dir(baseline_dir: Path) -> Path:
    return baseline_dir / "imv_ticket_semantic"


def _capture(tmp_path: Path, baseline_dir: Path) -> None:
    capture_imv_ticket_baseline(
        tmp_path,
        baseline_dir=baseline_dir,
        results_date=RESULTS_DATE,
        source_commit="baseline-commit",
    )


def _integrity_error_codes(report: dict[str, object]) -> set[str]:
    integrity = report["baseline_integrity"]
    assert isinstance(integrity, dict)
    errors = integrity["errors"]
    assert isinstance(errors, list)
    return {error["code"] for error in errors}


def test_imv_ticket_semantic_parity_passes_for_unchanged_outputs(
    tmp_path: Path,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    capture_imv_ticket_baseline(
        tmp_path,
        baseline_dir=baseline_dir,
        results_date=RESULTS_DATE,
        source_commit="baseline-commit",
    )
    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "pass"
    assert report["baseline_integrity"]["status"] == "pass"
    assert report["cohort_membership"]["equal"] is True
    assert report["rfv_assignments"]["equal"] is True


def test_imv_ticket_semantic_parity_fails_on_rfv_change(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    capture_imv_ticket_baseline(
        tmp_path,
        baseline_dir=baseline_dir,
        results_date=RESULTS_DATE,
        source_commit="baseline-commit",
    )
    nlp_path = tmp_path / "MIMIC tabular data" / CANONICAL_NLP_FILENAME
    nlp = pd.read_excel(nlp_path, engine="openpyxl")
    nlp.loc[0, "RFV1"] = "CHANGED"
    nlp.to_excel(nlp_path, index=False)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "fail"
    assert report["rfv_assignments"]["equal"] is False


def test_imv_ticket_semantic_parity_fails_on_existing_estimate_change(
    tmp_path: Path,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    capture_imv_ticket_baseline(
        tmp_path,
        baseline_dir=baseline_dir,
        results_date=RESULTS_DATE,
        source_commit="baseline-commit",
    )
    workbook = tmp_path / "Results" / RESULTS_DATE / BASELINE_RESULT_WORKBOOKS[0]
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"group": ["A", "B"], "value": [1.0, 3.0]}).to_excel(
            writer, sheet_name="Data", index=False
        )
        pd.DataFrame({"note": ["baseline"]}).to_excel(
            writer, sheet_name="Notes", index=False
        )

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "fail"
    workbook_report = report["workbooks"][BASELINE_RESULT_WORKBOOKS[0]]
    assert workbook_report["sheet_results"]["Data"]["changed_cells"] == 1


def test_imv_ticket_semantic_parity_warns_on_notes_only_change(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    capture_imv_ticket_baseline(
        tmp_path,
        baseline_dir=baseline_dir,
        results_date=RESULTS_DATE,
        source_commit="baseline-commit",
    )
    workbook = tmp_path / "Results" / RESULTS_DATE / NOTES_WORKBOOK
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"group": ["A", "B"], "value": [1.0, 2.0]}).to_excel(
            writer, sheet_name="Data", index=False
        )
        pd.DataFrame({"note": ["updated definition"]}).to_excel(
            writer, sheet_name="Notes", index=False
        )

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "warning"


def test_candidate_definitions_is_a_substantive_data_sheet(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    workbook = tmp_path / "Results" / RESULTS_DATE / CANDIDATE_WORKBOOK
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"group": ["A", "B"], "value": [1.0, 2.0]}).to_excel(
            writer, sheet_name="Data", index=False
        )
        pd.DataFrame(
            {"definition": ["Primary", "Sensitivity"], "yield": [10, 21]}
        ).to_excel(writer, sheet_name="Candidate_Definitions", index=False)
        pd.DataFrame({"note": ["baseline"]}).to_excel(
            writer, sheet_name="Notes", index=False
        )

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    sheet_report = report["workbooks"][CANDIDATE_WORKBOOK]["sheet_results"][
        "Candidate_Definitions"
    ]
    assert sheet_report["notes_sheet"] is False
    assert sheet_report["changed_cells"] == 1


def test_notes_allowlist_is_case_and_whitespace_normalized(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    workbook = tmp_path / "Results" / RESULTS_DATE / NOTES_WORKBOOK
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame({"group": ["A", "B"], "value": [1.0, 2.0]}).to_excel(
            writer, sheet_name="Data", index=False
        )
        pd.DataFrame({"note": ["updated"]}).to_excel(
            writer, sheet_name="NoTeS", index=False
        )

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "warning"
    workbook_report = report["workbooks"][NOTES_WORKBOOK]
    assert workbook_report["missing_sheets"] == ["Notes"]
    assert workbook_report["extra_sheets"] == ["NoTeS"]


def test_baseline_cohort_copy_must_match_capture_manifest(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    current_path = tmp_path / "MIMIC tabular data" / CANONICAL_COHORT_FILENAME
    baseline_path = (
        _semantic_dir(baseline_dir) / "MIMIC tabular data" / CANONICAL_COHORT_FILENAME
    )
    changed = pd.read_excel(current_path, engine="openpyxl")
    changed.loc[0, "subject_id"] = 999
    changed.to_excel(current_path, index=False)
    changed.to_excel(baseline_path, index=False)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert report["cohort_membership"]["status"] == "not_compared"
    assert "baseline_semantic_hash_mismatch" in _integrity_error_codes(report)


def test_baseline_classifier_copy_must_match_capture_manifest(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    current_path = tmp_path / "MIMIC tabular data" / CANONICAL_NLP_FILENAME
    baseline_path = (
        _semantic_dir(baseline_dir) / "MIMIC tabular data" / CANONICAL_NLP_FILENAME
    )
    changed = pd.read_excel(current_path, engine="openpyxl")
    changed.loc[0, "RFV1"] = "CHANGED"
    changed.to_excel(current_path, index=False)
    changed.to_excel(baseline_path, index=False)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert report["rfv_assignments"]["status"] == "not_compared"
    assert "baseline_semantic_hash_mismatch" in _integrity_error_codes(report)


def test_baseline_workbook_copy_must_match_capture_manifest(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    current_path = tmp_path / "Results" / RESULTS_DATE / NOTES_WORKBOOK
    baseline_path = (
        _semantic_dir(baseline_dir) / "Results" / RESULTS_DATE / NOTES_WORKBOOK
    )
    for path in (current_path, baseline_path):
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            pd.DataFrame({"group": ["A", "B"], "value": [1.0, 9.0]}).to_excel(
                writer, sheet_name="Data", index=False
            )
            pd.DataFrame({"note": ["baseline"]}).to_excel(
                writer, sheet_name="Notes", index=False
            )

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert report["workbooks"] == {}
    assert "workbook_sheet_signature_mismatch" in _integrity_error_codes(report)


def test_corrupt_baseline_workbook_fails_integrity_closed(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    baseline_path = (
        _semantic_dir(baseline_dir) / "Results" / RESULTS_DATE / NOTES_WORKBOOK
    )
    baseline_path.write_bytes(b"not an xlsx archive")

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert report["workbooks"] == {}
    assert "baseline_workbook_invalid" in _integrity_error_codes(report)


def test_missing_baseline_workbook_fails_integrity(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    baseline_path = (
        _semantic_dir(baseline_dir) / "Results" / RESULTS_DATE / NOTES_WORKBOOK
    )
    baseline_path.unlink()

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    error_codes = _integrity_error_codes(report)
    assert "workbook_copy_inventory_mismatch" in error_codes
    assert "baseline_workbook_missing" in error_codes


def test_extra_baseline_workbook_fails_integrity(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    extra_path = (
        _semantic_dir(baseline_dir) / "Results" / RESULTS_DATE / "Unexpected.xlsx"
    )
    pd.DataFrame({"aggregate": [1]}).to_excel(extra_path, index=False)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert "workbook_copy_inventory_mismatch" in _integrity_error_codes(report)


def test_manifest_workbook_inventory_mismatch_fails_integrity(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    manifest_path = _semantic_dir(baseline_dir) / "semantic_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    del manifest["result_workbooks"][NOTES_WORKBOOK]
    manifest_path.write_text(json.dumps(manifest))

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert "workbook_manifest_inventory_mismatch" in _integrity_error_codes(report)


def test_unknown_manifest_schema_fails_closed(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    manifest_path = _semantic_dir(baseline_dir) / "semantic_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = 2
    manifest_path.write_text(json.dumps(manifest))

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert "unsupported_schema_version" in _integrity_error_codes(report)


def test_malformed_manifest_fails_closed(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    manifest_path = _semantic_dir(baseline_dir) / "semantic_manifest.json"
    manifest_path.write_text("{")

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert report["baseline_results_date"] is None
    assert report["workbooks"] == {}
    assert _integrity_error_codes(report) == {"manifest_unreadable"}


def test_cli_writes_aggregate_report_and_exits_nonzero_on_baseline_tamper(
    tmp_path: Path,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    baseline_path = (
        _semantic_dir(baseline_dir) / "MIMIC tabular data" / CANONICAL_COHORT_FILENAME
    )
    changed = pd.read_excel(baseline_path, engine="openpyxl")
    changed.loc[0, "subject_id"] = 999
    changed.to_excel(baseline_path, index=False)
    output_path = tmp_path / "semantic_report.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(REPOSITORY_ROOT / "scripts" / "imv_ticket_parity.py"),
            "compare",
            "--baseline",
            str(baseline_dir),
            "--results-date",
            RESULTS_DATE,
            "--output",
            str(output_path),
        ],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    report = json.loads(output_path.read_text())
    assert report["status"] == "fail"
    assert report["baseline_integrity"]["status"] == "fail"
    assert report["cohort_membership"] == {"status": "not_compared"}
    for error in report["baseline_integrity"]["errors"]:
        assert set(error) <= {"code", "artifact", "sheet"}
