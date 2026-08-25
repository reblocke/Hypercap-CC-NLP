from __future__ import annotations

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
            pd.DataFrame({"note": ["baseline"]}).to_excel(
                writer, sheet_name="Notes", index=False
            )
    return baseline_dir


def test_imv_ticket_semantic_parity_passes_for_unchanged_outputs(tmp_path: Path) -> None:
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
    workbook = tmp_path / "Results" / RESULTS_DATE / BASELINE_RESULT_WORKBOOKS[0]
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
