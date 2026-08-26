from __future__ import annotations

import json
import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from hypercap_cc_nlp.imv_ticket_parity import (
    BASELINE_RESULT_WORKBOOKS,
    capture_imv_ticket_baseline,
    compare_imv_ticket_baseline,
    resolve_imv_capture_baseline_dir,
)
from hypercap_cc_nlp.workflow_contracts import (
    CANONICAL_COHORT_FILENAME,
    CANONICAL_NLP_FILENAME,
)


RESULTS_DATE = "2026-08-25"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
NOTES_WORKBOOK = "Table 1.xlsx"
CANDIDATE_WORKBOOK = "Candidate_Definition_Yield_Composition.xlsx"


def _git(work_dir: Path, *args: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-c", "user.name=Synthetic Test",
            "-c", "user.email=synthetic@example.invalid",
            *args,
        ],
        cwd=work_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _producer_path(work_dir: Path, stage: str) -> Path:
    return (
        work_dir / "MIMIC tabular data" / "prior runs"
        / f"{RESULTS_DATE} {stage}_run_manifest.json"
    )


def _write_producer_manifests(work_dir: Path, source_commit: str) -> None:
    cohort_relative = f"MIMIC tabular data/{CANONICAL_COHORT_FILENAME}"
    nlp_relative = f"MIMIC tabular data/{CANONICAL_NLP_FILENAME}"
    cohort_hash = _sha256(work_dir / cohort_relative)
    nlp_hash = _sha256(work_dir / nlp_relative)
    for offset, stage in enumerate(("cohort", "classifier", "analysis")):
        manifest = {
            "stage": stage,
            "results_date": RESULTS_DATE,
            "generated_utc": f"{RESULTS_DATE}T{10 + offset}:00:00+00:00",
            "git": {
                "commit": source_commit,
                "dirty": False,
                "require_clean_git": True,
            },
        }
        if stage in {"cohort", "classifier"}:
            manifest["outputs"] = {
                "canonical_output_path": cohort_relative if stage == "cohort" else nlp_relative,
                "canonical_output_sha256": cohort_hash if stage == "cohort" else nlp_hash,
            }
        if stage == "classifier":
            manifest["inputs"] = {
                "cohort_path": cohort_relative,
                "cohort_sha256": cohort_hash,
            }
        if stage == "analysis":
            manifest["analysis_input_path"] = nlp_relative
            manifest["analysis_input_sha256"] = nlp_hash
            manifest["output_verification"] = [
                {
                    "path": f"Results/{RESULTS_DATE}/{filename}",
                    "exists": True,
                    "sha256": _sha256(work_dir / "Results" / RESULTS_DATE / filename),
                }
                for filename in BASELINE_RESULT_WORKBOOKS
            ]
        path = _producer_path(work_dir, stage)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(manifest))


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
    _git(work_dir, "init", "--quiet")
    (work_dir / ".gitignore").write_text(
        "/artifacts/\n/Results/\n/MIMIC tabular data/\n"
    )
    _git(work_dir, "add", ".gitignore")
    _git(work_dir, "commit", "--quiet", "-m", "Synthetic producer")
    _git(work_dir, "tag", "baseline-commit")
    _write_producer_manifests(work_dir, _git(work_dir, "rev-parse", "HEAD"))
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
    manifest["schema_version"] = 99
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


def test_capture_resolves_producer_revision_across_checkout(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    producer_sha = _git(tmp_path, "rev-parse", "baseline-commit")
    _git(tmp_path, "commit", "--quiet", "--allow-empty", "-m", "Capture tooling")
    assert _git(tmp_path, "rev-parse", "HEAD") != producer_sha

    captured = capture_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE,
        source_commit="baseline-commit",
    )

    assert captured["source_commit"] == producer_sha
    assert captured["schema_version"] == 2
    assert len(captured["artifact_sha256"]) == 16
    assert set(captured["producer_manifests"]) == {"cohort", "classifier", "analysis"}
    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "pass"
    assert report["baseline_integrity"]["producer_provenance"] == "verified"


def test_capture_cannot_label_successor_outputs_as_parent(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _git(tmp_path, "commit", "--quiet", "--allow-empty", "-m", "Successor producer")
    _write_producer_manifests(tmp_path, _git(tmp_path, "rev-parse", "HEAD"))

    with pytest.raises(ValueError, match="requested source commit"):
        _capture(tmp_path, baseline_dir)

    assert not _semantic_dir(baseline_dir).exists()


def test_capture_default_head_must_match_producer(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _git(tmp_path, "commit", "--quiet", "--allow-empty", "-m", "New checkout")
    with pytest.raises(ValueError, match="requested source commit"):
        capture_imv_ticket_baseline(
            tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
        )
    assert not _semantic_dir(baseline_dir).exists()


def test_capture_rejects_unresolvable_source_revision(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    with pytest.raises(ValueError, match="does not resolve"):
        capture_imv_ticket_baseline(
            tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE,
            source_commit="not-a-commit",
        )
    assert not _semantic_dir(baseline_dir).exists()


@pytest.mark.parametrize("stage", ["cohort", "classifier", "analysis"])
@pytest.mark.parametrize(
    "failure",
    ["missing", "dirty", "unsealed", "wrong_stage", "wrong_commit", "wrong_date"],
)
def test_capture_requires_clean_producer_provenance(
    tmp_path: Path, stage: str, failure: str,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    path = _producer_path(tmp_path, stage)
    manifest = json.loads(path.read_text())
    if failure == "missing":
        path.unlink()
    else:
        if failure == "dirty":
            manifest["git"]["dirty"] = True
        elif failure == "unsealed":
            manifest["git"]["require_clean_git"] = False
        elif failure == "wrong_commit":
            manifest["git"]["commit"] = "0" * 40
        elif failure == "wrong_date":
            manifest["results_date"] = "2026-08-24"
        else:
            manifest["stage"] = "wrong-stage"
        path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="producer"):
        _capture(tmp_path, baseline_dir)

    assert not _semantic_dir(baseline_dir).exists()


@pytest.mark.parametrize("stage", ["cohort", "classifier", "analysis"])
def test_capture_rejects_current_bytes_not_matching_producer_hash(
    tmp_path: Path, stage: str,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    changed_path = {
        "cohort": tmp_path / "MIMIC tabular data" / CANONICAL_COHORT_FILENAME,
        "classifier": tmp_path / "MIMIC tabular data" / CANONICAL_NLP_FILENAME,
        "analysis": tmp_path / "Results" / RESULTS_DATE / NOTES_WORKBOOK,
    }[stage]
    changed_path.write_bytes(changed_path.read_bytes() + b"changed after production")

    with pytest.raises(ValueError, match="path/hash"):
        _capture(tmp_path, baseline_dir)

    assert not _semantic_dir(baseline_dir).exists()


@pytest.mark.parametrize("stage", ["classifier", "analysis"])
@pytest.mark.parametrize("failure", ["missing", "wrong_hash", "wrong_path"])
def test_capture_requires_hash_linked_producer_inputs(
    tmp_path: Path, stage: str, failure: str,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    path = _producer_path(tmp_path, stage)
    manifest = json.loads(path.read_text())
    section = manifest["inputs"] if stage == "classifier" else manifest
    hash_key = "cohort_sha256" if stage == "classifier" else "analysis_input_sha256"
    path_key = "cohort_path" if stage == "classifier" else "analysis_input_path"
    if failure == "missing":
        del section[hash_key]
    elif failure == "wrong_hash":
        section[hash_key] = "0" * 64
    else:
        section[path_key] = "<external>/wrong.xlsx"
    path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="producer input path/hash"):
        _capture(tmp_path, baseline_dir)

    assert not _semantic_dir(baseline_dir).exists()


@pytest.mark.parametrize(
    "failure", ["missing_hash", "wrong_date", "conflicting_duplicate", "not_produced"]
)
def test_capture_requires_unambiguous_hashed_workbook_outputs(
    tmp_path: Path, failure: str,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    path = _producer_path(tmp_path, "analysis")
    manifest = json.loads(path.read_text())
    entry = manifest["output_verification"][0]
    if failure == "missing_hash":
        del entry["sha256"]
    elif failure == "wrong_date":
        entry["path"] = entry["path"].replace(RESULTS_DATE, "2026-08-24")
    elif failure == "conflicting_duplicate":
        manifest["output_verification"].append({**entry, "sha256": "0" * 64})
    else:
        entry["exists"] = False
    path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="producer output path/hash"):
        _capture(tmp_path, baseline_dir)

    assert not _semantic_dir(baseline_dir).exists()


def test_capture_accepts_repeated_identical_output_verification(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    path = _producer_path(tmp_path, "analysis")
    manifest = json.loads(path.read_text())
    manifest["output_verification"] += [
        entry.copy() for entry in manifest["output_verification"]
    ]
    path.write_text(json.dumps(manifest))

    _capture(tmp_path, baseline_dir)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "pass"


def test_capture_results_date_can_differ_from_actual_production_date(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    for stage in ("cohort", "classifier", "analysis"):
        path = _producer_path(tmp_path, stage)
        manifest = json.loads(path.read_text())
        manifest["generated_utc"] = manifest["generated_utc"].replace(
            RESULTS_DATE, "2026-08-26"
        )
        path.write_text(json.dumps(manifest))

    _capture(tmp_path, baseline_dir)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "pass"


def test_capture_accepts_sanitized_symlink_handoff_paths(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    for stage in ("cohort", "classifier", "analysis"):
        path = _producer_path(tmp_path, stage)
        path.write_text(path.read_text().replace("MIMIC tabular data/", "<external>/"))

    _capture(tmp_path, baseline_dir)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )
    assert report["status"] == "pass"


def test_capture_rejects_out_of_order_producer_manifests(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    path = _producer_path(tmp_path, "analysis")
    manifest = json.loads(path.read_text())
    manifest["generated_utc"] = f"{RESULTS_DATE}T09:00:00+00:00"
    path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="not in cohort/classifier/analysis order"):
        _capture(tmp_path, baseline_dir)
    assert not _semantic_dir(baseline_dir).exists()


@pytest.mark.parametrize("absolute", [False, True])
def test_cli_capture_creates_new_relative_or_absolute_baseline(
    tmp_path: Path, absolute: bool,
) -> None:
    _build_workspace(tmp_path)
    new_baseline = tmp_path / "artifacts" / "qa" / "baselines" / "jupyter" / "new-capture"
    assert not new_baseline.exists()
    argument = str(new_baseline) if absolute else "new-capture"

    completed = subprocess.run(
        [
            sys.executable,
            str(REPOSITORY_ROOT / "scripts" / "imv_ticket_parity.py"),
            "capture", "--baseline", argument, "--results-date", RESULTS_DATE,
            "--source-commit", "baseline-commit",
        ],
        cwd=tmp_path, check=False, capture_output=True, text=True,
    )

    assert completed.returncode == 0, completed.stderr
    manifest = json.loads((_semantic_dir(new_baseline) / "semantic_manifest.json").read_text())
    assert manifest["source_commit"] == _git(tmp_path, "rev-parse", "HEAD")
    assert manifest["schema_version"] == 2


@pytest.mark.parametrize("baseline", ["latest", "", "../../../escape"])
def test_new_baseline_resolution_rejects_implicit_or_escaping_paths(
    tmp_path: Path, baseline: str,
) -> None:
    _build_workspace(tmp_path)
    with pytest.raises(ValueError):
        resolve_imv_capture_baseline_dir(tmp_path, baseline)


def test_new_baseline_rejects_absolute_and_symlink_escape(tmp_path: Path) -> None:
    _build_workspace(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "artifacts" / "qa" / "baselines" / "jupyter" / "link"
    link.symlink_to(outside, target_is_directory=True)
    for requested in (outside / "capture", link / "capture"):
        with pytest.raises(ValueError, match="must be a child"):
            resolve_imv_capture_baseline_dir(tmp_path, str(requested))
    assert not (outside / "capture").exists()


def test_capture_requires_ignored_private_baseline_root(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    ignore_path = tmp_path / ".gitignore"
    ignore_path.write_text(ignore_path.read_text().replace("/artifacts/\n", ""))

    with pytest.raises(ValueError, match="must be Git-ignored"):
        _capture(tmp_path, baseline_dir)
    assert not _semantic_dir(baseline_dir).exists()


@pytest.mark.parametrize("completed", [False, True])
def test_capture_never_overwrites_completed_or_partial_baseline(
    tmp_path: Path, completed: bool,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    semantic_dir = _semantic_dir(baseline_dir)
    if completed:
        _capture(tmp_path, baseline_dir)
    else:
        semantic_dir.mkdir()
        (semantic_dir / "partial.txt").write_text("preserve this partial capture")
    before = {str(p.relative_to(semantic_dir)): _sha256(p) for p in semantic_dir.rglob("*") if p.is_file()}

    with pytest.raises(FileExistsError, match="already exists"):
        _capture(tmp_path, baseline_dir)

    after = {str(p.relative_to(semantic_dir)): _sha256(p) for p in semantic_dir.rglob("*") if p.is_file()}
    assert after == before


def test_compare_does_not_create_a_missing_baseline(tmp_path: Path) -> None:
    _build_workspace(tmp_path)
    missing = tmp_path / "artifacts" / "qa" / "baselines" / "jupyter" / "missing"
    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=missing, results_date=RESULTS_DATE
    )
    assert report["status"] == "fail"
    assert _integrity_error_codes(report) == {"manifest_missing"}
    assert not missing.exists()


def test_v2_copy_bytes_are_checked_beyond_semantic_membership(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    baseline_path = _semantic_dir(baseline_dir) / "MIMIC tabular data" / CANONICAL_COHORT_FILENAME
    frame = pd.read_excel(baseline_path, engine="openpyxl")
    frame["unrelated_synthetic_value"] = [1, 2]
    frame.to_excel(baseline_path, index=False)

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert "artifact_file_hash_mismatch" in _integrity_error_codes(report)
    assert "baseline_semantic_hash_mismatch" not in _integrity_error_codes(report)


def test_v2_copied_producer_manifests_are_hash_checked(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    path = _semantic_dir(baseline_dir) / "producer_manifests" / "analysis_run_manifest.json"
    path.write_text(path.read_text() + "\n")

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert "producer_manifest_hash_mismatch" in _integrity_error_codes(report)


def test_v2_missing_copied_producer_manifest_fails_closed(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    path = _semantic_dir(baseline_dir) / "producer_manifests" / "analysis_run_manifest.json"
    path.unlink()

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert "producer_manifest_copy_inventory_mismatch" in _integrity_error_codes(report)
    assert "producer_manifest_copy_unreadable" in _integrity_error_codes(report)


def test_v2_relabeling_manifest_source_commit_fails_provenance(tmp_path: Path) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    path = _semantic_dir(baseline_dir) / "semantic_manifest.json"
    manifest = json.loads(path.read_text())
    manifest["source_commit"] = "0" * 40
    path.write_text(json.dumps(manifest))

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "fail"
    assert "producer_provenance_invalid" in _integrity_error_codes(report)


def test_legacy_v1_baseline_remains_read_only_and_explicitly_unverified(
    tmp_path: Path,
) -> None:
    baseline_dir = _build_workspace(tmp_path)
    _capture(tmp_path, baseline_dir)
    semantic_dir = _semantic_dir(baseline_dir)
    path = semantic_dir / "semantic_manifest.json"
    manifest = json.loads(path.read_text())
    manifest["schema_version"] = 1
    manifest["source_commit"] = "historical-source-label"
    del manifest["artifact_sha256"]
    del manifest["producer_manifests"]
    path.write_text(json.dumps(manifest))
    before = {str(p.relative_to(semantic_dir)): _sha256(p) for p in semantic_dir.rglob("*") if p.is_file()}

    report = compare_imv_ticket_baseline(
        tmp_path, baseline_dir=baseline_dir, results_date=RESULTS_DATE
    )

    assert report["status"] == "pass"
    assert report["baseline_integrity"]["producer_provenance"] == "legacy_unverified"
    after = {str(p.relative_to(semantic_dir)): _sha256(p) for p in semantic_dir.rglob("*") if p.is_file()}
    assert after == before
