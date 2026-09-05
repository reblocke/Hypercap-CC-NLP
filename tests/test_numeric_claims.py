from __future__ import annotations

import csv
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

from openpyxl import Workbook

from hypercap_cc_nlp import numeric_claims
from hypercap_cc_nlp.numeric_claims import (
    audit_accepted_provenance,
    audit_figure_manifest,
    audit_live_sources,
    audit_static_targets,
    extract_pdf_n_tokens,
    load_ledger,
)


WORK_DIR = Path(__file__).resolve().parents[1]
LEDGER_PATH = WORK_DIR / "docs" / "NUMERIC_CLAIMS.yml"


def test_repository_numeric_claims_static_contract_passes() -> None:
    ledger = load_ledger(LEDGER_PATH)

    assert audit_static_targets(ledger, repo_root=WORK_DIR) == []


def test_static_scan_finds_stale_phrase_but_excludes_historical_surface(tmp_path: Path) -> None:
    stale_phrase = "Published abstract reports mean F1 " + "0.67"
    (tmp_path / "active.md").write_text(stale_phrase)
    historical = tmp_path / "preprint"
    historical.mkdir()
    (historical / "old.md").write_text(stale_phrase)
    ledger = {
        "metadata": {
            "active_globs": ["**/*.md"],
            "excluded_globs": ["preprint/**"],
            "stale_phrases": [stale_phrase],
        },
        "claims": [],
    }

    findings = audit_static_targets(ledger, repo_root=tmp_path)

    assert len(findings) == 1
    assert findings[0]["issue_type"] == "outdated_section"
    assert findings[0]["location"].endswith("active.md")


def test_live_sources_read_xlsx_csv_json_and_aggregate_sum(tmp_path: Path) -> None:
    results_dir = tmp_path / "Results"
    artifacts_dir = tmp_path / "artifacts"
    results_dir.mkdir()
    artifacts_dir.mkdir()

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Values"
    sheet.append(["group", "flag", "value"])
    sheet.append(["A", True, 2])
    sheet.append(["B", True, 3])
    workbook.save(results_dir / "values.xlsx")
    with (artifacts_dir / "metrics.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["metric", "value"])
        writer.writeheader()
        writer.writerow({"metric": "f1", "value": "0.84"})
    (artifacts_dir / "audit.json").write_text(json.dumps({"counts": {"patients": 7}}))

    ledger = {
        "claims": [
            {
                "claim_id": "xlsx_row",
                "exact_value": 2,
                "source": {
                    "base": "results",
                    "kind": "xlsx_row",
                    "path": "values.xlsx",
                    "sheet": "Values",
                    "match": {"group": "A"},
                    "value_column": "value",
                },
            },
            {
                "claim_id": "xlsx_sum",
                "exact_value": 5,
                "source": {
                    "base": "results",
                    "kind": "xlsx_sum",
                    "path": "values.xlsx",
                    "sheet": "Values",
                    "match": {"flag": True},
                    "value_column": "value",
                },
            },
            {
                "claim_id": "csv",
                "exact_value": 0.84,
                "source": {
                    "base": "artifacts",
                    "kind": "csv_row",
                    "path": "metrics.csv",
                    "match": {"metric": "f1"},
                    "value_column": "value",
                },
            },
            {
                "claim_id": "json",
                "exact_value": 7,
                "source": {
                    "base": "artifacts",
                    "kind": "json_path",
                    "path": "audit.json",
                    "json_path": ["counts", "patients"],
                },
            },
        ]
    }

    findings, observations = audit_live_sources(
        ledger,
        repo_root=tmp_path,
        results_dir=results_dir,
        artifacts_dir=artifacts_dir,
        require_sources=True,
    )

    assert findings == []
    assert {claim_id: item["observed"] for claim_id, item in observations.items()} == {
        "xlsx_row": 2,
        "xlsx_sum": 5,
        "csv": 0.84,
        "json": 7,
    }


def test_live_source_mismatch_is_a_major_contradiction(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "audit.json").write_text(json.dumps({"count": 8}))
    ledger = {
        "claims": [
            {
                "claim_id": "patients",
                "exact_value": 7,
                "source": {
                    "base": "artifacts",
                    "kind": "json_path",
                    "path": "audit.json",
                    "json_path": ["count"],
                },
            }
        ]
    }

    findings, observations = audit_live_sources(
        ledger,
        repo_root=tmp_path,
        results_dir=tmp_path,
        artifacts_dir=artifacts_dir,
        require_sources=True,
    )

    assert observations["patients"]["status"] == "contradiction"
    assert findings[0]["issue_type"] == "contradiction"
    assert findings[0]["severity"] == "major"


def test_figure_manifest_mapping_fails_on_denominator_drift(tmp_path: Path) -> None:
    path = tmp_path / "figure_manifest.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["figure_key", "cohort_denominator_n"])
        writer.writeheader()
        writer.writerow({"figure_key": "figure_1_pdf", "cohort_denominator_n": 10})
    ledger = {"figure_manifest_denominators": {"figure_1_pdf": 11}}

    findings = audit_figure_manifest(ledger, results_dir=tmp_path)

    assert len(findings) == 1
    assert findings[0]["issue_type"] == "contradiction"
    assert "expected" in findings[0]["diagnosis"]


def test_accepted_provenance_requires_exact_date_sha_and_pass_status(tmp_path: Path) -> None:
    results_dir = tmp_path / "2026-08-25"
    artifacts_dir = tmp_path / "artifacts"
    audit_path = artifacts_dir / "acceptance" / "audit.json"
    results_dir.mkdir()
    audit_path.parent.mkdir(parents=True)
    accepted = {
        "results_date": "2026-08-25",
        "producer_sha": "a" * 40,
        "acceptance_audit": "acceptance/audit.json",
    }
    ledger = {"metadata": {"accepted_run": accepted}}
    audit_path.write_text(
        json.dumps(
            {"status": "pass", "code_sha": accepted["producer_sha"], "results_date": "2026-08-25"}
        )
    )

    assert (
        audit_accepted_provenance(
            ledger, results_dir=results_dir, artifacts_dir=artifacts_dir
        )
        == []
    )

    payload = json.loads(audit_path.read_text())
    payload["code_sha"] = "b" * 40
    audit_path.write_text(json.dumps(payload))
    findings = audit_accepted_provenance(
        ledger, results_dir=results_dir, artifacts_dir=artifacts_dir
    )
    assert findings[0]["issue_type"] == "contradiction"


def test_pdf_token_extraction_is_order_invariant_and_deduplicated(
    tmp_path: Path, monkeypatch
) -> None:
    pdf_path = tmp_path / "figure.pdf"
    pdf_path.write_bytes(b"placeholder")

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(stdout="Panel A N=1,542; panel B n = 441; repeated N=1542")

    monkeypatch.setattr(numeric_claims.subprocess, "run", fake_run)

    assert extract_pdf_n_tokens(pdf_path) == [441, 1542]


def test_ledger_sources_remain_aggregate_only_and_drafts_are_excluded() -> None:
    ledger = load_ledger(LEDGER_PATH)
    forbidden_source_tokens = ("MIMIC tabular data", "Annotation/", "subject_id", "hadm_id")
    serialized_sources = json.dumps([claim["source"] for claim in ledger["claims"]])

    assert not any(token in serialized_sources for token in forbidden_source_tokens)
    excluded = ledger["metadata"]["excluded_globs"]
    assert "preprint/**" in excluded
    assert "working-drafts/**" in excluded
    assert "docs/DECISIONS.md" in excluded


def test_ledger_rejects_duplicate_claim_ids(tmp_path: Path) -> None:
    ledger = deepcopy(load_ledger(LEDGER_PATH))
    ledger["claims"].append(deepcopy(ledger["claims"][0]))
    path = tmp_path / "ledger.yml"
    path.write_text(json.dumps(ledger))

    try:
        load_ledger(path)
    except numeric_claims.NumericClaimsError as exc:
        assert "unique" in str(exc)
    else:  # pragma: no cover - protects the schema guard itself
        raise AssertionError("Duplicate claim ids were accepted.")
