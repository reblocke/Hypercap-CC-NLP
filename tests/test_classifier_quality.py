from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hypercap_cc_nlp.classifier_quality import (
    validate_classifier_contract,
    verify_classifier_resources,
)


def test_validate_classifier_contract_reports_blank_policy() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "segment_preds": ["[]", "[]"],
            "RFV1_name": ["", ""],
            "cc_missing_reason": ["true_missing", "valid"],
            "cc_pseudomissing_flag": [False, False],
            "cc_missing_flag": [True, False],
        }
    )
    report = validate_classifier_contract(df)
    codes = {item["code"] for item in report["findings"]}

    assert report["status"] == "fail"
    assert "unexpected_blank_rfv1" in codes


def test_validate_classifier_contract_derives_pseudo_count_without_optional_flags() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "segment_preds": ["[]", "[]", "[]"],
            "RFV1_name": [
                "Symptom – Respiratory",
                "Uncodable/Unknown",
                "Symptom – Respiratory",
            ],
            "cc_missing_reason": ["valid", "pseudo_missing_token", "valid"],
        }
    )
    report = validate_classifier_contract(df)
    assert report["status"] == "pass"
    assert report["pseudo_missing_rows"] == 1


def test_verify_classifier_resources_checks_required_and_optional_hash(tmp_path: Path) -> None:
    annotation_dir = tmp_path / "Annotation"
    annotation_dir.mkdir(parents=True)
    appendix = annotation_dir / "nhamcs_rvc_2022_appendixII_codes.csv"
    summary = annotation_dir / "nhamcs_rvc_2022_summary_by_top_level_17.csv"
    appendix.write_text("a,b\n1,2\n")
    summary.write_text("a,b\n1,2\n")

    manifest = annotation_dir / "resource_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "resources": [
                    {
                        "path": "Annotation/nhamcs_rvc_2022_appendixII_codes.csv",
                        "sha256": "deadbeef",
                    }
                ]
            }
        )
    )

    strict_report = verify_classifier_resources(
        tmp_path,
        appendix_relpath="Annotation/nhamcs_rvc_2022_appendixII_codes.csv",
        summary_relpath="Annotation/nhamcs_rvc_2022_summary_by_top_level_17.csv",
        manifest_path="Annotation/resource_manifest.json",
        strict_hash=True,
    )
    assert strict_report["status"] == "fail"

    warn_report = verify_classifier_resources(
        tmp_path,
        appendix_relpath="Annotation/nhamcs_rvc_2022_appendixII_codes.csv",
        summary_relpath="Annotation/nhamcs_rvc_2022_summary_by_top_level_17.csv",
        manifest_path="Annotation/resource_manifest.json",
        strict_hash=False,
    )
    assert warn_report["status"] == "warning"
