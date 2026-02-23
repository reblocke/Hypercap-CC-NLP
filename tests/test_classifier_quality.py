from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hypercap_cc_nlp.classifier_quality import (
    annotate_cc_missingness,
    apply_blank_primary_uncodable_policy,
    apply_pseudomissing_uncodable_policy,
    build_cc_missing_audit,
    validate_classifier_contract,
    verify_classifier_resources,
)


def test_annotate_cc_missingness_labels_pseudo_and_true_missing() -> None:
    df = pd.DataFrame(
        {
            "ed_triage_cc": ["-", "___", "N", "??", "shortness of breath", "", None],
        }
    )
    out = annotate_cc_missingness(df, cc_column="ed_triage_cc")

    assert out["cc_missing_reason"].tolist() == [
        "pseudo_missing_token",
        "pseudo_missing_token",
        "pseudo_missing_token",
        "pseudo_missing_token",
        "valid",
        "true_missing",
        "true_missing",
    ]
    assert out["cc_missing_flag"].tolist() == [True, True, True, True, False, True, True]
    assert out["cc_text_for_nlp"].isna().tolist() == [True, True, True, True, False, True, True]


def test_apply_pseudomissing_uncodable_policy_sets_rfv_contract_fields() -> None:
    df = pd.DataFrame(
        {
            "cc_pseudomissing_flag": [True, False],
            "RFV1": ["", "resp"],
            "RFV1_name": ["", "Symptom – Respiratory"],
            "RFV1_support": ["", "sob"],
            "RFV1_sim": [None, 0.8],
            "RFV2": ["", ""],
            "RFV2_name": ["", ""],
            "RFV2_support": ["", ""],
            "RFV2_sim": [None, None],
            "segment_preds": ["[]", "[]"],
        }
    )
    out = apply_pseudomissing_uncodable_policy(df, max_rfv=2)

    assert out.loc[0, "RFV1"] == "uncodable"
    assert out.loc[0, "RFV1_name"] == "Uncodable/Unknown"
    payload = json.loads(out.loc[0, "segment_preds"])
    assert payload[0]["code"] == "RVC-UNCL"
    assert out.loc[1, "RFV1"] == "resp"


def test_apply_blank_primary_uncodable_policy_backfills_nonmissing_blanks() -> None:
    df = pd.DataFrame(
        {
            "cc_missing_reason": ["valid", "true_missing", "valid"],
            "RFV1": ["", "", "resp"],
            "RFV1_name": ["", "", "Symptom – Respiratory"],
            "RFV1_support": ["", "", "sob"],
            "RFV1_sim": [None, None, 0.8],
            "segment_preds": ["[]", "[]", "[]"],
        }
    )
    out = apply_blank_primary_uncodable_policy(df, max_rfv=2)

    assert out.loc[0, "RFV1"] == "uncodable"
    assert out.loc[0, "RFV1_name"] == "Uncodable/Unknown"
    assert json.loads(out.loc[0, "segment_preds"])[0]["rule_code"] == "blank_primary_fallback"
    assert out.loc[1, "RFV1_name"] == ""
    assert out.loc[2, "RFV1"] == "resp"


def test_build_cc_missing_audit_summarizes_examples() -> None:
    df = pd.DataFrame(
        {
            "ed_triage_cc": ["-", "-", "cough", None],
            "cc_missing_reason": [
                "pseudo_missing_token",
                "pseudo_missing_token",
                "valid",
                "true_missing",
            ],
        }
    )
    audit = build_cc_missing_audit(df, cc_column="ed_triage_cc")
    got = audit.set_index("cc_missing_reason")
    assert int(got.loc["pseudo_missing_token", "row_count"]) == 2
    assert "cough" in str(got.loc["valid", "examples"])


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


def test_validate_classifier_contract_passes_without_hypercap_duplicate_columns() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "segment_preds": ["[]", "[]"],
            "RFV1_name": ["Symptom – Respiratory", "Symptom – Respiratory"],
            "cc_missing_reason": ["valid", "valid"],
        }
    )
    report = validate_classifier_contract(df)
    assert report["status"] == "pass"


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
