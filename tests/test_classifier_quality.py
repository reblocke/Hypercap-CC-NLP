from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from hypercap_cc_nlp.classifier_quality import (
    add_hypercapnia_flags,
    annotate_cc_missingness,
    apply_blank_primary_uncodable_policy,
    apply_pseudomissing_uncodable_policy,
    build_cc_missing_audit,
    validate_classifier_contract,
    verify_classifier_resources,
)


def test_add_hypercapnia_flags_uses_schema_correct_defaults() -> None:
    df = pd.DataFrame(
        {
            "poc_abg_paco2": [46, 44, None],
            "poc_abg_paco2_uom": ["mmhg", "mmhg", "mmhg"],
            "poc_vbg_paco2": [None, 50, 6.7],
            "poc_vbg_paco2_uom": [None, "mmhg", "kpa"],
        }
    )
    out = add_hypercapnia_flags(df)

    assert out["hypercap_by_abg"].tolist() == [1, 0, 0]
    assert out["hypercap_by_vbg"].tolist() == [0, 1, 1]
    assert out["hypercap_by_bg"].tolist() == [1, 1, 1]


def test_add_hypercapnia_flags_prefers_authoritative_flags_over_numeric_inputs() -> None:
    df = pd.DataFrame(
        {
            "flag_abg_hypercapnia": [0, 1],
            "flag_vbg_hypercapnia": [1, 0],
            "first_abg_pco2": [80, 20],
            "first_vbg_pco2": [20, 80],
            "poc_abg_paco2": [80, 20],
            "poc_vbg_paco2": [20, 80],
        }
    )
    out = add_hypercapnia_flags(df)

    assert out["hypercap_by_abg"].tolist() == [0, 1]
    assert out["hypercap_by_vbg"].tolist() == [1, 0]
    assert out["hypercap_by_bg"].tolist() == [1, 1]


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


def test_validate_classifier_contract_reports_union_mismatch_and_blank_policy() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "segment_preds": ["[]", "[]"],
            "RFV1_name": ["", ""],
            "cc_missing_reason": ["true_missing", "valid"],
            "cc_pseudomissing_flag": [False, False],
            "cc_missing_flag": [True, False],
            "hypercap_by_abg": [1, 0],
            "hypercap_by_vbg": [0, 0],
            "hypercap_by_bg": [0, 0],
        }
    )
    report = validate_classifier_contract(df)
    codes = {item["code"] for item in report["findings"]}

    assert report["status"] == "fail"
    assert "bg_union_mismatch" in codes
    assert "unexpected_blank_rfv1" in codes


def test_validate_classifier_contract_detects_authoritative_disagreement() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "segment_preds": ["[]", "[]"],
            "RFV1_name": ["Symptom – Respiratory", "Symptom – Respiratory"],
            "cc_missing_reason": ["valid", "valid"],
            "cc_pseudomissing_flag": [False, False],
            "cc_missing_flag": [False, False],
            "hypercap_by_abg": [0, 0],
            "hypercap_by_vbg": [0, 0],
            "hypercap_by_bg": [0, 0],
            "flag_abg_hypercapnia": [1, 0],
        }
    )
    report = validate_classifier_contract(df)
    codes = {item["code"] for item in report["findings"]}
    assert "abg_authoritative_disagreement" in codes


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
