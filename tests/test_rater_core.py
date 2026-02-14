from __future__ import annotations

import pandas as pd
import pytest

from hypercap_cc_nlp.rater_core import build_r3_nlp_join_audit, normalize_join_keys


def test_normalize_join_keys_coerces_to_nullable_int() -> None:
    raw = pd.DataFrame(
        {
            "hadm_id": ["1001", "1002", "bad"],
            "subject_id": [2001, "2002", " 2003 "],
        }
    )

    normalized = normalize_join_keys(raw, ["hadm_id", "subject_id"])

    assert normalized["hadm_id"].tolist() == [1001, 1002, pd.NA]
    assert normalized["subject_id"].tolist() == [2001, 2002, 2003]
    assert str(normalized["hadm_id"].dtype) == "Int64"
    assert str(normalized["subject_id"].dtype) == "Int64"


def test_build_r3_nlp_join_audit_reports_matches_and_unmatched() -> None:
    df_r3 = pd.DataFrame(
        {
            "hadm_id": ["1", "2", "3"],
            "subject_id": ["10", "20", "30"],
            "annot3_rvs1_cat": ["A", "B", "C"],
        }
    )
    df_nlp = pd.DataFrame(
        {
            "hadm_id": [1, 2, 4],
            "subject_id": [10, 20, 40],
            "RFV1_name": ["A", "B", "D"],
        }
    )

    matched, unmatched_adjudicated, unmatched_nlp, audit = build_r3_nlp_join_audit(
        df_r3, df_nlp, ["hadm_id", "subject_id"]
    )

    assert len(matched) == 2
    assert unmatched_adjudicated.to_dict(orient="records") == [
        {"hadm_id": 3, "subject_id": 30}
    ]
    assert unmatched_nlp.to_dict(orient="records") == [{"hadm_id": 4, "subject_id": 40}]
    assert audit["matched_rows"] == 2
    assert audit["unmatched_adjudicated_rows"] == 1
    assert audit["unmatched_nlp_rows"] == 1
    assert audit["matched_rate_vs_adjudicated"] == pytest.approx(2 / 3)
    assert audit["join_interpretation"] == "partial_adjudicated_overlap"
    assert audit["severity"] == "warning"


def test_build_r3_nlp_join_audit_raises_on_duplicate_keys() -> None:
    df_r3 = pd.DataFrame(
        {
            "hadm_id": [1, 1],
            "subject_id": [10, 10],
            "annot3_rvs1_cat": ["A", "A"],
        }
    )
    df_nlp = pd.DataFrame({"hadm_id": [1], "subject_id": [10], "RFV1_name": ["A"]})

    with pytest.raises(ValueError, match="duplicate rows"):
        build_r3_nlp_join_audit(df_r3, df_nlp, ["hadm_id", "subject_id"])


def test_build_r3_nlp_join_audit_raises_on_zero_matches() -> None:
    df_r3 = pd.DataFrame({"hadm_id": [1], "subject_id": [10], "annot3_rvs1_cat": ["A"]})
    df_nlp = pd.DataFrame({"hadm_id": [2], "subject_id": [20], "RFV1_name": ["B"]})

    with pytest.raises(ValueError, match="produced zero matched rows"):
        build_r3_nlp_join_audit(df_r3, df_nlp, ["hadm_id", "subject_id"])


def test_build_r3_nlp_join_audit_marks_full_coverage_as_info() -> None:
    df_r3 = pd.DataFrame(
        {"hadm_id": [1, 2], "subject_id": [10, 20], "annot3_rvs1_cat": ["A", "B"]}
    )
    df_nlp = pd.DataFrame(
        {"hadm_id": [1, 2, 3], "subject_id": [10, 20, 30], "RFV1_name": ["A", "B", "C"]}
    )

    _, _, _, audit = build_r3_nlp_join_audit(df_r3, df_nlp, ["hadm_id", "subject_id"])

    assert audit["matched_rate_vs_adjudicated"] == 1.0
    assert audit["join_interpretation"] == "adjudicated_fully_covered_subset"
    assert audit["severity"] == "info"
