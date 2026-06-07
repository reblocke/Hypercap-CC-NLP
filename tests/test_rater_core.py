from __future__ import annotations

import pandas as pd
import pytest

from hypercap_cc_nlp.rater_core import (
    agreement_class,
    assign_annotation_row_id,
    bootstrap_per_category_prf_cis,
    bootstrap_set_agreement_cis,
    build_annotation_direct_join,
    build_join_key_diagnostics,
    build_r3_nlp_join_audit,
    hash_join_keys,
    multilabel_confusion_matrix,
    normalize_join_keys,
    per_category_prf,
    redact_chief_complaint_example,
)


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
    assert audit["human_human_denominator_n"] == 3
    assert audit["nlp_benchmark_denominator_n"] == 2
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


def test_build_join_key_diagnostics_profiles_candidate_strategies() -> None:
    df_r3 = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "subject_id": [10, 20],
            "ed_stay_id": [100, pd.NA],
        }
    )
    df_nlp = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "subject_id": [10, 20, 30],
            "ed_stay_id": [100, 200, 300],
        }
    )

    diagnostics = build_join_key_diagnostics(df_r3, df_nlp)

    assert diagnostics["join_key_strategy"].tolist() == [
        "ed_stay_id",
        "hadm_id_subject_id",
        "hadm_id",
    ]
    ed_row = diagnostics.loc[diagnostics["join_key_strategy"].eq("ed_stay_id")].iloc[0]
    hadm_subject_row = diagnostics.loc[
        diagnostics["join_key_strategy"].eq("hadm_id_subject_id")
    ].iloc[0]
    assert bool(ed_row["eligible_for_join"])
    assert bool(ed_row["selected"])
    assert ed_row["r3_missing_key_rows"] == 1
    assert bool(hadm_subject_row["eligible_for_join"])


def test_hash_join_keys_returns_only_hash_column() -> None:
    frame = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "subject_id": [10, 20],
        }
    )
    hashed = hash_join_keys(frame, key_cols=["hadm_id", "subject_id"])

    assert list(hashed.columns) == ["key_hash"]
    assert len(hashed) == 2
    assert hashed["key_hash"].str.len().eq(64).all()


def test_assign_annotation_row_id_is_deterministic_one_based() -> None:
    frame = pd.DataFrame({"chief_complaint": ["sob", "chest pain", "fall"]})

    keyed = assign_annotation_row_id(frame)

    assert keyed["annotation_row_id"].tolist() == [1, 2, 3]
    assert frame.columns.tolist() == ["chief_complaint"]


def test_build_annotation_direct_join_uses_row_id_only() -> None:
    df_r3 = assign_annotation_row_id(
        pd.DataFrame(
            {
                "subject_id": [10, 20],
                "annot3_rvs1_cat": ["RVC-SYM-RESP", "RVC-SYM-CIRC"],
            }
        )
    )
    df_nlp = pd.DataFrame(
        {
            "annotation_row_id": [1, 2, 3],
            "RFV1_name": ["Symptom – Respiratory", "Symptom – Circulatory", "Injuries"],
        }
    )

    matched, unmatched_adjudicated, unmatched_nlp, audit = build_annotation_direct_join(
        df_r3, df_nlp
    )

    assert matched["annotation_row_id"].tolist() == [1, 2]
    assert unmatched_adjudicated.empty
    assert unmatched_nlp.to_dict(orient="records") == [{"annotation_row_id": 3}]
    assert audit["benchmark_source"] == "annotation_direct"
    assert audit["key_strategy"] == "annotation_row_id"
    assert audit["nlp_benchmark_denominator_n"] == 2


def test_agreement_class_exact_partial_none() -> None:
    assert agreement_class({"RVC-SYM-RESP"}, {"RVC-SYM-RESP"}) == "exact"
    assert agreement_class({"RVC-SYM-RESP", "RVC-SYM-CIRC"}, {"RVC-SYM-RESP"}) == "partial"
    assert agreement_class({"RVC-SYM-RESP"}, {"RVC-SYM-CIRC"}) == "none"
    assert agreement_class(set(), set()) == "exact"


def test_per_category_prf_handles_zero_support_and_zero_predicted() -> None:
    categories = ["A", "B", "C"]
    gold_sets = [{"A"}, {"A", "B"}, set()]
    pred_sets = [{"A"}, {"C"}, set()]

    metrics = per_category_prf(gold_sets, pred_sets, categories).set_index("category_code")

    assert metrics.loc["A", "adjudicated_support"] == 2
    assert metrics.loc["A", "nlp_positives"] == 1
    assert metrics.loc["A", "tp"] == 1
    assert metrics.loc["A", "fn"] == 1
    assert metrics.loc["A", "precision"] == pytest.approx(1.0)
    assert metrics.loc["A", "recall"] == pytest.approx(0.5)
    assert pd.isna(metrics.loc["B", "precision"])
    assert metrics.loc["B", "recall"] == pytest.approx(0.0)
    assert pd.isna(metrics.loc["C", "recall"])
    assert metrics.loc["C", "precision"] == pytest.approx(0.0)


def test_bootstrap_cis_are_deterministic_with_fixed_seed() -> None:
    categories = ["A", "B"]
    gold_sets = [{"A"}, {"A", "B"}, {"B"}, set()]
    pred_sets = [{"A"}, {"B"}, {"B"}, {"A"}]

    first_prf = bootstrap_per_category_prf_cis(
        gold_sets, pred_sets, categories, n_boot=50, seed=20260607
    )
    second_prf = bootstrap_per_category_prf_cis(
        gold_sets, pred_sets, categories, n_boot=50, seed=20260607
    )
    first_set = bootstrap_set_agreement_cis(gold_sets, pred_sets, n_boot=50, seed=20260607)
    second_set = bootstrap_set_agreement_cis(gold_sets, pred_sets, n_boot=50, seed=20260607)

    pd.testing.assert_frame_equal(first_prf, second_prf)
    pd.testing.assert_frame_equal(first_set, second_set)
    assert set(first_set["metric"]) == {"exact_rate", "partial_rate", "none_rate"}


def test_multilabel_confusion_matrix_counts_label_cooccurrence() -> None:
    categories = ["A", "B"]
    gold_sets = [{"A"}, {"A", "B"}, set()]
    pred_sets = [{"A", "B"}, {"B"}, set()]

    matrix = multilabel_confusion_matrix(gold_sets, pred_sets, categories).set_index(
        "adjudicated_category"
    )

    assert matrix.loc["A", "A"] == 1
    assert matrix.loc["A", "B"] == 2
    assert matrix.loc["B", "B"] == 1
    assert matrix.loc["__none__", "__none__"] == 1


def test_redact_chief_complaint_example_removes_dates_numbers_and_mimic_brackets() -> None:
    redacted = redact_chief_complaint_example(
        "Seen on 12/25/2120 for [**Known lastname 123**] room 4567 with SOB"
    )

    assert "[REDACTED]" in redacted
    assert "[DATE]" in redacted
    assert "[NUMBER]" in redacted
    assert "4567" not in redacted
