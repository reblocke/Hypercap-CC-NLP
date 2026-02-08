from __future__ import annotations

import pandas as pd

from hypercap_cc_nlp.cohort_quality import (
    attach_closest_pre_ed_omr,
    classify_missingness_expectations,
    evaluate_uom_expectations,
    prepare_omr_records,
)


def test_prepare_omr_records_normalizes_and_filters_rows() -> None:
    omr_raw = pd.DataFrame(
        {
            "subject_id": ["1", "2", "bad", "3", "4"],
            "chartdate": [
                "2026-01-10",
                "2026-01-09",
                "2026-01-08",
                "bad-date",
                "2026-01-01",
            ],
            "result_name": ["BMI", "height", "weight", "weight", "pulse"],
            "result_value": ["30.1 kg/m2", "170 cm", "80", "75", "55"],
        }
    )

    prepared = prepare_omr_records(omr_raw)

    assert prepared["result_name"].tolist() == ["bmi", "height"]
    assert prepared["subject_id"].tolist() == [1, 2]
    assert prepared["result_value_num"].tolist() == [30.1, 170.0]


def test_attach_closest_pre_ed_omr_respects_window_and_direction() -> None:
    ed_df = pd.DataFrame(
        {
            "ed_stay_id": [1, 2, 3],
            "subject_id": [101, 102, 103],
            "ed_intime": [
                "2026-02-10 08:00:00",
                "2026-02-10 08:00:00",
                "2026-02-10 08:00:00",
            ],
        }
    )
    omr_raw = pd.DataFrame(
        {
            "subject_id": ["101", "101", "102", "103"],
            "chartdate": ["2026-02-09", "2024-01-01", "2026-02-11", "2025-01-01"],
            "result_name": ["weight", "weight", "height", "bmi"],
            "result_value": ["80", "70", "170", "31"],
        }
    )

    prepared = prepare_omr_records(omr_raw)
    attached, diagnostics = attach_closest_pre_ed_omr(ed_df, prepared, window_days=365)

    assert attached.loc[attached["ed_stay_id"] == 1, "weight_closest_pre_ed"].iat[0] == 80.0
    assert pd.isna(attached.loc[attached["ed_stay_id"] == 2, "height_closest_pre_ed"].iat[0])
    assert pd.isna(attached.loc[attached["ed_stay_id"] == 3, "bmi_closest_pre_ed"].iat[0])
    assert diagnostics["within_window_candidate_rows"] == 1
    assert diagnostics["nonnegative_candidate_rows"] == 3
    assert diagnostics["days_before_min"] == -1
    assert diagnostics["days_before_max"] == 771
    assert diagnostics["attached_non_null_counts"]["weight_closest_pre_ed"] == 1


def test_attach_closest_pre_ed_omr_reports_zero_overlap() -> None:
    ed_df = pd.DataFrame(
        {
            "ed_stay_id": [11],
            "subject_id": [5001],
            "ed_intime": ["2026-02-10 08:00:00"],
        }
    )
    omr_raw = pd.DataFrame(
        {
            "subject_id": [9001],
            "chartdate": ["2026-02-01"],
            "result_name": ["bmi"],
            "result_value": ["29.5"],
        }
    )

    prepared = prepare_omr_records(omr_raw)
    attached, diagnostics = attach_closest_pre_ed_omr(ed_df, prepared)

    assert diagnostics["subject_overlap_count"] == 0
    assert diagnostics["within_window_candidate_rows"] == 0
    assert diagnostics["days_before_min"] is None
    assert diagnostics["days_before_max"] is None
    assert attached["bmi_closest_pre_ed"].isna().all()
    assert attached["height_closest_pre_ed"].isna().all()
    assert attached["weight_closest_pre_ed"].isna().all()


def test_evaluate_uom_expectations_flags_mismatch_patterns() -> None:
    ed_df = pd.DataFrame(
        {
            "poc_abg_ph_uom": [pd.NA, pd.NA],
            "poc_vbg_ph_uom": [pd.NA, pd.NA],
            "poc_other_ph_uom": [pd.NA, pd.NA],
            "poc_abg_paco2": [50.0, pd.NA],
            "poc_abg_paco2_uom": ["mmhg", pd.NA],
            "poc_vbg_paco2": [55.0, 60.0],
            "poc_vbg_paco2_uom": ["kpa", "mmhg"],
            "poc_other_paco2": [pd.NA, 70.0],
            "poc_other_paco2_uom": [pd.NA, pd.NA],
        }
    )

    result = evaluate_uom_expectations(ed_df)

    assert result["structural_null_checks"]["poc_abg_ph_uom"]["all_null"] is True
    assert (
        result["paco2_uom_checks"]["poc_abg_paco2_uom"]["missing_uom_when_value_present"]
        == 0
    )
    assert (
        result["paco2_uom_checks"]["poc_vbg_paco2_uom"]["non_mmhg_uom_when_value_present"]
        == 1
    )
    assert (
        result["paco2_uom_checks"]["poc_other_paco2_uom"]["missing_uom_when_value_present"]
        == 1
    )


def test_classify_missingness_expectations_applies_expected_categories() -> None:
    ed_df = pd.DataFrame(
        {
            "complete": [1, 2, 3],
            "sparse": [1.0, pd.NA, 2.0],
            "all_null": [pd.NA, pd.NA, pd.NA],
            "poc_abg_ph_uom": [pd.NA, pd.NA, pd.NA],
        }
    )
    target_fields = ["complete", "sparse", "all_null", "poc_abg_ph_uom", "not_present"]

    classified = classify_missingness_expectations(ed_df, target_fields)
    got = dict(zip(classified["field"], classified["expectation"]))

    assert got["complete"] == "complete"
    assert got["sparse"] == "conditional_sparse"
    assert got["all_null"] == "unexpected_full_null"
    assert got["poc_abg_ph_uom"] == "expected_structural_null"
    assert got["not_present"] == "missing_column"
