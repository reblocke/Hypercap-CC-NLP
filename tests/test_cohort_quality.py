from __future__ import annotations

import pandas as pd
import pytest

from hypercap_cc_nlp.cohort_quality import (
    add_gas_model_fields,
    add_vitals_model_fields,
    attach_charted_anthro_fallback,
    attach_closest_pre_ed_omr,
    assert_gas_source_coverage,
    build_anthro_coverage_audit,
    build_first_other_pco2_audit,
    build_gas_source_overlap_summary,
    classify_missingness_expectations,
    evaluate_uom_expectations,
    infer_panel_gas_source_metadata,
    prepare_omr_records,
    summarize_gas_source,
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
            "subject_id": ["101", "101", "101", "102", "103"],
            "chartdate": [
                "2026-02-09",
                "2024-01-01",
                "2026-02-11",
                "2026-02-11",
                "2025-01-01",
            ],
            "result_name": ["weight", "weight", "weight", "height", "bmi"],
            "result_value": ["80", "70", "90", "170", "31"],
        }
    )

    prepared = prepare_omr_records(omr_raw)
    attached, diagnostics = attach_closest_pre_ed_omr(ed_df, prepared, window_days=365)

    assert attached.loc[attached["ed_stay_id"] == 1, "weight_closest_pre_ed"].iat[0] == 80.0
    assert attached.loc[attached["ed_stay_id"] == 2, "height_closest_pre_ed"].iat[0] == 170.0
    assert pd.isna(attached.loc[attached["ed_stay_id"] == 3, "bmi_closest_pre_ed"].iat[0])

    assert attached.loc[attached["ed_stay_id"] == 1, "anthro_timing_tier"].iat[0] == "pre_ed_365"
    assert attached.loc[attached["ed_stay_id"] == 2, "anthro_timing_tier"].iat[0] == "post_ed_365"
    assert attached.loc[attached["ed_stay_id"] == 3, "anthro_timing_tier"].iat[0] == "missing"
    assert attached.loc[attached["ed_stay_id"] == 1, "anthro_days_offset"].iat[0] == 1
    assert attached.loc[attached["ed_stay_id"] == 2, "anthro_days_offset"].iat[0] == -1
    assert pd.isna(attached.loc[attached["ed_stay_id"] == 3, "anthro_days_offset"].iat[0])
    assert (
        bool(attached.loc[attached["ed_stay_id"] == 1, "anthro_timing_uncertain"].iat[0])
        is False
    )
    assert (
        bool(attached.loc[attached["ed_stay_id"] == 2, "anthro_timing_uncertain"].iat[0])
        is True
    )
    assert pd.isna(attached.loc[attached["ed_stay_id"] == 3, "anthro_timing_uncertain"].iat[0])

    assert diagnostics["within_window_candidate_rows"] == 1
    assert diagnostics["nonnegative_candidate_rows"] == 3
    assert diagnostics["pre_window_candidate_rows"] == 1
    assert diagnostics["post_window_candidate_rows"] == 2
    assert diagnostics["closest_absolute_candidate_rows"] == 3
    assert diagnostics["days_before_min"] == -1
    assert diagnostics["days_before_max"] == 771
    assert diagnostics["attached_non_null_counts"]["weight_closest_pre_ed"] == 1
    assert diagnostics["attached_non_null_counts"]["height_closest_pre_ed"] == 1
    assert diagnostics["attached_any_non_null_rows"] == 2
    assert diagnostics["selected_tier_counts"] == {
        "pre_ed_365": 1,
        "post_ed_365": 1,
        "missing": 1,
    }
    assert diagnostics["timing_uncertain_count"] == 1


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
    assert attached["anthro_timing_tier"].eq("missing").all()
    assert attached["anthro_timing_uncertain"].isna().all()
    assert diagnostics["selected_tier_counts"] == {
        "pre_ed_365": 0,
        "post_ed_365": 0,
        "missing": 1,
    }


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


def test_classify_missingness_expectations_supports_expected_sparse_fields() -> None:
    ed_df = pd.DataFrame({"omr_height": [pd.NA, pd.NA, pd.NA]})

    classified = classify_missingness_expectations(
        ed_df,
        ["omr_height"],
        expected_sparse_fields={"omr_height"},
    )
    got = dict(zip(classified["field"], classified["expectation"]))
    assert got["omr_height"] == "expected_sparse"


def test_summarize_gas_source_detects_all_other_and_guard_raises() -> None:
    panel_df = pd.DataFrame({"source": ["other", "other", "other"]})
    audit = summarize_gas_source(panel_df)

    assert audit["all_other_or_unknown"] is True
    with pytest.raises(ValueError, match="classified all panel rows as other/unknown"):
        assert_gas_source_coverage(audit)


def test_summarize_gas_source_reports_mixed_sources() -> None:
    panel_df = pd.DataFrame(
        {
            "source": ["arterial", "venous", "other", "arterial"],
            "source_inference_tier": [
                "label_fluid",
                "label_fluid",
                "fallback_other",
                "specimen_text",
            ],
        }
    )
    audit = summarize_gas_source(panel_df)

    assert audit["all_other_or_unknown"] is False
    assert audit["source_counts"]["arterial"] == 2
    assert audit["tier_counts"]["label_fluid"] == 2
    assert_gas_source_coverage(audit)


def test_infer_panel_gas_source_metadata_uses_tiered_hints() -> None:
    panel_df = pd.DataFrame(
        {
            "ed_stay_id": [1, 1, 2, 3, 4],
            "specimen_id": [10, 11, 20, 30, 40],
            "pco2": [55.0, 52.0, 60.0, 48.0, 49.0],
        }
    )
    labs_df = pd.DataFrame(
        {
            "ed_stay_id": [1, 1, 2, 3, 4],
            "specimen_id": [10, 11, 20, 30, 40],
            "itemid": [52033, 50818, 59999, 50818, 59999],
            "value_text": ["Arterial sample", "", "venous draw", "unknown", ""],
        }
    )
    labitems_df = pd.DataFrame(
        {
            "itemid": [52033, 50818],
            "label": ["Specimen Type", "pCO2 VBG"],
            "fluid": ["Blood Gas", "Blood Gas"],
        }
    )

    updated, diagnostics = infer_panel_gas_source_metadata(
        panel_df,
        labs_df,
        labitems_df,
        specimen_source_itemids=[52033],
        pco2_itemids=[50818],
        mode="metadata_only",
    )

    by_specimen = updated.set_index("specimen_id")
    assert by_specimen.loc[10, "source"] == "arterial"
    assert by_specimen.loc[10, "source_inference_tier"] == "specimen_text"
    assert by_specimen.loc[11, "source"] == "venous"
    assert by_specimen.loc[11, "source_inference_tier"] == "label_fluid"
    assert by_specimen.loc[20, "source"] == "venous"
    assert by_specimen.loc[20, "source_inference_tier"] == "panel_cooccurrence"
    assert by_specimen.loc[40, "source"] == "other"
    assert by_specimen.loc[40, "source_inference_tier"] == "fallback_other"
    assert diagnostics["tier_counts"]["fallback_other"] == 1
    assert diagnostics["source_counts"]["other"] == 1


def test_build_first_other_pco2_audit_is_route_stratified() -> None:
    ed_df = pd.DataFrame(
        {
            "first_other_src": ["LAB", "LAB", "POC", "POC", "POC"],
            "first_other_pco2": [40.0, 45.0, 90.0, 160.0, 160.0],
        }
    )
    audit = build_first_other_pco2_audit(ed_df)
    got = audit.set_index("source")

    assert int(got.loc["LAB", "count_nonnull"]) == 2
    assert int(got.loc["POC", "count_nonnull"]) == 3
    assert got.loc["POC", "pct_eq_160"] == pytest.approx(2 / 3)
    assert got.loc["LAB", "status"] == "ok"
    assert got.loc["POC", "status"] == "ok"


def test_build_first_other_pco2_audit_returns_sentinel_when_columns_missing() -> None:
    audit = build_first_other_pco2_audit(pd.DataFrame({"unrelated": [1, 2]}))

    assert audit.shape[0] == 1
    row = audit.iloc[0]
    assert row["source"] == "UNAVAILABLE"
    assert row["status"] == "missing_columns"
    assert row["missing_columns"] == "first_other_pco2,first_other_src"


def test_add_vitals_model_fields_preserves_raw_and_nulls_outliers() -> None:
    ed_df = pd.DataFrame(
        {
            "ed_first_hr": [88.0, 500.0],
            "ed_first_sbp": [120.0, 2.0],
        }
    )
    updated, audit = add_vitals_model_fields(
        ed_df,
        ranges={"ed_first_hr": (20.0, 250.0), "ed_first_sbp": (40.0, 300.0)},
    )

    assert updated["ed_first_hr"].tolist() == [88.0, 500.0]
    assert updated["ed_first_hr_model"].iat[0] == 88.0
    assert pd.isna(updated["ed_first_hr_model"].iat[1])
    assert bool(updated["ed_first_hr_outlier_flag"].iat[0]) is False
    assert bool(updated["ed_first_hr_outlier_flag"].iat[1]) is True
    assert updated["ed_first_sbp_model"].iat[0] == 120.0
    assert pd.isna(updated["ed_first_sbp_model"].iat[1])
    assert bool(updated["ed_first_sbp_outlier_flag"].iat[1]) is True
    by_col = audit.set_index("raw_column")
    assert int(by_col.loc["ed_first_hr", "out_of_range_n"]) == 1
    assert int(by_col.loc["ed_first_sbp", "out_of_range_n"]) == 1
    assert by_col.loc["ed_first_hr", "outlier_flag_column"] == "ed_first_hr_outlier_flag"
    assert "500.0" in str(by_col.loc["ed_first_hr", "example_outlier_values"])


def test_add_vitals_model_fields_adds_explicit_temperature_units() -> None:
    ed_df = pd.DataFrame({"ed_first_temp": [98.6, 120.0]})
    updated, _ = add_vitals_model_fields(ed_df, ranges={"ed_first_temp": (90.0, 110.0)})

    assert "ed_first_temp_model" in updated.columns
    assert "ed_first_temp_f_model" in updated.columns
    assert "ed_first_temp_c_model" in updated.columns
    assert updated["ed_first_temp_f_model"].iat[0] == pytest.approx(98.6)
    assert updated["ed_first_temp_c_model"].iat[0] == pytest.approx(37.0, abs=0.2)
    assert pd.isna(updated["ed_first_temp_model"].iat[1])


def test_add_gas_model_fields_nulls_outliers() -> None:
    ed_df = pd.DataFrame({"first_ph": [7.2, 9.1], "first_pco2": [40.0, 1000.0]})
    updated, audit = add_gas_model_fields(ed_df, ranges={"first_ph": (6.8, 7.8), "first_pco2": (10.0, 200.0)})

    assert updated["first_ph_model"].iat[0] == pytest.approx(7.2)
    assert pd.isna(updated["first_ph_model"].iat[1])
    assert bool(updated["first_ph_outlier_flag"].iat[0]) is False
    assert bool(updated["first_ph_outlier_flag"].iat[1]) is True
    assert updated["first_pco2_model"].iat[0] == pytest.approx(40.0)
    assert pd.isna(updated["first_pco2_model"].iat[1])
    assert bool(updated["first_pco2_outlier_flag"].iat[1]) is True
    by_col = audit.set_index("raw_column")
    assert int(by_col.loc["first_ph", "out_of_range_n"]) == 1
    assert int(by_col.loc["first_pco2", "out_of_range_n"]) == 1
    assert by_col.loc["first_pco2", "outlier_flag_column"] == "first_pco2_outlier_flag"


def test_attach_charted_anthro_fallback_fills_missing_and_sets_provenance() -> None:
    ed_df = pd.DataFrame(
        {
            "ed_stay_id": [1, 2],
            "subject_id": [101, 102],
            "ed_intime": ["2026-02-10 08:00:00", "2026-02-10 08:00:00"],
            "bmi_closest_pre_ed": [pd.NA, pd.NA],
            "height_closest_pre_ed": [pd.NA, pd.NA],
            "weight_closest_pre_ed": [pd.NA, pd.NA],
        }
    )
    charted = pd.DataFrame(
        {
            "subject_id": [101, 101, 102],
            "obs_time": ["2026-02-11 08:00:00", "2026-02-11 09:00:00", "2026-02-09 08:00:00"],
            "result_name": ["weight", "height", "weight"],
            "result_value_num": [80.0, 170.0, 75.0],
            "source": ["icu_charted", "icu_charted", "icu_charted"],
        }
    )

    updated, diagnostics = attach_charted_anthro_fallback(ed_df, charted)
    assert int(diagnostics["rows_with_any_charted_fill"]) == 2
    assert updated["weight_closest_pre_ed"].notna().sum() == 2
    assert updated["height_closest_pre_ed"].notna().sum() == 1
    assert (updated["anthro_timing_basis"] == "nearest_anytime").sum() >= 1
    assert (updated["anthro_source"] == "icu_charted").sum() >= 1


def test_build_anthro_coverage_audit_reports_sources() -> None:
    ed_df = pd.DataFrame(
        {
            "bmi_closest_pre_ed": [30.0, pd.NA],
            "height_closest_pre_ed": [170.0, pd.NA],
            "weight_closest_pre_ed": [80.0, 70.0],
            "anthro_source": ["omr", "icu_charted"],
            "anthro_timing_basis": ["pre", "nearest_anytime"],
        }
    )
    audit = build_anthro_coverage_audit(ed_df)
    assert audit["field_nonnull_counts"]["weight_closest_pre_ed"] == 2
    assert audit["source_counts"]["omr"] == 1
    assert audit["source_counts"]["icu_charted"] == 1


def test_build_gas_source_overlap_summary_counts_other() -> None:
    ed_df = pd.DataFrame(
        {
            "flag_abg_hypercapnia": [1, 0, 0],
            "flag_vbg_hypercapnia": [0, 1, 0],
            "flag_other_hypercapnia": [0, 1, 1],
        }
    )
    summary = build_gas_source_overlap_summary(ed_df)
    counts = dict(zip(summary["gas_overlap"], summary["count"]))
    assert counts["ABG"] == 1
    assert counts["VBG+OTHER"] == 1
    assert counts["OTHER"] == 1
