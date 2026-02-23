from __future__ import annotations

from pathlib import Path


WORK_DIR = Path(__file__).resolve().parents[1]

PIPELINE_NOTEBOOKS = [
    WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd",
    WORK_DIR / "Hypercap CC NLP Classifier.qmd",
    WORK_DIR / "Rater Agreement Analysis.qmd",
    WORK_DIR / "Hypercap CC NLP Analysis.qmd",
]

DISALLOWED_CORE_IMPORTS = (
    "hypercap_cc_nlp.cohort_quality",
    "hypercap_cc_nlp.classifier_quality",
    "hypercap_cc_nlp.analysis_core",
    "hypercap_cc_nlp.rater_core",
    "hypercap_cc_nlp.workflow_contracts",
)


def test_pipeline_notebooks_do_not_import_core_modules() -> None:
    for notebook_path in PIPELINE_NOTEBOOKS:
        text = notebook_path.read_text()
        for disallowed in DISALLOWED_CORE_IMPORTS:
            assert (
                disallowed not in text
            ), f"{notebook_path.name} imports disallowed core module {disallowed}"


def test_pipeline_notebooks_define_local_table_renderer() -> None:
    for notebook_path in PIPELINE_NOTEBOOKS:
        text = notebook_path.read_text()
        assert "def render_latex_longtable(" in text, (
            f"{notebook_path.name} missing local longtable renderer"
        )


def test_cohort_notebook_has_generation_and_qa_sections() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "## Data Generation" in cohort_text
    assert "## QA & Data Fidelity" in cohort_text


def test_cohort_notebook_contains_ed_vitals_cleaning_helpers() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "def normalize_temperature_to_f(" in cohort_text
    assert "def clean_pain_score(" in cohort_text
    assert "def clean_bp(" in cohort_text
    assert "def clean_o2sat(" in cohort_text
    assert "def build_ed_vitals_audit_artifacts(" in cohort_text
    assert "ed_vitals_distribution_summary.csv" in cohort_text
    assert "ed_vitals_extreme_examples.csv" in cohort_text
    assert "ed_vitals_model_delta.csv" in cohort_text
    assert "ed_triage_temp_f_clean" in cohort_text
    assert "ed_first_temp_f_clean" in cohort_text
    assert "ed_triage_o2sat_clean" in cohort_text
    assert "ed_first_o2sat_clean" in cohort_text
    assert "residual_celsius_like_n" in cohort_text
    assert "load_blood_gas_itemid_manifest(" in cohort_text
    assert "specs/blood_gas_itemids.json" in cohort_text
    assert "blood_gas_itemid_manifest_audit.csv" in cohort_text
    assert "pco2_source_distribution_audit.csv" in cohort_text
    assert "ALL_CANDIDATE_PCO2_LAB_POC" in cohort_text
    assert "pco2_window_max_contributor_audit.csv" in cohort_text
    assert "blood_gas_triplet_completeness_audit.csv" in cohort_text
    assert "qualifying_pco2_distribution_by_type_audit.csv" in cohort_text
    assert "other_route_quarantine_audit.csv" in cohort_text
    assert "first_gas_anchor_audit.csv" in cohort_text
    assert "pco2_itemid_qc_audit.csv" in cohort_text
    assert "timing_integrity_audit.csv" in cohort_text
    assert "ventilation_timing_audit.csv" in cohort_text
    assert "anthropometric_cleaning_audit.csv" in cohort_text
    assert "bmi_closest_pre_ed_uom" in cohort_text
    assert "height_closest_pre_ed_uom" in cohort_text
    assert "weight_closest_pre_ed_uom" in cohort_text
    assert "bmi_closest_pre_ed_time" in cohort_text
    assert "height_closest_pre_ed_time" in cohort_text
    assert "weight_closest_pre_ed_time" in cohort_text
    assert "ANTHRO_BMI_PAIR_WINDOW_HOURS" in cohort_text
    assert "source_preference=(\"ED\", \"ICU\", \"HOSPITAL\")" in cohort_text
    assert "normalize_anthro_source(" in cohort_text
    assert "first_other_src_detail" in cohort_text
    assert "first_gas_anchor_has_pco2" in cohort_text
    assert "poc_itemid_qc_passed" in cohort_text
    assert "poc_itemid_qc_reason" in cohort_text
    assert "pco2_threshold_0_24h" in cohort_text
    assert "qualifying_pco2_time" in cohort_text
    assert "qualifying_pco2_mmhg" in cohort_text
    assert "qualifying_site" in cohort_text
    assert "qualifying_source_branch" in cohort_text
    assert "qualifying_threshold_mmhg" in cohort_text
    assert "dt_qualifying_hypercapnia_hours" in cohort_text
    assert "first_abg_hypercap_time_0_24h" in cohort_text
    assert "first_vbg_hypercap_time_0_24h" in cohort_text
    assert "first_other_hypercap_time_0_24h" in cohort_text
    assert "first_abg_hypercap_pco2_mmhg" in cohort_text
    assert "first_vbg_hypercap_pco2_mmhg" in cohort_text
    assert "first_other_hypercap_pco2_mmhg" in cohort_text
    assert "first_abg_po2" in cohort_text
    assert "first_vbg_po2" in cohort_text
    assert "first_other_po2" in cohort_text
    assert "enrollment_route" in cohort_text
    assert "abg_hypercap_threshold" in cohort_text
    assert "vbg_hypercap_threshold" in cohort_text
    assert "unknown_hypercap_threshold" in cohort_text
    assert "hypercap_timing_class" in cohort_text
    assert "timing_usable_for_model" in cohort_text
    assert "contract_warning_codes" in cohort_text
    assert "contract_error_codes" in cohort_text
    assert "qa_status_final" in cohort_text
    assert "max_pco2_0_24h_lt_qualifying_n" in cohort_text
    assert "max_pco2_0_6h_lt_qualifying_n" in cohort_text


def test_analysis_notebook_contains_requested_outputs() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    assert '"unknown_hypercap_threshold"' in analysis_text
    assert '"other_hypercap_threshold"' not in analysis_text
    assert "ICD_vs_Gas_Performance.xlsx" in analysis_text
    assert "ICD_Positive_Subset_Breakdown.xlsx" in analysis_text
    assert "Ascertainment_Overlap_UpSet.png" in analysis_text
    assert "from upsetplot import UpSet, from_indicators" in analysis_text
    assert "def select_preferred_vital_column(" in analysis_text
    assert "qualifying_gas_time_observed_rate" in analysis_text
    assert "poc_itemid_qc_passed" in analysis_text
    assert "UNKNOWN semantics" in analysis_text
    assert "panel_unknown_rate" in analysis_text
    assert "encounter_unknown_rate" in analysis_text


def test_classifier_notebook_contains_spell_mode_comparison_and_audit() -> None:
    classifier_text = (WORK_DIR / "Hypercap CC NLP Classifier.qmd").read_text()
    assert "CC_SPELL_CORRECTION_MODE" in classifier_text
    assert "SPELL_CORRECTION_MODES" in classifier_text
    assert "choose_spell_mode(" in classifier_text
    assert "classifier_spell_mode_comparison.csv" in classifier_text
    assert "classifier_spellfix_log.csv" in classifier_text
    assert "classifier_spellfix_guardrail_audit.csv" in classifier_text
    assert "SPELL_PROTECT_PHRASES" in classifier_text
    assert "SPELL_DENYLIST_SUBSTITUTIONS" in classifier_text
    assert "integrity_violation_total" in classifier_text
    assert "classifier_export_drop_columns" in classifier_text
    assert '"cc_missing_flag"' in classifier_text
    assert '"cc_pseudomissing_flag"' in classifier_text


def test_rater_notebook_contains_key_inventory_and_canonical_mapping() -> None:
    rater_text = (WORK_DIR / "Rater Agreement Analysis.qmd").read_text()
    assert "R3_vs_NLP_key_inventory.csv" in rater_text
    assert "R3_vs_NLP_label_mapping_audit.csv" in rater_text
    assert "target_sample_n" in rater_text
    assert "warn_below_target_fail_on_zero" in rater_text
    assert "canonicalize_rvc_code" in rater_text


def test_cohort_notebook_requires_manifest_hco3_and_poc_fallback_guard() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "lab.get(\"po2_itemids\"" in cohort_text
    assert "icu.get(\"po2_itemids\"" in cohort_text
    assert "po2_abg_itemids" in cohort_text
    assert "po2_vbg_itemids" in cohort_text
    assert "first_abg_po2" in cohort_text
    assert "first_vbg_po2" in cohort_text
    assert "first_other_po2" in cohort_text
    assert "lab.get(\"hco3_itemids\"" in cohort_text
    assert "icu.get(\"hco3_itemids\"" in cohort_text
    assert "CO2_other" not in cohort_text or "LAB-only OTHER quarantine policy" in cohort_text
    assert "first_hco3_source" in cohort_text
    assert "poc_explicit_itemid_fallback" in cohort_text
    assert "_extract_ed_charted_anthro" in cohort_text
    assert "ed_charted_rows_input" in cohort_text


def test_cohort_notebook_uses_icd_or_gas_enrollment_and_inclusive_thresholds() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "pco2_mmhg >= 45.0" in cohort_text
    assert "pco2_mmhg >= 50.0" in cohort_text
    assert "MAX(IF(site = 'arterial', 1, 0)) AS abg_hypercap_threshold" in cohort_text
    assert "MAX(IF(site = 'venous', 1, 0)) AS vbg_hypercap_threshold" in cohort_text
    assert "MAX(IF(site = 'unknown', 1, 0)) AS unknown_hypercap_threshold" in cohort_text
    assert "hypercapnia_by_abg" not in cohort_text
    assert "hypercapnia_by_vbg" not in cohort_text
    assert "hypercapnia_by_other" not in cohort_text
    assert 'cohort_any["enrollment_route"] = np.select(' in cohort_text
    assert 'ed_df["enrollment_route"] = np.select(' in cohort_text
    assert 'final_cc["enrollment_route"] = np.select(' in cohort_text


def test_cohort_notebook_uses_unknown_fallback_naming_and_drops_legacy_flags() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "gas_source_tier_fallback_unknown_rate" in cohort_text
    assert "gas_source_tier_fallback_other_rate" not in cohort_text
    assert "poc_hypercap_0_24h_edstay_n" in cohort_text
    assert '"flag_any_gas_hypercapnia_poc"' in cohort_text
    assert '"flag_any_gas_hypercapnia"' in cohort_text


def test_cohort_notebook_drops_redundant_export_columns() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "cohort_export_drop_columns" in cohort_text
    assert '"enrolled_any"' in cohort_text
    assert '"enrolled_any_icd_union_secondary"' in cohort_text
    assert '"gas_source_unknown_rate"' in cohort_text
    assert '"gas_source_other_rate"' in cohort_text
    assert '"gas_source_inference_primary_tier"' in cohort_text
    assert '"lab_abg_po2"' in cohort_text
    assert '"poc_abg_po2"' in cohort_text
