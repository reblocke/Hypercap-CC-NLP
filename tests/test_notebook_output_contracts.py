from __future__ import annotations

import csv
import hashlib
import re
import subprocess
from pathlib import Path

import yaml

WORK_DIR = Path(__file__).resolve().parents[1]

PIPELINE_NOTEBOOKS = [
    WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd",
    WORK_DIR / "Hypercap CC NLP Classifier.qmd",
    WORK_DIR / "Rater Agreement Analysis.qmd",
    WORK_DIR / "Hypercap CC NLP Analysis.qmd",
]

RENDERABLE_ANALYSIS_NOTEBOOKS = PIPELINE_NOTEBOOKS

DISALLOWED_RUNTIME_IMPORT_TOKENS = (
    "SRC_DIR = WORK_DIR / \"src\"",
    "sys.path.insert(0, str(SRC_DIR))",
    "from hypercap_cc_nlp.",
    "import hypercap_cc_nlp",
)


def test_renderable_notebooks_do_not_import_repo_local_runtime_modules() -> None:
    for notebook_path in RENDERABLE_ANALYSIS_NOTEBOOKS:
        text = notebook_path.read_text()
        for disallowed in DISALLOWED_RUNTIME_IMPORT_TOKENS:
            assert (
                disallowed not in text
            ), f"{notebook_path.name} contains disallowed runtime token {disallowed}"


def test_pipeline_notebooks_define_local_table_renderer() -> None:
    for notebook_path in PIPELINE_NOTEBOOKS:
        text = notebook_path.read_text()
        assert "def render_latex_longtable(" in text, (
            f"{notebook_path.name} missing local longtable renderer"
        )


def test_docs_require_notebook_self_containment() -> None:
    agents_text = (WORK_DIR / "AGENTS.md").read_text()
    spec_text = (WORK_DIR / "docs" / "SPEC.md").read_text()
    decisions_text = (WORK_DIR / "docs" / "DECISIONS.md").read_text()

    assert "must run without runtime imports from `src/`" in agents_text
    assert "self-contained at runtime" in agents_text
    assert "Renderable notebooks must be self-contained at runtime" in spec_text
    assert "execution-critical helpers must be defined in clearly labeled `Local helper functions` sections" in spec_text
    assert "must not rely on runtime imports from `src/`" in decisions_text


def test_repository_docs_define_the_current_authority_split() -> None:
    agents_text = (WORK_DIR / "AGENTS.md").read_text()
    readme_text = (WORK_DIR / "README.md").read_text()
    spec_path = WORK_DIR / "docs" / "SPEC.md"

    assert spec_path.exists()
    assert "`README.md` = onboarding/runbook" in agents_text
    assert "`docs/SPEC.md` = current normative contract" in agents_text
    assert "`docs/DECISIONS.md` = dated rationale/history" in agents_text
    assert "[`docs/SPEC.md`](docs/SPEC.md) is the current pipeline contract" in readme_text


def test_spec_describes_the_live_output_contract() -> None:
    spec_text = (WORK_DIR / "docs" / "SPEC.md").read_text()

    assert "Results/YYYY-MM-DD/" in spec_text
    assert "artifacts/qa/cohort/" in spec_text
    assert "artifacts/qa/rater_agreement/" in spec_text
    assert "artifacts/qa/analysis/" in spec_text
    assert "artifacts/qa/baselines/" in spec_text
    assert "`Drafts/` is manual-only working space" in spec_text
    assert "MIMIC tabular data/MIMICIV all with CC.xlsx" in spec_text
    assert "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" in spec_text
    assert "MIMIC tabular data/annotation_benchmark_with_NLP.xlsx" in spec_text
    assert "CLASSIFIER_ANNOTATION_BENCHMARK_MODE" in spec_text
    assert "RATER_BENCHMARK_SOURCE" in spec_text
    assert "artifacts/qa/cohort/gas_source_diagnostics_by_ed_stay.csv" in spec_text
    assert "committed `analysis_manifest.yml` freezes definition-only manuscript rules" in spec_text
    assert "`Results/YYYY-MM-DD/submission_manifest.xlsx`, `submission_manifest.csv`, and `OUTPUTS_README.md`" in spec_text
    assert "`Supplementary_Table_Acid_Base_Source_Missingness.xlsx`" in spec_text
    assert "`Candidate_Definition_Yield_Composition.xlsx`" in spec_text
    assert "`Sensitivity_Analysis_Suite.xlsx`" in spec_text
    assert "Submission-facing aggregate outputs must not include `subject_id`, `hadm_id`, `ed_stay_id`" in spec_text
    assert "Frozen submission pH bands are `<7.20`, `7.20-7.24`, `7.25-7.29`" in spec_text
    assert "Frozen submission bicarbonate bands are `<22`, `22-27`, `28-33`, and `>=34`" in spec_text


def test_readme_defers_contract_heavy_rules_to_spec() -> None:
    readme_text = (WORK_DIR / "README.md").read_text()
    spec_text = (WORK_DIR / "docs" / "SPEC.md").read_text()

    assert "[`docs/SPEC.md`](docs/SPEC.md)" in readme_text
    assert "Blood-gas item selection is versioned in `specs/blood_gas_itemids.json`" not in readme_text
    assert "The cohort-stage blood-gas item selection is versioned in `specs/blood_gas_itemids.json`." in spec_text
    assert "COHORT_POC_PCO2_MEDIAN_MIN = 45" not in readme_text
    assert "Stage-Owned Invariants" in spec_text


def test_notebooks_define_local_manifest_and_multilabel_helpers() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    classifier_text = (WORK_DIR / "Hypercap CC NLP Classifier.qmd").read_text()
    rater_text = (WORK_DIR / "Rater Agreement Analysis.qmd").read_text()
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    for text in (cohort_text, classifier_text, rater_text, analysis_text):
        assert "def collect_run_manifest(" in text
        assert "def sanitize_manifest_payload(" in text
    assert "def validate_cohort_contract(" in cohort_text
    assert "def write_contract_report(" in cohort_text
    assert "def write_contract_report(" in classifier_text
    assert "def build_segment_candidate_scores(" in classifier_text
    assert "def aggregate_visit_candidate_scores(" in classifier_text
    assert "def write_classifier_candidate_sidecars(" in classifier_text
    assert "def get_rfv_name_cols(" in analysis_text
    assert "def build_rfv_membership_long(" in analysis_text
    assert "def summarize_multilabel_prevalence(" in analysis_text
    assert "def summarize_rfv_prevalence_by_comorbidity(" in analysis_text


def test_cohort_notebook_has_generation_and_qa_sections() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "## Data Generation" in cohort_text
    assert "## QA & Data Fidelity" in cohort_text


def test_cohort_notebook_contains_ed_vitals_cleaning_helpers() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "def normalize_temperature_to_f(" in cohort_text
    assert "def clean_pain_score(" in cohort_text
    assert "pain_parsed_from_fraction" in cohort_text
    assert "pain_parsed_from_text_numeric" in cohort_text
    assert "pain_non_numeric_set_na" in cohort_text
    assert "def clean_bp(" in cohort_text
    assert "def clean_o2sat(" in cohort_text
    assert "chief_complaint_inclusion_mask(" in cohort_text
    assert "canonicalize_cc_for_inclusion(" in cohort_text
    assert "def build_ed_vitals_audit_artifacts(" in cohort_text
    assert "build_vitals_outlier_phase_audit(" in cohort_text
    assert "ed_vitals_distribution_summary.csv" in cohort_text
    assert "ed_vitals_extreme_examples.csv" in cohort_text
    assert "ed_vitals_model_delta.csv" in cohort_text
    assert "vitals_outlier_audit_raw_pre_clean.csv" in cohort_text
    assert "vitals_outlier_audit_clean_post_clean.csv" in cohort_text
    assert "qa_summary_ed_spine.json" in cohort_text
    assert "qa_summary_ed_cc.json" in cohort_text
    assert "ed_triage_temp_f_clean" in cohort_text
    assert "ed_first_temp_f_clean" in cohort_text
    assert "ed_triage_o2sat_clean" in cohort_text
    assert "ed_first_o2sat_clean" in cohort_text
    assert "residual_celsius_like_n" in cohort_text
    assert "load_blood_gas_itemid_manifest(" in cohort_text
    assert "specs/blood_gas_itemids.json" in cohort_text
    assert "blood_gas_itemid_manifest_audit.csv" in cohort_text
    assert "pco2_source_distribution_audit.csv" in cohort_text
    assert "gas_source_diagnostics_by_ed_stay.csv" in cohort_text
    assert "ALL_CANDIDATE_PCO2_LAB_POC" in cohort_text
    assert "pco2_window_max_contributor_audit.csv" in cohort_text
    assert "blood_gas_triplet_completeness_audit.csv" in cohort_text
    assert "hco3_itemid_qc_audit.csv" in cohort_text
    assert "hco3_coverage_audit.csv" in cohort_text
    assert "qualifying_pco2_distribution_by_type_audit.csv" in cohort_text
    assert "other_route_quarantine_audit.csv" in cohort_text
    assert "first_gas_anchor_audit.csv" in cohort_text
    assert "pco2_itemid_qc_audit.csv" in cohort_text
    assert "timing_integrity_audit.csv" in cohort_text
    assert "ventilation_timing_audit.csv" in cohort_text
    assert "anthropometric_cleaning_audit.csv" in cohort_text
    assert "bmi_recorded_vs_computed_abs_diff_gt_5_n" in cohort_text
    assert "bmi_recorded_vs_computed_abs_diff_gt_7_5_n" in cohort_text
    assert "bmi_recorded_vs_computed_abs_diff_quantiles" in cohort_text
    assert "raw_max_mmhg" in cohort_text
    assert "clean_max_mmhg" in cohort_text
    assert "clean_p05_mmhg" in cohort_text
    assert "clean_p25_mmhg" in cohort_text
    assert "distribution_plausible" in cohort_text
    assert "possible_po2_contamination" in cohort_text
    assert "insufficient_valid_rows" in cohort_text
    assert "sentinel_extreme_n" in cohort_text
    assert "sentinel_removed_n" in cohort_text
    assert "pco2_itemid_qc_sentinel_itemids_n" in cohort_text
    assert "pco2_itemid_qc_sentinel_removed_total_n" in cohort_text
    assert "out_of_range_removed_rate" in cohort_text
    assert "sentinel_removed_rate" in cohort_text
    assert "qc_blocking_flag" in cohort_text
    assert "qc_warning_flag" in cohort_text
    assert "qc_status" in cohort_text
    assert "qc_blocking_reason" in cohort_text
    assert "qc_warning_reason" in cohort_text
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
    assert "poc_itemid_qc_reason" in cohort_text
    assert "poc_itemid_qc_status" in cohort_text
    assert "poc_itemid_qc_blocking_passed" in cohort_text
    assert "poc_itemid_qc_failed_itemids_n" in cohort_text
    assert "poc_itemid_qc_warning_itemids_n" in cohort_text
    assert "poc_itemid_qc_fail_reasons" in cohort_text
    assert "poc_itemid_qc_warn_reasons" in cohort_text
    assert "poc_used_in_qualification_logic" in cohort_text
    assert "poc_qc_is_telemetry_only" in cohort_text
    assert "poc_qualifying_earliest_0_24h_hadm_n" in cohort_text
    assert "poc_qualifying_any_type_0_24h_hadm_n" in cohort_text
    assert "hco3_band_qc_inconsistency_n" in cohort_text
    assert "pco2_threshold_any" in cohort_text
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
    assert "timing_integrity_audit.csv" in cohort_text
    assert "ventilation_timing_audit.csv" in cohort_text
    assert "contract_warning_codes" in cohort_text
    assert "contract_error_codes" in cohort_text
    assert "qa_status_final" in cohort_text
    assert "hadm_other_rate_0_24h" in cohort_text
    assert "max_pco2_0_24h_lt_qualifying_n" in cohort_text
    assert "dt_first_qualifying_gas_hours_pct_le_24" in cohort_text
    assert "dt_first_qualifying_gas_hours_pct_gt_24" in cohort_text


def test_analysis_notebook_contains_requested_outputs() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    assert '"pco2_threshold_any"' in analysis_text
    assert '"unknown_hypercap_threshold"' in analysis_text
    assert '"other_hypercap_threshold"' not in analysis_text
    assert "ICD_vs_Gas_Performance.xlsx" in analysis_text
    assert "ICD_Positive_Subset_Breakdown.xlsx" in analysis_text
    assert "Ascertainment_Overlap_UpSet.png" in analysis_text
    assert "from upsetplot import UpSet, from_indicators" in analysis_text
    assert "def select_preferred_vital_column(" in analysis_text
    assert "qualifying_gas_time_observed_rate" in analysis_text
    assert "poc_itemid_qc_status" in analysis_text
    assert "poc_itemid_qc_reason" in analysis_text
    assert "poc_itemid_qc_failed_itemids_n" in analysis_text
    assert "poc_itemid_qc_warning_itemids_n" in analysis_text
    assert "poc_qualifying_earliest_0_24h_hadm_n" in analysis_text
    assert "poc_qualifying_any_type_0_24h_hadm_n" in analysis_text
    assert "UNKNOWN semantics" in analysis_text
    assert "panel_unknown_rate" in analysis_text
    assert "encounter_unknown_rate" in analysis_text
    assert "analysis_export_registry" in analysis_text
    assert "def write_excel_export(" in analysis_text
    assert "def canonicalize_rfv_label(" in analysis_text
    assert "def apply_uncodable_policy(" in analysis_text
    assert "def build_rfv_label_artifacts(" in analysis_text
    assert "CANONICAL_TO_GROUP" in analysis_text
    assert "summarize_multilabel_prevalence(" in analysis_text
    assert "summarize_labels_per_encounter(" in analysis_text
    assert "build_rfv_membership_long(" in analysis_text
    assert "RFV_Labels_Per_Encounter_Summary.xlsx" in analysis_text
    assert "RFV_Uncodable_Diagnostics.xlsx" in analysis_text
    assert "analysis_qc_checks.json" in analysis_text
    assert "analysis_qc_checks.csv" in analysis_text
    assert "quiet_unlink(" in analysis_text
    assert "display_path(" in analysis_text
    assert "MANUSCRIPT_ASSET_FILENAMES" in analysis_text
    assert "SECONDARY_OUTPUT_FILENAMES" in analysis_text
    assert "FIGURE_SPECS = {" in analysis_text
    assert '"table_1_xlsx": "Table 1.xlsx"' in analysis_text
    assert '"table_2_xlsx": "Table 2.xlsx"' in analysis_text
    assert '"figure_1_pdf": "Figure 1.pdf"' in analysis_text
    assert '"figure_2_xlsx": "Figure 2.xlsx"' in analysis_text
    assert '"figure_3_xlsx": "Figure 3.xlsx"' in analysis_text
    assert '"figure_4_xlsx": "Figure 4.xlsx"' in analysis_text
    assert '"figure_s1_xlsx": "Figure S1.xlsx"' in analysis_text
    assert '"figure_s8_png": "Figure S8.png"' in analysis_text
    assert '"figure_s9_xlsx": "Figure S9.xlsx"' in analysis_text
    assert '"figure_s9_png": "Figure S9.png"' in analysis_text
    assert '"candidate_compensation_matrix_plot": "Candidate_Figure_Compensation_Matrix.png"' in analysis_text
    assert '"candidate_compensation_matrix_workbook": "Candidate_Figure_Compensation_Matrix.xlsx"' in analysis_text
    assert '"submission_manifest_xlsx": "submission_manifest.xlsx"' in analysis_text
    assert '"submission_manifest_csv": "submission_manifest.csv"' in analysis_text
    assert '"outputs_readme": "OUTPUTS_README.md"' in analysis_text
    assert '"acid_base_source_missingness": "Supplementary_Table_Acid_Base_Source_Missingness.xlsx"' in analysis_text
    assert '"candidate_definition_summary": "Candidate_Definition_Yield_Composition.xlsx"' in analysis_text
    assert '"sensitivity_suite": "Sensitivity_Analysis_Suite.xlsx"' in analysis_text
    assert '"figure_manifest": "figure_manifest.csv"' in analysis_text
    assert '"baseline_characteristics_expanded": "Baseline_Characteristics_Expanded.xlsx"' in analysis_text
    assert "write_pdf_table_export(" in analysis_text
    assert "write_figure_manifest(" in analysis_text
    assert "Results/YYYY-MM-DD/" in analysis_text
    assert "artifacts/qa/analysis/" in analysis_text
    assert "artifacts/qa/cohort/qa_summary.json" in analysis_text
    assert "Drafts/Apr 16 2026/" not in analysis_text
    assert "Figure 1.pdf" in analysis_text
    assert "Figure 2.png" in analysis_text
    assert "Figure 3.png" in analysis_text
    assert "Figure 4.png" in analysis_text
    assert "Figure S1.png" in analysis_text
    assert "Figure S8.png" in analysis_text
    assert "Figure S9.png" in analysis_text
    assert "Candidate_Figure_Compensation_Matrix.png" in analysis_text
    assert "Candidate_Figure_Compensation_Matrix.xlsx" in analysis_text
    assert "Table 1.pdf" in analysis_text
    assert "Table 2.pdf" in analysis_text
    assert "Sensitivity_Primary_Label_Table_2.xlsx" in analysis_text
    assert "Sensitivity_First_Priority_RFV_Route_Prevalence.png" in analysis_text
    assert "Sensitivity_First_Priority_RFV_Age_Prevalence.png" in analysis_text
    assert "Sensitivity_First_Priority_RFV_Acidemia_Severity.png" in analysis_text
    assert "Sensitivity_First_Priority_RFV_Acidemia_Timing.png" in analysis_text
    assert "Acidemia_Severity_Prevalence.png" in analysis_text
    assert "Acidemia_Timing_Prevalence.png" in analysis_text
    assert "Other grouped RFV categories" in analysis_text
    assert "Ventilation_Regression_Forest.png" in analysis_text
    assert "RFV_Prevalence_by_Comorbidity.xlsx" in analysis_text
    assert "RFV_Prevalence_by_Comorbidity_Heatmap.png" in analysis_text
    assert "RFV_COMORBIDITY_BASE_COHORT = analytic_df" in analysis_text
    assert "timing_prevalence_panel_plot_path" in analysis_text
    assert "obsolete_figure_5_assets_absent" in analysis_text
    assert 'SYMPTOM_COL = "RFV1_name"' not in analysis_text
    assert 'RFV_PRIMARY_COL = "RFV1_name"' not in analysis_text


def test_analysis_manifest_freezes_submission_revision_definitions() -> None:
    manifest_path = WORK_DIR / "analysis_manifest.yml"
    manifest = yaml.safe_load(manifest_path.read_text())

    assert manifest["analysis_version"] == "2026-06-submission"
    assert manifest["unit_of_analysis"] == "hospital admission linked to ED presentation"
    assert manifest["rfv_taxonomy"]["category_system"] == "current package NHAMCS RFV categories"
    assert manifest["rfv_taxonomy"]["no_new_granular_categories"] is True
    assert manifest["rfv_taxonomy"]["no_new_composite_chief_concern_categories"] is True
    assert manifest["hypercapnia_evidence"]["abg_pco2_threshold_mmhg"] == 45
    assert manifest["hypercapnia_evidence"]["vbg_pco2_threshold_mmhg"] == 50
    assert manifest["hypercapnia_evidence"]["unknown_pco2_threshold_mmhg"] == 50
    assert manifest["acid_base"]["ph_bands"] == [
        "<7.20",
        "7.20-7.24",
        "7.25-7.29",
        "7.30-7.34",
        "7.35-7.44",
        ">=7.45",
    ]
    assert manifest["acid_base"]["hco3_bands_mmol_l"] == ["<22", "22-27", "28-33", ">=34"]
    assert manifest["models"]["new_regression_models"] is False
    assert manifest["models"]["new_prediction_models"] is False


def test_large_revision_outputs_are_registered_and_aggregate_only() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert "def build_submission_manifest(" in analysis_text
    assert "def write_outputs_readme(" in analysis_text
    assert "submission_manifest = build_submission_manifest(" in analysis_text
    assert 'submission_manifest_xlsx_path = write_excel_export(\n    "submission_manifest_xlsx"' in analysis_text
    assert 'register_export(\n    key="submission_manifest_csv"' in analysis_text
    assert "outputs_readme_path = write_outputs_readme(OUTPUT_DIR, submission_manifest)" in analysis_text
    assert "build_acid_base_source_missingness_table(" in analysis_text
    assert "build_candidate_definition_membership(" in analysis_text
    assert "build_gas_source_sensitivity_summary(" in analysis_text
    assert "def _administrative_only_rfv_mask(" in analysis_text
    assert "def source_specific_pco2_threshold_mask(" in analysis_text
    assert '"first_abg_hypercap_pco2_mmhg"' in analysis_text
    assert '"first_vbg_hypercap_pco2_mmhg"' in analysis_text
    assert '"first_other_pco2"' in analysis_text
    assert "build_pco2_threshold_sensitivity_summary(" in analysis_text
    assert "build_sensitivity_denominator_audit(" in analysis_text
    assert '"Source_Missingness": acid_base_source_missingness' in analysis_text
    assert '"Frozen_Band_Rules": frozen_band_rules' in analysis_text
    assert '"PH_by_HCO3_Counts": ph_hco3_bivariate_counts' in analysis_text
    assert '"Candidate_Definitions": candidate_definition_yield' in analysis_text
    assert '"RFV_Composition_Grouped": candidate_definition_rfv_grouped' in analysis_text
    assert '"Denominator_Audit": build_sensitivity_denominator_audit(analytic_df)' in analysis_text
    assert '"Gas_Source": build_gas_source_sensitivity_summary(analytic_df)' in analysis_text
    assert '"Administrative_Exclusion": build_administrative_exclusion_sensitivity(analytic_df)' in analysis_text
    assert '"Analytic_Cohort_Thresholds": build_analytic_cohort_threshold_sensitivity_summary(analytic_df)' in analysis_text
    assert '"ICD_Era": build_icd_era_sensitivity_summary(analytic_df)' not in analysis_text
    assert "All rows are aggregate-only and exclude subject_id, hadm_id, ed_stay_id" in analysis_text
    assert "No row-level identifiers or raw chief complaint text are included." in analysis_text
    threshold_block = analysis_text.split("def build_pco2_threshold_sensitivity_summary", maxsplit=1)[
        1
    ].split("def build_sensitivity_denominator_audit", maxsplit=1)[0]
    assert "source_specific_pco2_threshold_mask(" in threshold_block
    assert 'source.eq("ABG") & pco2.ge' not in threshold_block
    admin_block = analysis_text.split("def build_administrative_exclusion_sensitivity", maxsplit=1)[
        1
    ].split("def _any_numeric_candidate_ge", maxsplit=1)[0]
    assert "admin_only = _administrative_only_rfv_mask(df)" in admin_block
    assert "canonical_cols =" not in admin_block
    denominator_block = analysis_text.split("def build_sensitivity_denominator_audit", maxsplit=1)[
        1
    ].split("def _sensitivity_denominator_definition", maxsplit=1)[0]
    assert "first_admission = first_eligible_admission_mask(df)" in denominator_block
    assert "admin_only = _administrative_only_rfv_mask(df)" in denominator_block
    assert "canonical_cols =" not in denominator_block
    assert 'if "subject_id" in df.columns else' not in denominator_block


def test_table_1_uses_explicit_summary_types_and_journal_formatting() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert 'TABLE1_SUMMARY_MEDIAN_IQR = "median [IQR]"' in analysis_text
    assert 'TABLE1_SUMMARY_COUNT_PCT = "n (%)"' in analysis_text
    assert '"Summary type": summary_type' in analysis_text
    assert "def summarize_table1_cell(" in analysis_text
    assert "def validate_table1_output(" in analysis_text
    assert "def format_table1_excel(" in analysis_text
    assert "TABLE1_COMMA_NUMERAL_PATTERN" in analysis_text
    assert "TABLE1_MEDIAN_IQR_PATTERN" in analysis_text
    assert "TABLE1_COUNT_PCT_PATTERN" in analysis_text
    assert 'output_row[f"{group_name} (N={len(frame)})"]' in analysis_text
    assert "format_table1_excel(table_1_xlsx_path)" in analysis_text
    assert "format_table1_excel(expanded_table_1_path)" in analysis_text
    assert "table1_notes = pd.DataFrame" in analysis_text
    assert '"Table_1": manuscript_table_1' in analysis_text
    assert '"Baseline_Characteristics": expanded_table_1' in analysis_text
    assert '"Notes": table1_notes' in analysis_text

    table1_group_block = analysis_text.split("table1_group_frames = {", maxsplit=1)[1].split(
        "}\n\ntable1_notes = pd.DataFrame",
        maxsplit=1,
    )[0]
    assert '"Overall": analytic_df' in table1_group_block
    assert '"Gas-only": gas_only_df' in table1_group_block
    assert '"ICD-only": icd_only_df' in table1_group_block
    assert '"Both ICD + gas": both_df' in table1_group_block
    assert '"ICD-positive": icd_positive_df' not in table1_group_block
    assert "mutually exclusive ascertainment strata" in analysis_text
    assert "The unit of analysis is the hospital admission linked to an ED encounter" in analysis_text
    assert "COPD = chronic obstructive pulmonary disease" in analysis_text
    assert "IMV = invasive mechanical ventilation" in analysis_text

    assert 'f"Overall (N={len(analytic_df):,})"' not in analysis_text
    assert 'f"ICD-positive (N={len(icd_positive_df):,})"' not in analysis_text
    assert 'f"Gas-only (N={len(gas_only_df):,})"' not in analysis_text
    assert "def summarize_row(" not in analysis_text
    assert "def summarize_full(" not in analysis_text
    assert "Any ICD diagnosis, n (%)" not in analysis_text
    assert "Hospital LOS" not in analysis_text
    assert "ICU LOS" not in analysis_text
    assert "COPD (hospital flag)" not in analysis_text

    main_table_block = analysis_text.split("table1_rows = (", maxsplit=1)[1].split(
        "manuscript_table_1 = build_table1_frame(",
        maxsplit=1,
    )[0]
    assert "table1_comorbidity_rows" in main_table_block
    assert "table1_outcome_rows" in main_table_block
    assert '"flag_copd"' in analysis_text
    assert '"flag_osa_ohs"' in analysis_text
    assert '"flag_chf"' in analysis_text
    assert '"flag_neuromuscular"' in analysis_text
    assert '"flag_opioid_substance"' in analysis_text
    assert '"flag_pneumonia"' in analysis_text
    assert 'table1_row("ICD-positive", "any_hypercap_icd", TABLE1_SUMMARY_COUNT_PCT)' in analysis_text
    assert 'table1_row("Blood-gas criteria met", "pco2_threshold_any", TABLE1_SUMMARY_COUNT_PCT)' in analysis_text
    assert "ed_triage_acuity_missing_unknown" in analysis_text
    assert "ED triage acuity missing/unknown" in analysis_text
    assert 'table1_row("In-hospital death", "death_in_hosp", TABLE1_SUMMARY_COUNT_PCT)' in analysis_text
    assert 'table1_row("Hospital length of stay, days", "hosp_los_days", TABLE1_SUMMARY_MEDIAN_IQR)' in analysis_text
    assert 'table1_row("ICU length of stay, days", "icu_los_days", TABLE1_SUMMARY_MEDIAN_IQR)' in analysis_text


def test_table_2_submission_table_exposes_bootstrap_confidence_intervals() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    table2_block = analysis_text.split("table_2_submission =", maxsplit=1)[1].split(
        "table_2_xlsx_path = write_excel_export(",
        maxsplit=1,
    )[0]

    assert 'rfv_counts[["RFV category", "Count", "Percent"]].copy()' in table2_block
    assert 'table_2_submission["95% CI"]' in table2_block
    assert "row['ci_lower']" in table2_block
    assert "row['ci_upper']" in table2_block
    assert '"NHAMCS RFV category"' in table2_block
    assert '"n admissions"' in table2_block
    assert '"% admissions"' in table2_block
    assert '"Table_2": table_2_submission' in analysis_text
    assert '"Combined_View"' not in analysis_text
    assert '"RFV_Prevalence_Source": rfv_counts' in analysis_text
    assert '"table_2_source_workbook": "Table_2_Source_Diagnostics.xlsx"' in analysis_text
    assert "The Table_2 sheet reports patient-cluster bootstrap 95% confidence intervals" in analysis_text
    assert '"title": "NLP-derived NHAMCS RFV category prevalence"' in analysis_text
    assert (
        '"intended_caption": "Admission-level multi-label NHAMCS RFV category prevalence in the analytic cohort. '
        "Counts and percentages reflect admissions with each RFV category; categories are not mutually exclusive, "
        'and 95% confidence intervals use patient-cluster bootstrap."'
    ) in analysis_text
    table2_spec = analysis_text.split('"Table 2": {', maxsplit=1)[1].split("    },", maxsplit=1)[0]
    assert "Raw triage chief complaint field frequencies" not in table2_spec
    assert "raw triage entries" not in table2_spec.lower()


def test_submission_asset_bundle_has_clean_allowlist_and_manifest() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert 'SUBMISSION_ASSET_DIRNAME = "submission_assets"' in analysis_text
    assert 'SUBMISSION_ASSET_MANIFEST_FILENAME = "submission_assets_manifest.csv"' in analysis_text
    assert 'SUBMISSION_OPTIONAL_MISSING_FILENAME = "submission_asset_optional_missing.csv"' in analysis_text
    assert "SUBMISSION_OPTIONAL_UPSTREAM_FILENAMES" in analysis_text
    assert "SUBMISSION_REQUIRED_FILENAMES = frozenset(" in analysis_text
    assert "SUBMISSION_LABEL_TO_FIGURE_KEY" in analysis_text
    assert "SUBMISSION_TABLE_SPECS" in analysis_text
    assert "def build_submission_asset_allowlist(" in analysis_text
    assert "def validate_submission_asset_bundle(" in analysis_text
    assert "def build_submission_asset_bundle(" in analysis_text
    assert ") = build_submission_asset_bundle(OUTPUT_DIR)" in analysis_text
    assert 'key="submission_assets_dir"' in analysis_text
    assert 'key="submission_asset_manifest"' in analysis_text
    assert 'key="submission_asset_optional_missing"' in analysis_text

    allowlist_body = analysis_text.split("def build_submission_asset_allowlist(", maxsplit=1)[1].split(
        "def validate_submission_asset_bundle(",
        maxsplit=1,
    )[0]
    manuscript_filename_body = analysis_text.split("MANUSCRIPT_ASSET_FILENAMES = {", maxsplit=1)[1].split(
        "SECONDARY_OUTPUT_FILENAMES = {",
        maxsplit=1,
    )[0]
    secondary_filename_body = analysis_text.split("SECONDARY_OUTPUT_FILENAMES = {", maxsplit=1)[1].split(
        "FIGURE_SPECS = {",
        maxsplit=1,
    )[0]
    assert "Candidate_Figure_Compensation_Matrix" in secondary_filename_body
    assert "Candidate_Figure_Compensation_Matrix" not in manuscript_filename_body
    assert "Candidate_Figure_Compensation_Matrix" not in allowlist_body
    for asset_name in (
        "Figure_1.pdf",
        "Figure_1.png",
        "Figure_2.pdf",
        "Figure_2.png",
        "Figure_3.pdf",
        "Figure_3.png",
        "Figure_4.pdf",
        "Figure_4.png",
        "Table_1_submission.xlsx",
        "Table_2_submission.xlsx",
        "Figure_S1.pdf",
        "Figure_S1.png",
        "Figure_S2.pdf",
        "Figure_S2.png",
        "Figure_S3.pdf",
        "Figure_S3.png",
        "Figure_S4.pdf",
        "Figure_S4.png",
        "Figure_S5.pdf",
        "Figure_S5.png",
        "Figure_S6.pdf",
        "Figure_S6.png",
        "Figure_S7.pdf",
        "Figure_S7.png",
        "Figure_S8.pdf",
        "Figure_S8.png",
        "Figure_S9.pdf",
        "Figure_S9.png",
    ):
        assert asset_name in allowlist_body

    assert "Figure 2.xlsx" not in allowlist_body
    assert "Figure S6.xlsx" not in allowlist_body
    assert "Rater_Benchmark_Supplement_Tables.xlsx" not in allowlist_body
    assert "NLP_Classifier_Supplement_Tables.xlsx" not in allowlist_body
    assert '"Table S1.xlsx"' in allowlist_body
    assert '"Supplementary_Table_S1.xlsx"' in allowlist_body
    assert 'required_asset=source_filename != "Table S1.xlsx"' in allowlist_body
    assert '"pdf_to_png"' in allowlist_body
    assert "pdftoppm" in analysis_text
    assert '"required_asset"' in analysis_text
    assert '"producer_stage"' in analysis_text
    assert "if bool(spec.get(\"required_asset\", True)):" in analysis_text
    assert "Missing required allowlisted submission asset" in analysis_text
    assert "missing_optional_rows.append" in analysis_text
    assert "optional_upstream_asset_missing" in analysis_text
    assert "validate_submission_asset_bundle(bundle_dir, manifest_df, missing_optional_df)" in analysis_text
    assert "analysis_export_registry.pop(\"submission_asset_optional_missing\", None)" in analysis_text
    for manifest_column in (
        "file_name",
        "manuscript_label",
        "title",
        "main_or_supplement",
        "source_notebook",
        "source_data_file",
        "intended_caption",
        "include_in_submission",
    ):
        assert f'"{manifest_column}"' in analysis_text

    for forbidden_pattern in (
        "Clinical_Outcomes",
        "Outcome_Rates",
        "Ventilation_Regression",
        "Ventilation_Strategy",
        "RFV_Prevalence_by_Comorbidity",
        "Time_To_Gas_By_Symptom_Category",
        "Recognition_By_PCO2",
        "Candidate",
        "__MACOSX",
        "qa",
        "debug",
    ):
        assert forbidden_pattern in analysis_text
    assert "hidden_or_archive_hits" in analysis_text
    assert "Included submission assets have blank required fields" in analysis_text
    assert "submission_assets missing required files" in analysis_text
    assert "Figure S7/S8 assets require explicit optional citation allowlisting" not in analysis_text
    assert "submission_assets file mismatch" in analysis_text


def test_cohort_notebook_generates_ed_only_exclusion_audit() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()

    assert "ed_only_exclusion_audit_sql" in cohort_text
    assert "ed_only_exclusion_audit.csv" in cohort_text
    assert "ed_only_exclusion_audit.json" in cohort_text
    assert "ed_only_with_nonmissing_triage_chief_complaint" in cohort_text
    assert "ed_only_with_target_hypercap_ohs_ed_icd" in cohort_text
    assert "ed_only_gas_eligibility_count" in cohort_text
    assert "not_assessable" in cohort_text
    assert "cohort_logic_changed" in cohort_text
    assert "ed_only_exclusion_audit_csv_path" in cohort_text
    assert "ed_only_exclusion_audit_json_path" in cohort_text


def test_supplement_sensitivity_figures_use_first_priority_admission_language() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    for figure_key in (
        "figure_s2_png",
        "figure_s3_png",
        "figure_s4_png",
        "figure_s5_png",
        "sensitivity_route_plot",
        "sensitivity_age_plot",
        "acidemia_severity_sensitivity_plot",
        "timing_acidemia_sensitivity_plot",
    ):
        figure_spec = analysis_text.split(f'"{figure_key}": {{', maxsplit=1)[1].split("    },", maxsplit=1)[0]
        assert "First-priority RFV assignment" in figure_spec
        assert "admission-level" in figure_spec
        assert "Sensitivity analysis: first-priority RFV only" in figure_spec
        if figure_key in {"figure_s2_png", "sensitivity_route_plot"}:
            assert "percentages within each indicator panel sum to 100%" in figure_spec
            assert "percentages within each stratum sum to 100%" not in figure_spec
        else:
            assert "percentages within each stratum sum to 100%" in figure_spec
        assert "RFV1-only" not in figure_spec
        assert "primary-label" not in figure_spec

    sensitivity_block = analysis_text.split("route_sensitivity = (", maxsplit=1)[1].split(
        "table1_group_frames = {",
        maxsplit=1,
    )[0]
    acidemia_block = analysis_text.split("acidemia_acidemia_timing_sensitivity_counts = (", maxsplit=1)[1].split(
        "etiology_order =",
        maxsplit=1,
    )[0]
    target_blocks = sensitivity_block + acidemia_block
    assert target_blocks.count('y_label="First-priority presenting-concern category"') >= 4
    assert target_blocks.count("render_manuscript_prevalence_panels(") >= 4
    assert "sensitivity_strip_text =" not in analysis_text
    assert "strip_text=sensitivity_strip_text" not in target_blocks
    assert 'strip_text="Sensitivity analysis: first-priority RFV only"' not in target_blocks
    assert target_blocks.count("prevalence_lollipop_xmax(") >= 4
    assert "muted_marks=True" not in target_blocks
    assert "x_max=100.0" not in target_blocks
    assert "stacked=True" not in target_blocks
    assert 'ax.legend(title="First-priority RFV group"' not in target_blocks
    assert "Percent of encounters" not in target_blocks
    assert "Primary RFV group" not in target_blocks
    assert "Sensitivity_Primary_Label_Route_Prevalence.png" not in analysis_text
    assert "Sensitivity_Primary_Label_Age_Prevalence.png" not in analysis_text
    assert "Sensitivity_Primary_Label_Acidemia_Severity.png" not in analysis_text
    assert "Sensitivity_Primary_Label_Acidemia_Timing.png" not in analysis_text


def test_figure_s1_acidemia_timing_matches_publication_style() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure_s1_spec = analysis_text.split('"figure_s1_png": {', maxsplit=1)[1].split(
        "    },",
        maxsplit=1,
    )[0]
    prevalence_helper = analysis_text.split("def render_manuscript_prevalence_panels(", maxsplit=1)[1].split(
        "def render_age_profile_panels(",
        maxsplit=1,
    )[0]
    figure_s1_block = analysis_text.split(
        "timing_prevalence_plot_path = render_manuscript_prevalence_panels(",
        maxsplit=1,
    )[1].split("note_export(timing_prevalence_plot_path)", maxsplit=1)[0]

    assert 'ACIDEMIA_TIMING_ORDER = ["Early acidemia", "Late acidemia", "No acidemia"]' in analysis_text
    assert "multi-label and not mutually exclusive" in figure_s1_spec
    assert "Timing definitions are described in the caption and source workbook" in figure_s1_spec
    assert "build_acidemia_timing_denominator_audit" in analysis_text
    assert '"Denominator_Audit": acidemia_timing_denominator_audit' in analysis_text
    assert "excluded_no_ph_within_24h" in analysis_text
    assert "excluded_acidemia_within_24h_missing_0_6h_ph" in analysis_text
    assert "facet_order=ACIDEMIA_TIMING_ORDER" in figure_s1_block
    assert "category_order=grouped_category_order" in figure_s1_block
    assert 'y_label="Grouped presenting-concern category"' in figure_s1_block
    assert 'fig.supxlabel("Percent of admissions"' in prevalence_helper
    assert "N={format_figure_n(group_sizes.get(facet, 0))}" in prevalence_helper
    assert "0-6" not in figure_s1_block
    assert "6-24" not in figure_s1_block


def test_supplement_operational_figures_s6_s7_contract() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure_s6_spec = analysis_text.split('"figure_s6_png": {', maxsplit=1)[1].split(
        "    },",
        maxsplit=1,
    )[0]
    figure_s7_spec = analysis_text.split('"figure_s7_png": {', maxsplit=1)[1].split(
        "    },",
        maxsplit=1,
    )[0]
    figure_s7_block = analysis_text.split("time_to_gas_source_df = biochemical_df.copy()", maxsplit=1)[
        1
    ].split("recognition_pco2_bin_order =", maxsplit=1)[0]
    source_overlap_helpers = analysis_text.split("SOURCE_OVERLAP_SOURCES =", maxsplit=1)[1].split(
        "def render_manuscript_prevalence_panels(",
        maxsplit=1,
    )[0]
    figure_s6_block = analysis_text.rsplit("for source_label, expected_count in {", maxsplit=1)[1].split(
        "acidemia_timing_df =",
        maxsplit=1,
    )[0]

    assert "ABG, VBG, ICD, and UNKNOWN-source gas" in figure_s6_spec
    assert "Supplementary UpSet-style source-specific overlap summary" in figure_s6_spec
    assert "Figure 2 remains limited to ABG/VBG/ICD overlapping ascertainment indicators" in figure_s6_spec
    assert 'SOURCE_OVERLAP_SOURCES = ["ABG", "VBG", "ICD", "UNKNOWN"]' in analysis_text
    assert '"UNKNOWN": "unknown_hypercap_threshold"' in source_overlap_helpers
    assert "color=rfv_display_color(row.category)" in figure_s7_block
    assert 'matrix_set_order = list(SOURCE_OVERLAP_MATRIX_COLUMNS.values())' in figure_s6_block
    assert "matrix_ax.vlines(" in figure_s6_block
    assert "UNKNOWN-positive" in figure_s6_block
    assert "UNKNOWN gas" in figure_s6_block
    assert "unknown_intersection_total" in figure_s6_block
    assert "Indeterminate_Source_Audit" in figure_s6_block
    assert "Intersection_Counts" in figure_s6_block
    assert "Set_Sizes" in figure_s6_block
    assert "Indeterminate gas only" in source_overlap_helpers
    assert "ICD + no gas" in source_overlap_helpers
    assert "ICD only" not in source_overlap_helpers
    assert "ascending=[False, True]" in source_overlap_helpers
    assert "format_figure_n(count)" in figure_s6_block
    assert "icd_intersection_total" in figure_s6_block
    assert "figure1_source_counts[\"ICD-positive\"]" in figure_s6_block
    assert "source-specific intersections sum" in figure_s6_block
    assert "Source-specific ascertainment intersection" in figure_s6_block

    assert "blood-gas documentation" in figure_s7_spec
    assert "diagnostic delay" in figure_s7_spec
    assert "delay" not in figure_s7_block.lower()
    assert '.sort_values(["median", "category"], ascending=[True, True])' in figure_s7_block
    assert "ax.axvline(6.0" in figure_s7_block
    assert 'color=ANALYSIS_COLORS["dark"]' in figure_s7_block
    assert "color=rfv_display_color(row.category)" in figure_s7_block
    assert "median_q1_q3_hours_label" in figure_s7_block
    assert "category_n" in figure_s7_block
    assert "valid_timing_n" in figure_s7_block
    assert "missing_timing_n" in figure_s7_block
    assert "excluded_out_of_range_timing_n" in figure_s7_block
    assert "Time_To_Gas" in figure_s7_block
    assert "Timing_Missingness" in figure_s7_block


def test_figure_s8_icd_recognition_uses_grouped_heatmap_contract() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure_s8_spec = analysis_text.split('"figure_s8_png": {', maxsplit=1)[1].split(
        "    },",
        maxsplit=1,
    )[0]
    s8_helper = analysis_text.split("def render_recognition_by_pco2_heatmap(", maxsplit=1)[1].split(
        "def direct_label_lines(",
        maxsplit=1,
    )[0]
    s8_block = analysis_text.split("recognition_pco2_bin_order =", maxsplit=1)[1].split(
        "abg_idx = set(analytic_df.index[to_binary_flag",
        maxsplit=1,
    )[0]
    s8_text = figure_s8_spec + s8_helper + s8_block

    assert "Matrix heatmap" in figure_s8_spec
    assert "diagnostic accuracy without chart review" in figure_s8_spec
    assert "def render_recognition_by_pco2_heatmap(" in analysis_text
    assert "sns.heatmap(" in s8_helper
    assert 'cmap="cividis"' in s8_helper
    assert "icd_recognition_grayscale" not in s8_helper
    assert "ordered_rfv_display_categories(RFV_GROUP_ORDER)" in s8_helper
    assert "format_figure_pct" in s8_helper
    assert "format_figure_n(numerator)" in s8_helper
    assert "format_figure_n(denominator)" in s8_helper
    assert "cbar=False" in s8_helper
    assert "colorbar" not in s8_helper.lower()
    assert "legend" not in s8_helper.lower()
    assert "readable_text_color_for_cmap_value(" in analysis_text
    assert 'plt.get_cmap(cmap_name)(normalized_value)' in analysis_text
    assert 'text.set_color(readable_text_color_for_cmap_value(cell_value, cmap_name="cividis"))' in s8_helper
    assert 'ax.set_xlabel("Qualifying PCO2 stratum")' in s8_helper
    assert 'ax.set_ylabel("Grouped presenting-concern category")' in s8_helper

    assert "rfv_group_membership_long" in s8_block
    assert "grouped_presenting_concern_category" in s8_block
    assert "Recognition_Tidy" in s8_block
    assert "ChartReady_Matrix" in s8_block
    assert "PCO2_Bin_N" in s8_block
    for column_name in (
        "numerator",
        "denominator",
        "percent_icd_positive",
        "ci_lower",
        "ci_upper",
    ):
        assert column_name in s8_block
    assert "proportion_confint(" in s8_block
    assert "direct_label_lines(" not in s8_block
    assert "Symptom –" not in s8_text


def test_publication_figure_style_contract_is_unified_and_title_free() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    for helper_name in (
        "theme_rr_minimal",
        "finish_rr_axis",
        "format_figure_pct",
        "format_figure_n",
        "ordered_rfv_display_categories",
        "rfv_display_color",
    ):
        assert f"def {helper_name}(" in analysis_text

    assert "savefig.dpi" in analysis_text
    assert '"Respiratory": "#0072B2"' in analysis_text
    assert '"Injuries & adverse effects": "#E69F00"' in analysis_text
    assert "fig.suptitle(" not in analysis_text
    assert "plt.suptitle(" not in analysis_text
    assert "ax.set_title(FIGURE_SPECS" not in analysis_text
    assert "bar_ax.set_title(FIGURE_SPECS" not in analysis_text
    assert 'grouped_category_order = list(RFV_GROUP_ORDER)' in analysis_text
    assert "RVC" not in analysis_text


def test_first_admission_sensitivity_is_secondary_aggregate_workbook() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert '"first_admission_sensitivity": "First_Admission_Sensitivity.xlsx"' in analysis_text
    assert "select_first_eligible_admission_per_patient" in analysis_text
    assert "FIRST_ADMISSION_SIMILARITY_THRESHOLD_PP = 2.0" in analysis_text
    for sheet_name in (
        "Cohort_Counts",
        "Overall_Grouped",
        "Overall_Canonical",
        "By_Ascertainment",
        "By_Age",
        "By_Acidemia",
        "Top_Category_Flags",
        "Interpretation_Flags",
    ):
        assert sheet_name in analysis_text
    assert "row-level identifiers and raw chief complaint text are not exported" in analysis_text
    assert "manuscript_asset_name=\"First_Admission_Sensitivity.xlsx\"" not in analysis_text


def test_timing_safeguard_is_secondary_aggregate_workbook() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert (
        '"timing_safeguard_rfv_comparison": "Timing_Safeguard_RFV_Comparison.xlsx"'
        in analysis_text
    )
    assert "build_timing_safeguard_cohort_membership" in analysis_text
    assert "TIMING_SAFEGUARD_SIMILARITY_THRESHOLD_PP = 2.0" in analysis_text
    for cohort_label in (
        "Any-admission qualifying hypercapnia",
        "First qualifying gas within 24h",
        "First qualifying gas within 6h",
        "ICD-positive",
        "ICD + early gas",
    ):
        assert cohort_label in analysis_text
    assert "ICD-positive only" not in analysis_text
    assert "Definition note" in analysis_text
    assert "Administrative RFV" in analysis_text
    assert "administrative_prevalence=timing_safeguard_canonical_prevalence" in analysis_text
    for sheet_name in (
        "Cohort_Denominators",
        "Main_Text_Table",
        "Grouped_RFV_Prevalence",
        "Canonical_RFV_Prevalence",
        "Prevalence_Comparisons",
        "Top_Category_Flags",
        "Interpretation_Flags",
        "Notes",
    ):
        assert sheet_name in analysis_text
    assert "Primary analysis remains the broad EHR-ascertained admission-level cohort." in analysis_text
    assert "row-level identifiers and raw chief complaint text are not exported" in analysis_text
    assert "manuscript_asset_name=\"Timing_Safeguard_RFV_Comparison.xlsx\"" not in analysis_text


def test_cohort_exports_paired_qualifying_ph_and_aggregate_audit() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    contract_text = (WORK_DIR / "src" / "hypercap_cc_nlp" / "pipeline_contracts.py").read_text()
    spec_text = (WORK_DIR / "docs" / "SPEC.md").read_text()
    decisions_text = (WORK_DIR / "docs" / "DECISIONS.md").read_text()
    dictionary_text = (WORK_DIR / "data_dictionary.csv").read_text()

    for field_name in (
        "qualifying_ph",
        "qualifying_ph_time",
        "qualifying_ph_source_branch",
        "qualifying_ph_site",
        "qualifying_ph_pairing_status",
    ):
        assert field_name in cohort_text
    for status_label in (
        "paired_same_specimen_panel",
        "paired_same_time_panel",
        "qualifying_panel_missing_ph",
    ):
        assert status_label in cohort_text
    for scope_key in (
        "broad_gas_positive",
        "first_qualifying_gas_within_24h",
        "first_qualifying_gas_within_6h",
        "late_gas_after_24h",
        "icd_plus_gas",
        "icd_only",
    ):
        assert scope_key in cohort_text
    assert "qualifying_ph_pairing_completeness_audit.csv" in cohort_text
    assert "qualifying_ph_pairing_completeness_audit.csv" in contract_text
    assert "source branch, site, and panel time" in spec_text
    assert "source branch, site, and panel time" in dictionary_text
    with (WORK_DIR / "data_dictionary.csv").open(newline="") as dictionary_file:
        dictionary_rows = {row["variable_name"]: row for row in csv.DictReader(dictionary_file)}
    source_branch_row = dictionary_rows["qualifying_ph_source_branch"]
    site_row = dictionary_rows["qualifying_ph_site"]
    assert source_branch_row["allowed_values"] == "LAB|POC"
    assert site_row["allowed_values"] == "arterial|venous|unknown"
    paired_ph_contract_text = "\n".join(
        (
            source_branch_row["definition"],
            source_branch_row["description"],
            source_branch_row["allowed_values"],
            source_branch_row["derivation_or_transformation"],
            source_branch_row["validation_rules"],
            source_branch_row["notes"],
            site_row["definition"],
            site_row["description"],
            site_row["allowed_values"],
            site_row["derivation_or_transformation"],
            site_row["validation_rules"],
            site_row["notes"],
        )
    )
    for stale_token in ("hosp_lab", "icu_bg", "poc_bg", "ABG|VBG|UNKNOWN"):
        assert stale_token not in paired_ph_contract_text
    assert "before stricter ICU site-compatible pairing" in decisions_text
    assert "requires a new private cohort rerun" in decisions_text
    assert "supporting a switch from source-priority pH" not in decisions_text
    icu_ph_clean_block = cohort_text.split("icu_ph_clean AS (", maxsplit=1)[1].split(
        "),\nicu_pco2_raw AS",
        maxsplit=1,
    )[0]
    icu_pco2_clean_block = cohort_text.split("icu_pco2_clean AS (", maxsplit=1)[1].split(
        "),\nall_pco2 AS",
        maxsplit=1,
    )[0]
    stay_pco2_block = cohort_text.split("stay_pco2 AS (", maxsplit=1)[1].split(
        "),\nqualifying_candidates AS",
        maxsplit=1,
    )[0]
    qualifying_candidates_block = cohort_text.split("qualifying_candidates AS (", maxsplit=1)[1].split(
        "),\nqualifying_ranked AS",
        maxsplit=1,
    )[0]
    assert "FROM icu_ph_ranked" in icu_ph_clean_block
    assert "END AS site" in icu_ph_clean_block
    assert "specimen_type_text" in icu_ph_clean_block
    assert "AND h.site = p.site" in icu_pco2_clean_block
    assert "WHEN h.ph_value IS NOT NULL THEN h.site" in icu_pco2_clean_block
    assert "WHEN h.ph_value IS NOT NULL THEN 'POC'" in icu_pco2_clean_block
    assert "p.qualifying_ph_source_branch" in stay_pco2_block
    assert "p.qualifying_ph_site" in stay_pco2_block
    assert "qualifying_ph_source_branch" in qualifying_candidates_block
    assert "qualifying_ph_site" in qualifying_candidates_block
    assert "site AS qualifying_ph_site" not in qualifying_candidates_block
    assert "source_branch AS qualifying_ph_source_branch" not in qualifying_candidates_block
    assert "subject_id" not in cohort_text.split("qualifying_ph_pairing_completeness_audit = pd.DataFrame(")[
        1
    ].split("qualifying_ph_pairing_completeness_audit_path", maxsplit=1)[0]


def test_generated_supplement_table_assets_are_notebook_native() -> None:
    classifier_text = (WORK_DIR / "Hypercap CC NLP Classifier.qmd").read_text()
    rater_text = (WORK_DIR / "Rater Agreement Analysis.qmd").read_text()
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert "NLP_Classifier_Supplement_Tables.xlsx" in classifier_text
    for sheet_name in (
        "classifier_config",
        "prototype_metadata",
        "normalization_resources",
        "guardrail_examples",
        "classification_examples",
    ):
        assert sheet_name in classifier_text
    assert "nlp_classifier_supplement_tables_path" in classifier_text

    assert "Rater_Benchmark_Supplement_Tables.xlsx" in rater_text
    for sheet_name in (
        "annotation_examples",
        "grouped_mapping",
        "matched_denominators",
        "agreement_summary",
        "set_agreement_ci",
        "per_category_metrics",
        "per_category_ci",
        "confusion_canonical",
        "confusion_grouped",
        "disagreement_examples",
        "cohort_overlap_audit",
        "benchmark_notes",
        "threshold_summary",
    ):
        assert sheet_name in rater_text
    assert "rater_benchmark_supplement_tables_path" in rater_text

    assert "Rater_Benchmark_Supplement_Tables.xlsx" in analysis_text
    assert "NLP_Classifier_Supplement_Tables.xlsx" in analysis_text


def test_figure_1_uses_current_manuscript_labels_and_counts() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure1_helpers = analysis_text.split("def draw_nlp_workflow_panel(", maxsplit=1)[1].split(
        "def render_manuscript_prevalence_panels(",
        maxsplit=1,
    )[0]
    figure1_generation = analysis_text.split("#| label: hypercap-cc-nlp-analysis-cell-011b", maxsplit=1)[1].split(
        "plot_time_df =",
        maxsplit=1,
    )[0]
    figure1_text = figure1_helpers + figure1_generation

    assert "Analytic cohort, ascertainment definitions, and chief-concern NLP classification" in analysis_text
    assert "Two-panel figure showing mutually exclusive ascertainment strata and the chief-concern NLP workflow" in analysis_text
    assert "A. Cohort and ascertainment strata" in figure1_text
    assert "B. Chief-concern NLP classification" in figure1_text
    assert "Admissions linked to ED presentation\\nwith nonmissing triage chief complaint" in figure1_text
    assert "Overlapping ascertainment indicators" in figure1_text
    assert "Mutually exclusive ascertainment strata" in figure1_text
    assert "draw_compact_ascertainment_indicator_overlap_panel" not in figure1_text
    assert "C. Source-specific ascertainment overlap" not in figure1_text
    assert "Panel C shows source-specific overlap across ABG, VBG, ICD, and UNKNOWN-source gas" not in figure1_text
    assert "Both ICD and blood-gas criteria" in analysis_text
    assert "Both ICD + gas" in figure1_text
    assert "Triage chief\\ncomplaint field" in figure1_text
    assert "Normalize and\\nsegment text" in figure1_text
    assert "Embed fragments" in figure1_text
    assert "Score against\\nNHAMCS RFV prototypes" in figure1_text
    assert "Apply deterministic\\noverrides" in figure1_text
    assert "Assign up to five\\nRFV categories per admission" in figure1_text
    assert "Final classified cohort" in figure1_text
    assert "NHAMCS RFV categories" in figure1_text
    assert "draw_flow_box(" in figure1_text
    assert "draw_flow_arrow_between_boxes(" in figure1_text
    assert 'fig = plt.figure(figsize=get_figure_size("figure_1_pdf", height=5.8))' in figure1_text
    assert "fig.add_gridspec(\n    1,\n    2," in figure1_text
    assert '"Admissions linked to ED presentation with nonmissing triage chief complaint": 11941' in figure1_text
    assert '"Analytic cohort": 11941' in figure1_text
    assert '"Gas-only": 9958' in analysis_text
    assert '"Both ICD and blood-gas criteria": 1542' in analysis_text
    assert '"ICD-only": 441' in analysis_text
    assert '"ABG criteria met": 7454' in analysis_text
    assert '"VBG criteria met": 6388' in analysis_text
    assert '"ICD-positive": 1983' in analysis_text
    assert '"UNKNOWN-source gas": 1346' in analysis_text
    assert '"count_scope": count_scope' in analysis_text
    assert '"cohort_frame"' in analysis_text
    assert '"mutually_exclusive_ascertainment_stratum"' in analysis_text
    assert '"overlapping_ascertainment_indicator"' in analysis_text

    assert "biochemical criteria" not in figure1_text.lower()
    assert "Overlapping source-specific ascertainment" not in figure1_text
    assert "Mutually exclusive inclusion groups" not in figure1_text
    assert "Yale chief complaint labels" not in figure1_text
    assert "Up to 5 RFV categories per encounter" not in figure1_text
    assert "RFV1 only" not in figure1_text
    assert "Sensitivity:" not in figure1_text
    assert "first-priority RFV assignment only" not in figure1_text
    assert "Primary analysis:" not in figure1_text
    assert "Excluded: missing triage chief complaint field" not in figure1_text
    assert "n = 36 rows" not in figure1_text
    assert "17 symptom categories" not in figure1_text
    assert "Spell-correction applied" not in figure1_text
    assert "fig.suptitle(" not in figure1_text
    assert "Three-panel figure" not in figure1_text


def test_manuscript_prevalence_figures_use_admission_display_language() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    helper_body = analysis_text.split("def render_manuscript_prevalence_panels(", maxsplit=1)[1].split(
        "def direct_label_lines(",
        maxsplit=1,
    )[0]
    assert "RFV_CATEGORY_COLORS = {" in analysis_text
    for category_color in (
        '"Respiratory": "#0072B2"',
        '"Nervous": "#CC79A7"',
        '"Digestive": "#009E73"',
        '"Circulatory": "#D55E00"',
        '"Injuries & adverse effects": "#E69F00"',
        '"Other grouped RFV categories": "#7A869A"',
    ):
        assert category_color in analysis_text
    assert "return RFV_CATEGORY_COLORS.get(grouped_category, ANALYSIS_COLORS[\"neutral\"])" in analysis_text
    assert 'fig.supxlabel("Percent of admissions"' in helper_body
    assert helper_body.count('fig.supxlabel("Percent of admissions"') == 1
    assert 'axis.set_xlabel("Percent of admissions")' not in helper_body
    assert 'axis.set_xlabel("")' in helper_body
    assert "Percent of encounters" not in helper_body
    assert 'y_label: str = "Grouped presenting-concern category"' in helper_body
    assert "Grouped complaint category" not in helper_body

    for figure_key in ("figure_2_png", "figure_3_png", "figure_4_png", "figure_s1_png", "figure_s9_png"):
        figure_spec = analysis_text.split(f'"{figure_key}": {{', maxsplit=1)[1].split("    },", maxsplit=1)[0]
        assert '"caption_stub":' in figure_spec
        assert "admission-level" in figure_spec
        assert "presenting-concern" in figure_spec


def test_manuscript_prevalence_outputs_include_clustered_ci_contract() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    helper_body = analysis_text.split(
        "def summarize_multilabel_prevalence_with_clustered_ci(",
        maxsplit=1,
    )[1].split("def _first_available_datetime(", maxsplit=1)[0]
    figure2_workbook_block = analysis_text.split("figure_2_xlsx_path = write_excel_export(", maxsplit=1)[
        1
    ].split("note_export(figure_2_xlsx_path)", maxsplit=1)[0]
    figure3_workbook_block = analysis_text.split("figure_3_xlsx_path = write_excel_export(", maxsplit=1)[
        1
    ].split("note_export(figure_3_xlsx_path)", maxsplit=1)[0]
    figure4_workbook_block = analysis_text.split("figure_4_xlsx_path = write_excel_export(", maxsplit=1)[
        1
    ].split("note_export(figure_4_xlsx_path)", maxsplit=1)[0]
    table2_block = analysis_text.split("rfv_counts = (", maxsplit=1)[1].split(
        "rfv_primary_counts = (",
        maxsplit=1,
    )[0]
    uncertainty_block = analysis_text.split("prevalence_uncertainty_path = write_excel_export(", maxsplit=1)[
        1
    ].split("note_export(prevalence_uncertainty_path)", maxsplit=1)[0]

    assert "PREVALENCE_CI_BOOTSTRAP_REPLICATES = 2000" in analysis_text
    assert "PREVALENCE_CI_BOOTSTRAP_SEED = 20260607" in analysis_text
    assert "PREVALENCE_CI_CLUSTER_UNIT = \"subject_id\"" in analysis_text
    assert "cluster_weights = np.bincount(sampled_clusters" in helper_body
    assert "row_weights = cluster_weights[cluster_codes]" in helper_body
    assert "resolve_encounter_id(base)" in helper_body
    assert "missing {cluster_col}" in analysis_text

    for workbook_block in (figure2_workbook_block, figure3_workbook_block, figure4_workbook_block):
        assert '"Prevalence_Tidy"' in workbook_block
        assert '"ChartReady_CI_Lower"' in workbook_block
        assert '"ChartReady_CI_Upper"' in workbook_block

    for column_name in (
        "ci_lower",
        "ci_upper",
        "bootstrap_replicates",
        "bootstrap_seed",
        "cluster_unit",
        "n_clusters",
    ):
        assert column_name in table2_block

    assert '"prevalence_uncertainty": "Prevalence_Uncertainty.xlsx"' in analysis_text
    assert '"Table2_Canonical": rfv_counts' in uncertainty_block
    assert '"Figure2_Grouped": route_prevalence_summary' in uncertainty_block
    assert '"Figure3_Grouped": age_prevalence_summary' in uncertainty_block
    assert '"Figure4_Grouped": ph_prevalence' in uncertainty_block
    assert '"Timing_Grouped": timing_safeguard_grouped_prevalence' in uncertainty_block
    assert '"Timing_Canonical": timing_safeguard_canonical_prevalence' in uncertainty_block
    assert "include_clustered_ci=True" in analysis_text
    assert "subject_id, hadm_id, ed_stay_id, and raw chief complaint text" in analysis_text


def test_prevalence_figure_helpers_render_ci_whiskers_when_supplied() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure2_helper = analysis_text.split("def render_manuscript_prevalence_panels(", maxsplit=1)[1].split(
        "def render_age_profile_panels(",
        maxsplit=1,
    )[0]
    figure3_helper = analysis_text.split("def render_age_profile_panels(", maxsplit=1)[1].split(
        "def render_ph_severity_profile_panels(",
        maxsplit=1,
    )[0]
    figure4_helper = analysis_text.split("def render_ph_severity_profile_panels(", maxsplit=1)[1].split(
        "def render_bicarbonate_profile_panels(",
        maxsplit=1,
    )[0]

    assert "ci_lower_df: pd.DataFrame | None = None" in figure2_helper
    assert "ci_upper_df: pd.DataFrame | None = None" in figure2_helper
    assert "xerr=xerr" in figure2_helper
    assert "axis.errorbar(" in figure2_helper
    assert "fmt=\"none\"" in figure2_helper
    assert "emphasize_ci: bool = False" in figure2_helper
    assert "stem_linewidth = 1.2 if emphasize_ci else 2.2" in figure2_helper
    assert "stem_alpha = 0.65 if emphasize_ci else 1.0" in figure2_helper
    assert "elinewidth=1.4 if emphasize_ci else 0.9" in figure2_helper
    assert "capsize=3.2 if emphasize_ci else 2.4" in figure2_helper
    assert "label_x_padding = 2.0" in figure2_helper
    assert "label_bound_max = max(label_bound_max, ci_max)" in figure2_helper
    assert "label_anchor = max(label_anchor, float(upper_values.loc[category]))" in figure2_helper
    assert "label_anchor + label_x_padding" in figure2_helper
    assert "value + 0.6" not in figure2_helper
    assert '"facecolor": "white"' in figure2_helper
    assert "point_color = rfv_display_color(category)" in figure2_helper

    for helper_body in (figure3_helper, figure4_helper):
        assert "ci_lower_df: pd.DataFrame | None = None" in helper_body
        assert "ci_upper_df: pd.DataFrame | None = None" in helper_body
        assert "yerr=yerr" in helper_body
        assert "axis.errorbar(" in helper_body
        assert "fmt=\"none\"" in helper_body


def test_figure_2_emphasizes_ascertainment_route_gradient() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure2_spec = analysis_text.split('"figure_2_png": {', maxsplit=1)[1].split("    },", maxsplit=1)[0]
    figure2_block = analysis_text.split("figure_2_png_path = render_manuscript_prevalence_panels(", maxsplit=1)[
        1
    ].split("note_export(figure_2_png_path)", maxsplit=1)[0]

    assert 'figure2_route_group_order = [\n    "ICD-positive",\n    "VBG (PCO2 >= 50 mmHg)",\n    "ABG (PCO2 >= 45 mmHg)",\n]' in analysis_text
    assert 'route_group_order = [\n    "ABG (PCO2 >= 45 mmHg)",\n    "VBG (PCO2 >= 50 mmHg)",\n    "ICD-positive",\n]' in analysis_text
    assert "figure2_expected_route_group_sizes" in analysis_text
    assert '"ICD-positive": 1983' in analysis_text
    assert '"VBG (PCO2 >= 50 mmHg)": 6388' in analysis_text
    assert '"ABG (PCO2 >= 45 mmHg)": 7454' in analysis_text
    assert "figure2_route_strip_labels" in analysis_text
    assert "(N = {format_figure_n(route_group_sizes.get(group_name, 0))})" in analysis_text

    assert "overlapping ascertainment indicators" in figure2_spec
    assert "admissions may contribute to more than one panel" in figure2_spec
    assert "multi-label and not mutually exclusive" in figure2_spec
    assert "patient-cluster bootstrap 95% confidence intervals" in figure2_spec
    assert "narrative overlapping-indicator order" in figure2_spec
    assert "source-specific ascertainment routes" not in figure2_spec
    assert "source-specific ascertainment strata" not in figure2_spec
    assert '"export_type": "double-column"' in figure2_spec
    assert '"Notes": pd.DataFrame' in analysis_text
    assert "Figure 2 panels are overlapping ascertainment indicators" in analysis_text
    assert "facet_order=figure2_route_group_order" in figure2_block
    assert "facet_strip_label_map=figure2_route_strip_labels" in figure2_block
    assert "ci_lower_df=route_ci_lower_plot_df" in figure2_block
    assert "ci_upper_df=route_ci_upper_plot_df" in figure2_block
    assert 'highlight_categories={"Respiratory"}' in figure2_block
    assert 'highlight_color=ANALYSIS_COLORS["accent"]' not in figure2_block
    assert "x_max=55.0" in figure2_block
    assert "emphasize_ci=True" in figure2_block
    assert "legend" not in figure2_block.lower()


def test_ascertainment_indicator_and_stratum_vocabulary_is_documented() -> None:
    spec_text = (WORK_DIR / "docs" / "SPEC.md").read_text()
    decisions_text = (WORK_DIR / "docs" / "DECISIONS.md").read_text()
    mapping_text = (WORK_DIR / "docs" / "MANUSCRIPT_MAPPING.md").read_text()

    assert "**Ascertainment indicators** are overlapping ABG-positive, VBG-positive, and ICD-positive indicators" in spec_text
    assert "**Source-specific overlap displays** are reconciliation views across ABG, VBG, ICD, and UNKNOWN-source gas" in spec_text
    assert "Figure S6 shows all nonzero source-specific intersections" in spec_text
    assert "**Ascertainment strata** are mutually exclusive gas-only, ICD-only, and both ICD + gas groups" in spec_text
    assert "Figure 1 Panel C" not in spec_text
    assert "Figure 2 uses this overlapping indicator vocabulary" in spec_text

    assert "two-panel analytic cohort construction" in decisions_text
    assert "source-specific ABG/VBG/ICD/UNKNOWN-source gas overlap matrix" not in decisions_text
    assert "Figure 2.png/.xlsx` = grouped presenting category prevalence across overlapping ascertainment indicators" in decisions_text
    assert "expanded ascertainment-overlap figure remains supplement-only as `Figure S6`" in decisions_text
    assert "full source-specific ABG/VBG/ICD/UNKNOWN-source gas overlap" in decisions_text

    assert "Figure 1: analytic cohort construction, ascertainment definitions, and NLP workflow" in mapping_text
    assert "Figure 1: analytic cohort construction, source-specific overlap" not in mapping_text
    assert "Figure 2: presenting-concern prevalence by overlapping ascertainment indicator" in mapping_text
    assert "Figure S1-S9" in mapping_text
    assert "Figure S1-S9.pdf/.png" in spec_text
    assert "selected `Figure S*.xlsx` workbooks" in spec_text
    assert "Figure S1-S9.pdf/.xlsx" not in spec_text
    assert "submission_assets_manifest.csv" in spec_text
    assert "submission_assets_manifest.csv" in mapping_text
    assert "submission_asset_manifest.csv" not in spec_text
    assert "submission_asset_manifest.csv" not in mapping_text
    assert "Figure 2: presenting-concern prevalence by ascertainment route" not in mapping_text
    assert "Figure S1-S8" not in spec_text
    assert "Figure S1-S8" not in mapping_text


def test_figure_3_uses_ordered_age_profile_small_multiples() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure3_spec = analysis_text.split('"figure_3_png": {', maxsplit=1)[1].split("    },", maxsplit=1)[0]
    figure3_helper = analysis_text.split("def render_age_profile_panels(", maxsplit=1)[1].split(
        "def direct_label_lines(",
        maxsplit=1,
    )[0]
    figure3_block = analysis_text.split("figure_3_png_path = render_age_profile_panels(", maxsplit=1)[1].split(
        "note_export(figure_3_png_path)",
        maxsplit=1,
    )[0]

    assert "def render_age_profile_panels(" in analysis_text
    assert "figure_3_png_path = render_manuscript_prevalence_panels(" not in analysis_text
    assert "ordered age groups" in figure3_spec
    assert "multi-label and not mutually exclusive" in figure3_spec
    assert "patient-cluster bootstrap 95% confidence intervals" in figure3_spec
    assert "respiratory presenting concerns increase with age" in figure3_spec

    assert 'age_group_order = ["18–39", "40–64", "65–79", "≥80"]' in analysis_text
    assert '"18–39": 966' in analysis_text
    assert '"40–64": 4088' in analysis_text
    assert '"65–79": 4162' in analysis_text
    assert '"≥80": 2725' in analysis_text
    assert 'fig.supxlabel("Age group, years"' in figure3_helper
    assert 'fig.supylabel("Percent of admissions"' in figure3_helper
    assert "y_max=55.0" in figure3_block
    assert "ci_lower_df=age_ci_lower_plot_df" in figure3_block
    assert "ci_upper_df=age_ci_upper_plot_df" in figure3_block
    assert 'highlight_categories={"Respiratory", "Injuries & adverse effects"}' in figure3_block
    assert "line_color = rfv_display_color(category)" in figure3_helper
    assert '"Respiratory": ANALYSIS_COLORS["accent"]' not in figure3_helper
    assert '"Injuries & adverse effects": ANALYSIS_COLORS["injury"]' not in figure3_helper
    assert "figure3_respiratory_values.is_monotonic_increasing" in analysis_text
    assert "figure3_injury_values.iloc[0] <= figure3_injury_values.iloc[1:].max()" in analysis_text
    assert "legend" not in figure3_block.lower()
    assert "legend(" not in figure3_helper

    for disallowed_age_label in ("80+", "Young", "Middle-aged", "Older", "Elderly"):
        assert disallowed_age_label not in analysis_text


def test_figure_4_uses_prespecified_ph_profile_small_multiples() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure4_spec = analysis_text.split('"figure_4_png": {', maxsplit=1)[1].split("    },", maxsplit=1)[0]
    figure4_helper = analysis_text.split("def render_ph_severity_profile_panels(", maxsplit=1)[1].split(
        "def direct_label_lines(",
        maxsplit=1,
    )[0]
    figure4_block = analysis_text.split("figure_4_png_path = render_ph_severity_profile_panels(", maxsplit=1)[
        1
    ].split("note_export(figure_4_png_path)", maxsplit=1)[0]
    figure4_context = figure4_spec + figure4_helper + figure4_block
    figure4_workbook_block = analysis_text.split("figure_4_xlsx_path = write_excel_export(", maxsplit=1)[
        1
    ].split("note_export(figure_4_xlsx_path)", maxsplit=1)[0]

    assert "def render_ph_severity_profile_panels(" in analysis_text
    assert "FIGURE4_USE_PAIRED_QUALIFYING_PH = True" in analysis_text
    assert "def select_paired_qualifying_ph_for_figure4(" in analysis_text
    assert "def select_figure4_analysis_ph(" in analysis_text
    assert "qualifying_ph is absent from the cohort handoff" in analysis_text
    assert "build_acidemia_severity_denominator_audit" in analysis_text
    assert "build_acidemia_ph_source_audit" in analysis_text
    for sheet_name in (
        "Denominator_Audit",
        "pH_Source_Audit",
    ):
        assert sheet_name in figure4_workbook_block
    assert "figure_4_png_path = render_manuscript_prevalence_panels(" not in analysis_text
    assert "prespecified descriptive acidemia severity strata" in figure4_spec
    assert "multi-label and not mutually exclusive" in figure4_spec
    assert "patient-cluster bootstrap 95% confidence intervals" in figure4_spec
    assert "prespecified pH severity strata" in figure4_spec
    assert "ci_lower_df=ph_ci_lower" in figure4_block
    assert "ci_upper_df=ph_ci_upper" in figure4_block

    assert (
        'ph_severity_order = [\n'
        '    "Normal/compensated (pH ≥7.35)",\n'
        '    "Mild (7.30–7.34)",\n'
        '    "Moderate (7.25–7.29)",\n'
        '    "Severe (pH <7.25)",\n'
        "]"
    ) in analysis_text
    assert '"Normal/compensated (pH ≥7.35)": 4232' in analysis_text
    assert '"Mild (7.30–7.34)": 2595' in analysis_text
    assert '"Moderate (7.25–7.29)": 1677' in analysis_text
    assert '"Severe (pH <7.25)": 2113' in analysis_text
    assert 'fig.supxlabel("Acidemia severity stratum"' in figure4_helper
    assert 'fig.supylabel("Percent of admissions"' in figure4_helper
    assert "y_max=55.0" in figure4_block
    assert 'highlight_categories={"Respiratory", "Injuries & adverse effects"}' in figure4_block
    assert "line_color = rfv_display_color(category)" in figure4_helper
    assert '"Respiratory": ANALYSIS_COLORS["accent"]' not in figure4_helper
    assert '"Injuries & adverse effects": ANALYSIS_COLORS["injury"]' not in figure4_helper
    assert "figure4_required_ph_labels" in analysis_text
    assert "figure4_expected_ph_group_sizes" in analysis_text
    assert "legend" not in figure4_block.lower()
    assert "legend(" not in figure4_helper

    for forbidden in ("mortality", "outcome", "phenotype", "phenotypes", "pH <7.20"):
        assert forbidden not in figure4_context


def test_figure_s9_uses_bicarbonate_profile_contract() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    figure_s9_spec = analysis_text.split('"figure_s9_png": {', maxsplit=1)[1].split("    },", maxsplit=1)[0]
    figure_s9_helper = analysis_text.split("def render_bicarbonate_profile_panels(", maxsplit=1)[1].split(
        "def direct_label_lines(",
        maxsplit=1,
    )[0]
    figure_s9_block = analysis_text.split("bicarbonate_df = biochemical_df.copy()", maxsplit=1)[1].split(
        "candidate_compensation_base_df = biochemical_df.copy()",
        maxsplit=1,
    )[0]

    assert "def render_bicarbonate_profile_panels(" in analysis_text
    assert "figure_s9_png_path = render_bicarbonate_profile_panels(" in figure_s9_block
    assert "figure_s9_xlsx_path = write_excel_export(" in figure_s9_block
    assert "blood-gas-ascertained admissions with available bicarbonate" in figure_s9_spec
    assert "multi-label and not mutually exclusive" in figure_s9_spec
    assert "descriptive evidence of acid-base compensation" in figure_s9_spec
    assert "definitive chronicity classification" in figure_s9_spec

    assert (
        'bicarbonate_stratum_order = [\n'
        '    "Low/normal bicarbonate (HCO3 <28)",\n'
        '    "Mildly elevated bicarbonate (HCO3 28–31)",\n'
        '    "Markedly elevated bicarbonate (HCO3 ≥32)",\n'
        "]"
    ) in analysis_text
    assert "bins=[-np.inf, 28.0, 32.0, np.inf]" in figure_s9_block
    assert "right=False" in figure_s9_block
    assert "first_hco3" in figure_s9_block
    assert "first_hco3_source" in figure_s9_block
    assert "first_hco3_qc_pass_mask(bicarbonate_df)" in figure_s9_block
    assert "first_hco3_qc_flag" in analysis_text
    assert "hco3_band" not in figure_s9_spec + figure_s9_helper + figure_s9_block

    assert 'fig.supxlabel("Bicarbonate stratum, mmol/L"' in figure_s9_helper
    assert 'fig.supylabel("Percent of admissions"' in figure_s9_helper
    assert "ordered_rfv_display_categories(category_order)" in figure_s9_helper
    assert 'highlight_categories={"Respiratory", "Injuries & adverse effects"}' in figure_s9_block
    assert "line_color = rfv_display_color(category)" in figure_s9_helper
    assert '"Respiratory": ANALYSIS_COLORS["accent"]' not in figure_s9_helper
    assert '"Injuries & adverse effects": ANALYSIS_COLORS["injury"]' not in figure_s9_helper
    assert "legend" not in figure_s9_block.lower()
    assert "legend(" not in figure_s9_helper

    for sheet_name in (
        "Prevalence_Tidy",
        "ChartReady_Pivot",
        "Bicarbonate_Source_Summary",
        "Bicarbonate_Strata_N",
    ):
        assert sheet_name in figure_s9_block
    for column_name in ("numerator", "denominator", "percent_of_admissions", "first_hco3_source_summary"):
        assert column_name in figure_s9_block
    assert '"figure_s9_png": len(bicarbonate_df)' in analysis_text


def test_candidate_compensation_matrix_is_review_only_heatmap() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    manuscript_filename_body = analysis_text.split("MANUSCRIPT_ASSET_FILENAMES = {", maxsplit=1)[1].split(
        "SECONDARY_OUTPUT_FILENAMES = {",
        maxsplit=1,
    )[0]
    secondary_filename_body = analysis_text.split("SECONDARY_OUTPUT_FILENAMES = {", maxsplit=1)[1].split(
        "FIGURE_SPECS = {",
        maxsplit=1,
    )[0]
    allowlist_body = analysis_text.split("def build_submission_asset_allowlist(", maxsplit=1)[1].split(
        "def validate_submission_asset_bundle(",
        maxsplit=1,
    )[0]
    candidate_spec = analysis_text.split('"candidate_compensation_matrix_plot": {', maxsplit=1)[1].split(
        "    },",
        maxsplit=1,
    )[0]
    candidate_helper = analysis_text.split("def render_compensation_matrix_heatmap(", maxsplit=1)[1].split(
        "def direct_label_lines(",
        maxsplit=1,
    )[0]
    candidate_block = analysis_text.split("candidate_compensation_base_df = biochemical_df.copy()", maxsplit=1)[
        1
    ].split("ph_sensitivity_counts = (", maxsplit=1)[0]

    assert '"candidate_compensation_matrix_plot": "Candidate_Figure_Compensation_Matrix.png"' in secondary_filename_body
    assert (
        '"candidate_compensation_matrix_workbook": "Candidate_Figure_Compensation_Matrix.xlsx"'
        in secondary_filename_body
    )
    assert "Candidate_Figure_Compensation_Matrix" not in manuscript_filename_body
    assert "Candidate_Figure_Compensation_Matrix" not in allowlist_body

    assert "descriptive pH and bicarbonate evidence groups" in candidate_spec
    assert "multi-label and not mutually exclusive" in candidate_spec
    assert "definitive chronicity classification" in candidate_spec
    assert "causal" not in candidate_spec.lower()
    assert "Figure 5" not in candidate_spec
    assert "Figure S10" not in candidate_spec

    assert "def select_analysis_ph(" in analysis_text
    assert "def select_analysis_ph_with_source(" in analysis_text
    assert "def first_hco3_qc_pass_mask(" in analysis_text
    assert "def render_compensation_matrix_heatmap(" in analysis_text
    assert 'lab_abg_ph = maybe_numeric(df, ["lab_abg_ph"])' in analysis_text
    assert 'maybe_numeric(df, ["lab_vbg_ph"])' in analysis_text
    assert 'maybe_numeric(df, ["first_ph", "min_ph_0_24h"])' in analysis_text
    assert "first_hco3_qc_flag" in analysis_text

    assert (
        'candidate_compensation_matrix_group_order = [\n'
        '    "Acidemic, HCO3 <28",\n'
        '    "Acidemic, HCO3 ≥28",\n'
        '    "Normal/alkalemic, HCO3 <28",\n'
        '    "Normal/alkalemic, HCO3 ≥28",\n'
        "]"
    ) in analysis_text
    for criterion in (
        "pH <7.35 and HCO3 <28",
        "pH <7.35 and HCO3 ≥28",
        "pH ≥7.35 and HCO3 <28",
        "pH ≥7.35 and HCO3 ≥28",
    ):
        assert criterion in analysis_text

    assert "sns.heatmap(" in candidate_helper
    assert "LinearSegmentedColormap.from_list(" in candidate_helper
    assert "ordered_rfv_display_categories(RFV_GROUP_ORDER)" in candidate_helper
    assert "format_figure_pct" in candidate_helper
    assert "annot=cell_labels.to_numpy()" in candidate_helper
    assert "cbar=False" in candidate_helper
    assert "legend" not in candidate_helper.lower()
    assert 'ax.set_xlabel("Descriptive acid-base group")' in candidate_helper
    assert 'ax.set_ylabel("Grouped presenting-concern category")' in candidate_helper

    assert "candidate_compensation_matrix_workbook_path = write_excel_export(" in candidate_block
    for sheet_name in (
        "Prevalence_Tidy",
        "ChartReady_Matrix",
        "Group_Denominators",
        "Missingness_Summary",
        "HCO3_Source_Summary",
    ):
        assert sheet_name in candidate_block
    for column_name in (
        "numerator",
        "denominator",
        "percent_of_admissions",
        "pH_criterion",
        "HCO3_criterion",
        "blood_gas_subset_denominator",
        "first_hco3_source",
    ):
        assert column_name in candidate_block
    assert "candidate_compensation_df.empty" in candidate_block
    assert "candidate_compensation_missingness categories do not reconcile" not in candidate_block
    assert "Candidate compensation matrix missingness categories do not reconcile" in candidate_block
    assert "Candidate compensation matrix group denominators do not sum" in candidate_block
    assert "Candidate compensation matrix plot values drifted from Prevalence_Tidy" in candidate_block
    assert '"candidate_compensation_matrix_plot": len(candidate_compensation_df)' in analysis_text


def test_grouped_rfv_figure_specs_use_presenting_concern_language() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    grouped_rfv_figure_keys = (
        "figure_2_png",
        "figure_3_png",
        "figure_4_png",
        "figure_s1_png",
        "figure_s7_png",
        "figure_s8_png",
        "figure_s9_png",
        "time_to_gas_plot",
        "recognition_by_pco2_plot",
        "etiology_plot",
        "candidate_compensation_matrix_plot",
        "ventilation_regression_plot",
    )
    disallowed_display_terms = (
        "complaint category",
        "complaint categories",
        "presenting complaint",
        "symptom category",
        "symptom categories",
        "Grouped symptom category",
    )

    for figure_key in grouped_rfv_figure_keys:
        figure_spec = analysis_text.split(f'"{figure_key}": {{', maxsplit=1)[1].split("    },", maxsplit=1)[0]
        assert "presenting-concern" in figure_spec
        for term in disallowed_display_terms:
            assert term not in figure_spec

    assert 'ax.set_ylabel("Grouped presenting-concern category")' in analysis_text
    assert 'ax.set_ylabel("Grouped symptom category")' not in analysis_text
    assert "chief-concern NLP" in analysis_text


def test_analysis_notebook_replaces_reyan_as_the_canonical_manuscript_stage() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    assert "single deterministic workflow for the merged analysis plus manuscript-facing figures/tables stage" in analysis_text
    assert "former Reyan manuscript-facing figures/tables stage" in analysis_text
    assert "legacy_manuscript_names_absent_from_registry" in analysis_text


def test_public_release_excludes_legacy_compatibility_notebooks() -> None:
    notebook_names = (
        "MIMICIV_hypercap_EXT_cohort.ipynb",
        "Hypercap CC NLP Classifier.ipynb",
        "Rater Agreement Analysis.ipynb",
        "Hypercap CC NLP Analysis.ipynb",
    )
    for notebook_name in notebook_names:
        assert not (WORK_DIR / notebook_name).exists()
        assert not (WORK_DIR / "Legacy Code" / notebook_name).exists()
    assert not (WORK_DIR / "Legacy Code").exists()


def test_makefile_public_release_is_quarto_only() -> None:
    makefile_text = (WORK_DIR / "Makefile").read_text()
    assert "notebook-cohort:" not in makefile_text
    assert "notebook-pipeline:" not in makefile_text
    assert "Legacy Code/" not in makefile_text

    completed = subprocess.run(
        ["make", "-n", "quarto-pipeline"],
        cwd=WORK_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    assert 'quarto render "MIMICIV_hypercap_EXT_cohort.qmd"' in completed.stdout
    assert 'quarto render "Hypercap CC NLP Classifier.qmd"' in completed.stdout


def test_public_release_tree_excludes_generated_and_private_roots() -> None:
    for relative_path in (
        "artifacts",
        "debug",
        "Drafts",
        "Legacy Code",
        "MIMIC tabular data",
        "outputs",
        "Results",
        "tmp",
    ):
        tracked = subprocess.run(
            ["git", "ls-files", "--", relative_path],
            cwd=WORK_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        assert tracked.stdout.strip() == ""
        if (WORK_DIR / relative_path).exists():
            ignored = subprocess.run(
                ["git", "check-ignore", "-q", relative_path],
                cwd=WORK_DIR,
                check=False,
            )
            assert ignored.returncode == 0


def test_public_release_git_hygiene_patterns() -> None:
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=WORK_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked_paths = completed.stdout.splitlines()
    forbidden_path_pattern = re.compile(
        r"(^MIMIC tabular data/|^Drafts/|^Results/|^debug/|^outputs/|"
        r"^artifacts/|^tmp/|^Legacy Code/|^\.codex/|^\.jupyter/|"
        r"\.DS_Store$|\.Rhistory$)"
    )
    forbidden_matches = [
        path for path in tracked_paths if forbidden_path_pattern.search(path)
    ]
    assert forbidden_matches == []

    forbidden_binary_suffixes = {
        ".jpeg",
        ".jpg",
        ".parquet",
        ".png",
        ".pptx",
        ".tif",
        ".tiff",
        ".xls",
        ".xlsx",
        ".zip",
    }
    tracked_binary_paths = [
        WORK_DIR / path
        for path in tracked_paths
        if Path(path).suffix.casefold()
        in forbidden_binary_suffixes | {".docx", ".pdf"}
    ]
    assert not [
        path for path in tracked_binary_paths if path.suffix.casefold() in forbidden_binary_suffixes
    ]

    for binary_path in tracked_binary_paths:
        notice_path = binary_path.parent / "README.md"
        assert notice_path.is_file()
        notice_text = notice_path.read_text().casefold()
        if binary_path.suffix.casefold() == ".docx":
            assert "not intended for use" in notice_text
            assert "files are stored unchanged" in notice_text
        else:
            digest = hashlib.sha256(binary_path.read_bytes()).hexdigest()
            assert digest in notice_text
            assert "unchanged copy" in notice_text


def test_makefile_and_readme_document_analysis_as_the_merged_stage() -> None:
    makefile_text = (WORK_DIR / "Makefile").read_text()
    readme_text = (WORK_DIR / "README.md").read_text()
    assert "quarto-reyan-figures:" in makefile_text
    assert 'quarto render "Hypercap CC NLP Analysis.qmd" --to pdf --output-dir "$(RESULTS_DIR)" --output "Hypercap CC NLP Analysis.pdf"' in makefile_text
    assert '$(RESULTS_DIR)/Figure 4.png' in makefile_text
    assert '$(RESULTS_DIR)/Figure S1.png' in makefile_text
    assert 'REPORT_DIR ?= artifacts/reports/$(REPORT_RUN_ID)' not in makefile_text
    assert "make quarto-reyan-figures" in readme_text
    assert "Hypercap CC NLP Analysis.qmd" in readme_text
    assert "Figure 1" in readme_text
    assert "Figure 4" in readme_text
    assert "Figure S1" in readme_text
    assert "Figure S9" in readme_text
    assert "submission_assets_manifest.csv" in readme_text
    assert "analysis_manifest.yml" in readme_text
    assert "submission_manifest.xlsx" in readme_text
    assert "OUTPUTS_README.md" in readme_text
    assert "submission_asset_manifest.csv" not in readme_text
    assert "Presenting-concern prevalence by overlapping ascertainment indicator" in readme_text
    assert "Presenting-concern prevalence by ascertainment route" not in readme_text
    assert "Hypercap CC NLP Reyan Figures.qmd" not in readme_text


def test_manuscript_mapping_lists_revision_manifest_outputs() -> None:
    mapping_text = (WORK_DIR / "docs" / "MANUSCRIPT_MAPPING.md").read_text()

    assert "Run-level submission manifest and output README" in mapping_text
    assert "submission_manifest.xlsx" in mapping_text
    assert "submission_manifest.csv" in mapping_text
    assert "OUTPUTS_README.md" in mapping_text
    assert "Aggregate acid-base missingness, candidate definitions, and sensitivity suite" in mapping_text
    assert "Supplementary_Table_Acid_Base_Source_Missingness.xlsx" in mapping_text
    assert "Candidate_Definition_Yield_Composition.xlsx" in mapping_text
    assert "Sensitivity_Analysis_Suite.xlsx" in mapping_text


def test_classifier_notebook_contains_spell_mode_comparison_and_audit() -> None:
    classifier_text = (WORK_DIR / "Hypercap CC NLP Classifier.qmd").read_text()
    assert 'scoring_method: str = "max"' in classifier_text
    assert "group_scores_from_proto_row(" in classifier_text
    assert "score_one_segment_soft(" in classifier_text
    assert "CC_SPELL_CORRECTION_MODE" in classifier_text
    assert "SPELL_CORRECTION_MODES" in classifier_text
    assert "SymSpell(max_dictionary_edit_distance=1" in classifier_text
    assert "choose_spell_mode(" in classifier_text
    assert "classifier_spell_mode_comparison.csv" in classifier_text
    assert "classifier_spellfix_log.csv" in classifier_text
    assert "classifier_spellfix_guardrail_audit.csv" in classifier_text
    assert "classifier_phrase_guardrail_cases.csv" in classifier_text
    assert "SPELL_PROTECT_PHRASES" in classifier_text
    assert "LEMMA_PROTECT_PHRASES" in classifier_text
    assert "SPELL_DENYLIST_SUBSTITUTIONS" in classifier_text
    assert '("femer", "fever")' in classifier_text
    assert '("black", "back")' in classifier_text
    assert '"femer": "femur"' in classifier_text
    assert '"trach": "tracheostomy"' in classifier_text
    assert '"mvc": "motor vehicle collision"' in classifier_text
    assert '"mva": "motor vehicle accident"' in classifier_text
    assert "bleed_token_truncation" in classifier_text
    assert "femer_to_fever" in classifier_text
    assert "RX_NEURO_BLEED" in classifier_text
    assert "RX_NEURO_BLEED_TRAUMA_CONTEXT" in classifier_text
    assert "RX_NEURO_BLEED_DIAGNOSIS_CONTEXT" in classifier_text
    assert "run_phrase_guardrail_suite" in classifier_text
    assert "HEAD BLEED" in classifier_text
    assert "Upper GI bleed" in classifier_text
    assert "FEMER FX" in classifier_text
    assert '"aaa": "abdominal aortic aneurysm"' in classifier_text
    assert '"dka": "diabetic ketoacidosis"' in classifier_text
    assert '"pnx": "pneumothorax"' in classifier_text
    assert '"lle": "left leg"' in classifier_text
    assert '"rle": "right leg"' in classifier_text
    assert '"lue": "left arm"' in classifier_text
    assert '"rue": "right arm"' in classifier_text
    assert '"ble": "bilateral leg"' in classifier_text
    assert "RX_EXPLICIT_DISEASE_DIAGNOSIS" in classifier_text
    assert "RX_ABSCESS_DISEASE" in classifier_text
    assert "RX_SWELLING_LOWER_EXT" in classifier_text
    assert "RX_SWELLING_GU" in classifier_text
    assert "RX_SWELLING_FACIAL" in classifier_text
    assert "RX_SWELLING_ABDOMINAL" in classifier_text
    assert "EXACT_UNCODABLE_SEGMENTS" in classifier_text
    assert "_should_force_uncodable_segment(" in classifier_text
    assert "AAA" in classifier_text
    assert "ruptured AAA" in classifier_text
    assert "DKA" in classifier_text
    assert "hyperglycemia" in classifier_text
    assert "PNX" in classifier_text
    assert "pleural effusion" in classifier_text
    assert "facial swelling" in classifier_text
    assert "testicular swelling" in classifier_text
    assert "abdominal distention" in classifier_text
    assert "perforation" in classifier_text
    assert "unresolved_short_segment_nonuncodable_rows" in classifier_text
    assert "glycemia_nondisease_rows" in classifier_text
    assert "abscess_nondisease_rows" in classifier_text
    assert "lower_ext_swelling_noncirculatory_rows" in classifier_text
    assert "gu_swelling_nongu_rows" in classifier_text
    assert "facial_swelling_nonskin_rows" in classifier_text
    assert "abdominal_swelling_nondigestive_rows" in classifier_text
    assert "integrity_violation_total" in classifier_text
    assert "scoring_config" in classifier_text
    assert "classifier_export_drop_columns" in classifier_text
    assert "visit_candidate_scores.csv" in classifier_text
    assert "segment_candidate_scores.csv" in classifier_text
    assert "CLASSIFIER_ANNOTATION_BENCHMARK_MODE" in classifier_text
    assert "CLASSIFIER_ANNOTATION_BENCHMARK_PATH" in classifier_text
    assert "CLASSIFIER_ANNOTATION_BENCHMARK_SHEET" in classifier_text
    assert "CLASSIFIER_ANNOTATION_CC_COLUMN" in classifier_text
    assert "annotation_benchmark_with_NLP.xlsx" in classifier_text
    assert "annotation_visit_candidate_scores.csv" in classifier_text
    assert "annotation_segment_candidate_scores.csv" in classifier_text
    assert 'early_annotation_benchmark_mode == "required"' in classifier_text
    assert "assign_annotation_row_id(" in classifier_text
    assert "annotation_benchmark_status" in classifier_text
    assert "validate_candidate_sidecars(" in classifier_text
    assert '"cc_missing_flag"' in classifier_text
    assert '"cc_pseudomissing_flag"' in classifier_text


def test_rater_direct_benchmark_does_not_require_cohort_workbook() -> None:
    rater_text = (WORK_DIR / "Rater Agreement Analysis.qmd").read_text()
    config_block = rater_text.split(
        "CANONICAL_NLP_FILENAME = ",
        maxsplit=1,
    )[1].split("# Fail fast for the benchmark source that will actually be used.", maxsplit=1)[0]
    preflight_block = rater_text.split(
        "# Fail fast for the benchmark source that will actually be used.",
        maxsplit=1,
    )[1].split("# Ensure output folders exist before writing artifacts.", maxsplit=1)[0]
    cohort_audit_block = rater_text.split(
        "def _cohort_overlap_not_run(",
        maxsplit=1,
    )[1].split('cohort_overlap_join_audit["benchmark_source"] = "cohort_overlap"', maxsplit=1)[0]

    assert "def resolve_rater_data_path(" in config_block
    assert "resolve_rater_nlp_input_path(" not in rater_text
    assert "resolve_rater_candidate_sidecar_path(" not in rater_text
    assert "Expected rater NLP input workbook was not found" not in rater_text
    assert "Expected classifier candidate sidecar was not found" not in rater_text
    assert "nlp_path = resolve_rater_data_path(" in config_block
    assert "visit_candidate_scores_path = resolve_rater_data_path(" in config_block
    assert "segment_candidate_scores_path = resolve_rater_data_path(" in config_block
    assert "DIRECT_ANNOTATION_BENCHMARK_PATHS" in preflight_block
    assert "COHORT_OVERLAP_BENCHMARK_PATHS" in preflight_block
    assert 'BENCHMARK_SOURCE_REQUESTED == "annotation_direct"' in preflight_block
    annotation_direct_block = preflight_block.split(
        'if BENCHMARK_SOURCE_REQUESTED == "annotation_direct":',
        maxsplit=1,
    )[1].split('elif BENCHMARK_SOURCE_REQUESTED == "cohort_overlap":', maxsplit=1)[0]
    assert "required_paths = DIRECT_ANNOTATION_BENCHMARK_PATHS" in annotation_direct_block
    assert "COHORT_OVERLAP_BENCHMARK_PATHS" not in annotation_direct_block
    assert "elif DIRECT_BENCHMARK_COMPLETE:" in preflight_block
    assert "for path in (ANNOTATION_PATH, NLP_PATH" not in preflight_block
    assert "Required benchmark input file not found" in preflight_block

    assert "cohort_overlap_not_run_for_direct_benchmark" in cohort_audit_block
    assert '"coverage_status": "not_run"' in cohort_audit_block
    assert "except Exception as exc:" in cohort_audit_block
    assert "if not use_direct_annotation_benchmark:" in cohort_audit_block
    assert "raise" in cohort_audit_block


def test_rater_notebook_contains_key_inventory_and_canonical_mapping() -> None:
    rater_text = (WORK_DIR / "Rater Agreement Analysis.qmd").read_text()
    assert "R3_vs_NLP_key_inventory.csv" in rater_text
    assert "R3_vs_NLP_cohort_overlap_key_inventory.csv" in rater_text
    assert "R3_vs_NLP_label_mapping_audit.csv" in rater_text
    assert "join_key_diagnostics.csv" in rater_text
    assert "cohort_overlap_join_key_diagnostics.csv" in rater_text
    assert "join_audit_summary.csv" in rater_text
    assert "cohort_overlap_join_audit_summary.csv" in rater_text
    assert "unmatched_adjudicated_rows.csv" in rater_text
    assert "unmatched_nlp_rows_sample.csv" in rater_text
    assert "target_sample_n" in rater_text
    assert "warn_below_target_fail_on_zero" in rater_text
    assert "build_join_key_diagnostics(" in rater_text
    assert "build_annotation_direct_join(" in rater_text
    assert "annotation_row_id" in rater_text
    assert "canonicalize_rvc_code" in rater_text
    assert "Human inter-rater agreement was evaluated in the full adjudicated sample" in rater_text
    assert "direct annotation benchmark" in rater_text
    assert "final-cohort overlap join remains a secondary diagnostic" in rater_text
    assert "RATER_BENCHMARK_SOURCE" in rater_text
    assert "R3_vs_NLP_cohort_overlap_join_audit.json" in rater_text
    assert "R3_vs_NLP_per_category_prf.csv" in rater_text
    assert "R3_vs_NLP_per_category_bootstrap_ci.csv" in rater_text
    assert "R3_vs_NLP_set_agreement_bootstrap_ci.csv" in rater_text
    assert "R3_vs_NLP_confusion_matrix_canonical.csv" in rater_text
    assert "R3_vs_NLP_confusion_matrix_grouped.csv" in rater_text
    assert "R3_vs_NLP_disagreement_examples_redacted.csv" in rater_text
    assert "BOOTSTRAP_SEED = 20260607" in rater_text
    assert "Partial agreement is defined as non-exact agreement" in rater_text
    disagreement_block = rater_text.split("disagreement_candidates = pd.DataFrame(", maxsplit=1)[
        1
    ].split("for forbidden_column", maxsplit=1)[0]
    for forbidden_column in ("subject_id", "hadm_id", "ed_stay_id"):
        assert forbidden_column not in disagreement_block
    assert "chief_complaint_redacted" in disagreement_block
    assert "display_path(" in rater_text
    assert "non_exact_visit_n" in rater_text
    assert "binary_disagreement_n" in rater_text
    assert "category_prevalence_nonzero_n" in rater_text
    assert "RATER_VISIT_CANDIDATE_SCORES_FILENAME" in rater_text
    assert "R3_vs_NLP_headline_metrics.csv" in rater_text
    assert "R3_vs_NLP_topk_summary.csv" in rater_text
    assert "R3_vs_NLP_cardinality_summary.csv" in rater_text
    assert "R3_vs_NLP_stratified_validation.xlsx" in rater_text
    assert "R3_vs_NLP_threshold_sweep.csv" in rater_text
    assert "topk_metrics_by_visit(" in rater_text
    visit_candidate_helper = rater_text.split(
        "def _prediction_sets_from_visit_candidates(",
        maxsplit=1,
    )[1].split("def _prediction_sets_from_segment_candidates(", maxsplit=1)[0]
    assert "uncodable_threshold: float | None = None" in visit_candidate_helper
    assert "uncodable_candidates" in visit_candidate_helper
    assert "if not emitted and uncodable_threshold is not None:" in visit_candidate_helper
    assert "emitted = {UNCODABLE_RVC_CODE}" in visit_candidate_helper
    visit_uncodable_block = rater_text.split(
        "for uncodable_threshold in VISIT_UNCODABLE_THRESHOLDS:",
        maxsplit=1,
    )[1].split("sweep = pd.DataFrame(rows)", maxsplit=1)[0]
    assert "uncodable_threshold=uncodable_threshold" in visit_uncodable_block


def test_chart_review_notebook_avoids_runtime_package_installs() -> None:
    chart_review_text = (WORK_DIR / "Chart Review Sample Calc.qmd").read_text()
    assert not re.search(r"^\s*install\.packages\s*\(", chart_review_text, re.MULTILINE)
    assert "requireNamespace" in chart_review_text


def test_cohort_notebook_requires_manifest_hco3_and_poc_fallback_guard() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "lab.get(\"po2_itemids\"" in cohort_text
    assert "icu.get(\"po2_itemids\"" in cohort_text
    assert "icu.get(\"pco2_unknown_itemids\"" in cohort_text
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
    assert "1 AS pco2_threshold_any" in cohort_text
    assert "IF(q.dt_qualifying_hypercapnia_hours <= 24.0, 1, 0) AS pco2_threshold_0_24h" in cohort_text
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
    assert "poc_qualifying_earliest_0_24h_hadm_n" in cohort_text
    assert "poc_qualifying_any_type_0_24h_hadm_n" in cohort_text
    assert '"flag_any_gas_hypercapnia_poc"' in cohort_text
    assert '"flag_any_gas_hypercapnia"' in cohort_text


def test_cohort_notebook_drops_redundant_export_columns() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "cohort_export_drop_columns" in cohort_text
    assert "model_overwrite_audit.csv" in cohort_text
    assert "model_overwrite_threshold = 0.05" in cohort_text
    assert '"first_pco2"' in cohort_text
    assert '"ed_gender"' in cohort_text
    assert '"ed_race"' in cohort_text
    assert '"ed_intime_first"' in cohort_text
    assert '"age_at_admit"' in cohort_text
    assert '"first_gas_time"' in cohort_text
    assert '"dt_first_qualifying_gas_hours"' in cohort_text
    assert '"lab_other_ph"' in cohort_text
    assert '"lab_other_paco2"' in cohort_text
    assert '"lab_other_time"' in cohort_text
    assert '"hospital_expire_flag"' in cohort_text
    assert '"enrolled_any"' in cohort_text
    assert '"enrolled_any_icd_union_secondary"' in cohort_text
    assert '"gas_source_unknown_rate"' in cohort_text
    assert '"gas_source_other_rate"' in cohort_text
    assert '"gas_source_inference_primary_tier"' in cohort_text
    assert '"lab_abg_po2"' in cohort_text
    assert '"poc_abg_po2"' in cohort_text
    assert '"ed_triage_hr_model"' in cohort_text
    assert '"ed_first_temp_model"' in cohort_text
    assert '"ed_triage_temp_f_clean"' in cohort_text
    assert '"ed_triage_pain_clean"' in cohort_text
    assert '"ed_first_o2sat_model"' in cohort_text
    assert '"first_ph_model"' in cohort_text
    assert '"first_pco2_model"' in cohort_text
    assert '"first_lactate_model"' in cohort_text
