from __future__ import annotations

from pathlib import Path
import re
import subprocess


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
    assert "artifacts/qa/cohort/gas_source_diagnostics_by_ed_stay.csv" in spec_text


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
    assert "Table 1.pdf" in analysis_text
    assert "Table 2.pdf" in analysis_text
    assert "Sensitivity_Primary_Label_Table_2.xlsx" in analysis_text
    assert "Sensitivity_Primary_Label_Route_Prevalence.png" in analysis_text
    assert "Sensitivity_Primary_Label_Age_Prevalence.png" in analysis_text
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
    assert 'table1_row("In-hospital death", "death_in_hosp", TABLE1_SUMMARY_COUNT_PCT)' in analysis_text
    assert 'table1_row("Hospital length of stay, days", "hosp_los_days", TABLE1_SUMMARY_MEDIAN_IQR)' in analysis_text
    assert 'table1_row("ICU length of stay, days", "icu_los_days", TABLE1_SUMMARY_MEDIAN_IQR)' in analysis_text


def test_submission_asset_bundle_has_clean_allowlist_and_manifest() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert 'SUBMISSION_ASSET_DIRNAME = "submission_assets"' in analysis_text
    assert 'SUBMISSION_ASSET_MANIFEST_FILENAME = "submission_asset_manifest.csv"' in analysis_text
    assert 'SUBMISSION_OPTIONAL_MISSING_FILENAME = "submission_asset_optional_missing.csv"' in analysis_text
    assert "SUBMISSION_OPTIONAL_UPSTREAM_FILENAMES" in analysis_text
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
    for asset_name in (
        "Figure 1.pdf",
        "Figure 2.pdf",
        "Figure 3.pdf",
        "Figure 4.pdf",
        "Table 1.xlsx",
        "Table 2.xlsx",
        "Figure 2.xlsx",
        "Figure 3.xlsx",
        "Figure 4.xlsx",
        "Figure S1.pdf",
        "Figure S2.pdf",
        "Figure S3.pdf",
        "Figure S4.pdf",
        "Figure S5.pdf",
        "Figure S6.pdf",
        "Figure S7.pdf",
        "Figure S8.pdf",
        "Figure S1.xlsx",
        "Figure S6.xlsx",
        "Figure S7.xlsx",
        "Figure S8.xlsx",
        "Rater_Benchmark_Supplement_Tables.xlsx",
        "NLP_Classifier_Supplement_Tables.xlsx",
    ):
        assert asset_name in allowlist_body

    supplement_table_block = allowlist_body.split("supplement_table_assets = [", maxsplit=1)[1].split(
        "    ]",
        maxsplit=1,
    )[0]
    assert "Rater_Benchmark_Supplement_Tables.xlsx" in supplement_table_block
    assert '"rater"' in supplement_table_block
    assert "NLP_Classifier_Supplement_Tables.xlsx" in supplement_table_block
    assert '"classifier"' in supplement_table_block
    supplement_table_append_block = allowlist_body.split(
        "for filename, manuscript_location, included_reason, producer_stage in supplement_table_assets:",
        maxsplit=1,
    )[1].split("    return rows", maxsplit=1)[0]
    assert "required_asset=False" in supplement_table_append_block
    assert "producer_stage=producer_stage" in supplement_table_append_block
    assert '"required_asset"' in analysis_text
    assert '"producer_stage"' in analysis_text
    assert "if bool(spec.get(\"required_asset\", True)):" in analysis_text
    assert "Missing required allowlisted submission asset" in analysis_text
    assert "missing_optional_rows.append" in analysis_text
    assert "optional_upstream_asset_missing" in analysis_text
    assert "validate_submission_asset_bundle(bundle_dir, manifest_df, missing_optional_df)" in analysis_text
    assert "analysis_export_registry.pop(\"submission_asset_optional_missing\", None)" in analysis_text

    for forbidden_pattern in (
        "Clinical_Outcomes",
        "Outcome_Rates",
        "Ventilation_Regression",
        "Ventilation_Strategy",
        "RFV_Prevalence_by_Comorbidity",
        "Time_To_Gas_By_Symptom_Category",
        "Recognition_By_PCO2",
        "qa",
        "debug",
    ):
        assert forbidden_pattern in analysis_text
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
    assert target_blocks.count('ax.set_ylabel("Percent of admissions")') >= 4
    assert target_blocks.count('ax.legend(title="First-priority RFV group"') >= 4
    assert "Percent of encounters" not in target_blocks
    assert "Primary RFV group" not in target_blocks


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
        "threshold_summary",
    ):
        assert sheet_name in rater_text
    assert "rater_benchmark_supplement_tables_path" in rater_text

    assert "Rater_Benchmark_Supplement_Tables.xlsx" in analysis_text
    assert "NLP_Classifier_Supplement_Tables.xlsx" in analysis_text


def test_figure_1_uses_current_manuscript_labels_and_counts() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    assert "Analytic cohort construction and triage chief complaint field NLP workflow" in analysis_text
    assert "Blood-gas subset\\n(qualifying gas criteria)" in analysis_text
    assert "Overlapping source evidence" in analysis_text
    assert "Both ICD and blood-gas criteria" in analysis_text
    assert "Embedding + prototype scoring\\n(NHAMCS RFV prototypes)" in analysis_text
    assert "Up to 5 RFV categories per admission" in analysis_text
    assert "Sensitivity: first-priority RFV assignment only" in analysis_text
    assert "NHAMCS RFV categories; N" in analysis_text
    assert '"Gas-only": 9958' in analysis_text
    assert '"Both ICD and blood-gas criteria": 1542' in analysis_text
    assert '"ICD-only": 441' in analysis_text
    assert '"ABG criteria met": 7454' in analysis_text
    assert '"VBG criteria met": 6388' in analysis_text
    assert '"ICD-positive": 1983' in analysis_text
    assert '"count_scope": count_scope' in analysis_text
    assert '"mutually_exclusive_route"' in analysis_text
    assert '"overlapping_source_evidence"' in analysis_text

    assert "Biochemical subset\\n(qualifying gas criteria)" not in analysis_text
    assert "Yale chief complaint labels" not in analysis_text
    assert "Up to 5 RFV categories per encounter" not in analysis_text
    assert "Sensitivity: RFV1 only" not in analysis_text
    assert "symptom categories; N" not in analysis_text


def test_manuscript_prevalence_figures_use_admission_display_language() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()

    helper_body = analysis_text.split("def render_manuscript_prevalence_panels(", maxsplit=1)[1].split(
        "def direct_label_lines(",
        maxsplit=1,
    )[0]
    assert 'axis.set_xlabel("Percent of admissions")' in helper_body
    assert "Percent of encounters" not in helper_body
    assert 'y_label: str = "Grouped presenting-concern category"' in helper_body
    assert "Grouped complaint category" not in helper_body

    for figure_key in ("figure_2_png", "figure_3_png", "figure_4_png", "figure_s1_png"):
        figure_spec = analysis_text.split(f'"{figure_key}": {{', maxsplit=1)[1].split("    },", maxsplit=1)[0]
        assert '"caption_stub":' in figure_spec
        assert "admission-level" in figure_spec
        assert "presenting-concern" in figure_spec


def test_grouped_rfv_figure_specs_use_presenting_concern_language() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    grouped_rfv_figure_keys = (
        "figure_2_png",
        "figure_3_png",
        "figure_4_png",
        "figure_s1_png",
        "figure_s7_png",
        "figure_s8_png",
        "time_to_gas_plot",
        "recognition_by_pco2_plot",
        "etiology_plot",
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
    assert "chief complaint field" in analysis_text


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
    forbidden_pattern = re.compile(
        r"(^MIMIC tabular data/|^Drafts/|^Results/|^debug/|^outputs/|"
        r"^artifacts/|^tmp/|^Legacy Code/|^\.codex/|^\.jupyter/|"
        r"\.DS_Store$|\.Rhistory$|\.(xlsx|xls|docx|pptx|pdf|png|jpg|jpeg|tif|tiff|zip|parquet)$)"
    )
    forbidden_matches = [path for path in tracked_paths if forbidden_pattern.search(path)]
    assert forbidden_matches == []


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
    assert "Hypercap CC NLP Reyan Figures.qmd" not in readme_text


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
    assert "validate_candidate_sidecars(" in classifier_text
    assert '"cc_missing_flag"' in classifier_text
    assert '"cc_pseudomissing_flag"' in classifier_text


def test_rater_notebook_contains_key_inventory_and_canonical_mapping() -> None:
    rater_text = (WORK_DIR / "Rater Agreement Analysis.qmd").read_text()
    assert "R3_vs_NLP_key_inventory.csv" in rater_text
    assert "R3_vs_NLP_label_mapping_audit.csv" in rater_text
    assert "join_key_diagnostics.csv" in rater_text
    assert "join_audit_summary.csv" in rater_text
    assert "unmatched_adjudicated_rows.csv" in rater_text
    assert "unmatched_nlp_rows_sample.csv" in rater_text
    assert "target_sample_n" in rater_text
    assert "warn_below_target_fail_on_zero" in rater_text
    assert "build_join_key_diagnostics(" in rater_text
    assert "canonicalize_rvc_code" in rater_text
    assert "Human inter-rater agreement was evaluated in the full adjudicated sample" in rater_text
    assert "NLP-vs-adjudicator benchmarking was evaluated in the matched subset" in rater_text
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
