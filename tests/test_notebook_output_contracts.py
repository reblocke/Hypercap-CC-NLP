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
    assert "other_route_quarantine_audit.csv" in cohort_text
    assert "first_gas_anchor_audit.csv" in cohort_text
    assert "first_other_src_detail" in cohort_text
    assert "first_gas_anchor_has_pco2" in cohort_text


def test_analysis_notebook_contains_requested_outputs() -> None:
    analysis_text = (WORK_DIR / "Hypercap CC NLP Analysis.qmd").read_text()
    assert '"other_hypercap_threshold"' in analysis_text
    assert "ICD_vs_Gas_Performance.xlsx" in analysis_text
    assert "ICD_Positive_Subset_Breakdown.xlsx" in analysis_text
    assert "Ascertainment_Overlap_UpSet.png" in analysis_text
    assert "from upsetplot import UpSet, from_indicators" in analysis_text
    assert "def select_preferred_vital_column(" in analysis_text


def test_classifier_notebook_contains_spell_mode_comparison_and_audit() -> None:
    classifier_text = (WORK_DIR / "Hypercap CC NLP Classifier.qmd").read_text()
    assert "CC_SPELL_CORRECTION_MODE" in classifier_text
    assert "SPELL_CORRECTION_MODES" in classifier_text
    assert "choose_spell_mode(" in classifier_text
    assert "classifier_spell_mode_comparison.csv" in classifier_text
    assert "classifier_spellfix_log.csv" in classifier_text
    assert "integrity_violation_total" in classifier_text


def test_cohort_notebook_requires_manifest_hco3_and_poc_fallback_guard() -> None:
    cohort_text = (WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd").read_text()
    assert "lab.get(\"hco3_itemids\"" in cohort_text
    assert "icu.get(\"hco3_itemids\"" in cohort_text
    assert "CO2_other" not in cohort_text or "LAB-only OTHER quarantine policy" in cohort_text
    assert "first_hco3_source" in cohort_text
    assert "poc_explicit_itemid_fallback" in cohort_text
