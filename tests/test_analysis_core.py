from __future__ import annotations

import ast
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


WORK_DIR = Path(__file__).resolve().parents[1]
ANALYSIS_NOTEBOOK = WORK_DIR / "Hypercap CC NLP Analysis.qmd"
HELPER_FUNCTIONS = [
    "ensure_required_columns",
    "to_binary_flag",
    "_binary_or_zero",
    "classify_icd_category_vectorized",
    "classify_inclusion_type_vectorized",
    "binary_crosstab_yes_no",
    "symptom_distribution_by_overlap",
    "classify_gas_source_overlap",
    "get_rfv_name_cols",
    "resolve_encounter_id",
    "canonicalize_rfv_label",
    "canonicalize_rfv_set",
    "apply_uncodable_policy",
    "group_canonical_rfv_label",
    "normalize_rfv_group",
    "_slugify_category",
    "_coerce_label_set",
    "_membership_long_from_sets",
    "_indicator_frame_from_long",
    "build_rfv_label_artifacts",
    "build_rfv_membership_long",
    "make_category_indicators",
    "summarize_multilabel_prevalence",
    "_require_nonmissing_cluster_unit",
    "_stratum_mask",
    "summarize_multilabel_prevalence_with_clustered_ci",
    "ordered_categories",
    "ordered_rfv_display_categories",
    "format_figure_n",
    "format_figure_pct",
    "maybe_series",
    "maybe_numeric",
    "maybe_datetime",
    "_first_available_datetime",
    "_numeric_sort_key",
    "first_eligible_admission_mask",
    "select_first_eligible_admission_per_patient",
    "build_timing_safeguard_cohort_membership",
    "summarize_timing_safeguard_denominators",
    "summarize_timing_safeguard_rfv_prevalence",
    "compare_timing_safeguard_prevalence",
    "build_timing_safeguard_top_category_flags",
    "build_timing_safeguard_interpretation_flags",
    "coerce_nullable_boolean",
    "_nullable_boolean_mismatch",
    "_coerce_contract_datetime_for_analysis",
    "_coerce_contract_numeric_for_analysis",
    "_select_first_observed_imv_for_analysis",
    "_legacy_imv_timing_discordant_for_analysis",
    "validate_imv_timing_analysis_contract",
    "prepare_imv_timing_gas_positive_frame",
    "_binary_count_denominator_pct",
    "build_imv_timing_group_yield",
    "build_imv_timing_group_characteristics",
    "summarize_imv_timing_rfv_prevalence",
    "paired_cluster_bootstrap_imv_no_prior_sensitivity",
    "build_imv_timing_source_audit",
    "build_imv_timing_definitions",
    "assert_imv_timing_export_privacy",
    "format_imv_timing_prevalence_for_export",
    "select_max_imv_timing_grouped_contrast",
    "imv_timing_interpretation_sentence",
    "build_imv_timing_manuscript_summary",
    "classify_timing_group",
    "select_analysis_ph_with_source",
    "select_analysis_ph",
    "select_paired_qualifying_ph_for_figure4",
    "select_figure4_analysis_ph",
    "derive_ph_severity",
    "first_hco3_qc_pass_mask",
    "assign_frozen_ph_band",
    "assign_frozen_hco3_band",
    "derive_acid_base_state",
    "build_acid_base_source_missingness_table",
    "build_candidate_definition_membership",
    "summarize_candidate_definition_yield",
    "_administrative_only_rfv_mask",
    "build_administrative_exclusion_sensitivity",
    "build_gas_source_sensitivity_summary",
    "build_icd_era_sensitivity_summary",
    "_any_numeric_candidate_ge",
    "source_specific_pco2_threshold_mask",
    "build_pco2_threshold_sensitivity_summary",
    "build_analytic_cohort_threshold_sensitivity_summary",
    "build_sensitivity_denominator_audit",
    "_sensitivity_denominator_definition",
    "build_acidemia_ph_source_audit",
    "build_acidemia_severity_denominator_audit",
    "build_acidemia_timing_denominator_audit",
    "summarize_labels_per_encounter",
    "_ordered_categories_for_comorbidity_summary",
    "summarize_rfv_prevalence_by_comorbidity",
    "format_median_iqr",
]
HELPER_CONSTANTS = {
    "RFV_UNCODABLE_LABEL",
    "RFV_CANONICAL_LABELS",
    "RFV_GROUP_ORDER",
    "RFV_LABEL_ALIASES",
    "CANONICAL_TO_GROUP",
    "COMORBIDITY_FLAGS",
    "TIMING_SAFEGUARD_COHORTS",
    "TIMING_SAFEGUARD_SIMILARITY_THRESHOLD_PP",
    "IMV_TIMING_STRATA",
    "IMV_TIMING_STRATUM_KEYS",
    "IMV_TIMING_STRATUM_LABELS",
    "IMV_TIMING_STRATUM_LABEL_MAP",
    "IMV_TIMING_SOURCE_VALUES",
    "IMV_TIMING_REQUIRED_INPUT_COLS",
    "IMV_TIMING_TIMESTAMP_COLS",
    "IMV_TIMING_FORBIDDEN_EXPORT_COLUMNS",
    "FIGURE4_USE_PAIRED_QUALIFYING_PH",
    "PREVALENCE_CI_BOOTSTRAP_REPLICATES",
    "PREVALENCE_CI_BOOTSTRAP_SEED",
    "PREVALENCE_CI_CLUSTER_UNIT",
    "PREVALENCE_CI_LEVEL",
    "FROZEN_PH_BAND_ORDER",
    "FROZEN_HCO3_BAND_ORDER",
    "ACID_BASE_STATE_ORDER",
}


def _extract_python_chunks(qmd_path: Path) -> str:
    text = qmd_path.read_text()
    chunks = re.findall(r"```\{python\}\n(.*?)```", text, flags=re.S)
    return "\n\n".join(chunks)


def _load_analysis_helpers() -> dict[str, object]:
    module_source = _extract_python_chunks(ANALYSIS_NOTEBOOK)
    tree = ast.parse(module_source)
    selected_nodes = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            target_names = {
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            if target_names & HELPER_CONSTANTS:
                selected_nodes.append(node)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id in HELPER_CONSTANTS:
                selected_nodes.append(node)
        if isinstance(node, ast.FunctionDef) and node.name in HELPER_FUNCTIONS:
            selected_nodes.append(node)
    helper_module = ast.Module(body=selected_nodes, type_ignores=[])
    namespace = {"np": np, "pd": pd, "Path": Path, "re": re}
    exec(compile(helper_module, str(ANALYSIS_NOTEBOOK), "exec"), namespace)
    return namespace


HELPERS = _load_analysis_helpers()

resolve_required_columns = HELPERS["ensure_required_columns"]
classify_icd_category_vectorized = HELPERS["classify_icd_category_vectorized"]
classify_inclusion_type_vectorized = HELPERS["classify_inclusion_type_vectorized"]
binary_crosstab_yes_no = HELPERS["binary_crosstab_yes_no"]
symptom_distribution_by_overlap = HELPERS["symptom_distribution_by_overlap"]
classify_gas_source_overlap = HELPERS["classify_gas_source_overlap"]
get_rfv_name_cols = HELPERS["get_rfv_name_cols"]
resolve_encounter_id = HELPERS["resolve_encounter_id"]
canonicalize_rfv_label = HELPERS["canonicalize_rfv_label"]
canonicalize_rfv_set = HELPERS["canonicalize_rfv_set"]
apply_uncodable_policy = HELPERS["apply_uncodable_policy"]
group_canonical_rfv_label = HELPERS["group_canonical_rfv_label"]
normalize_rfv_group = HELPERS["normalize_rfv_group"]
build_rfv_label_artifacts = HELPERS["build_rfv_label_artifacts"]
build_rfv_membership_long = HELPERS["build_rfv_membership_long"]
make_category_indicators = HELPERS["make_category_indicators"]
summarize_multilabel_prevalence = HELPERS["summarize_multilabel_prevalence"]
summarize_multilabel_prevalence_with_clustered_ci = HELPERS[
    "summarize_multilabel_prevalence_with_clustered_ci"
]
ordered_rfv_display_categories = HELPERS["ordered_rfv_display_categories"]
format_figure_n = HELPERS["format_figure_n"]
format_figure_pct = HELPERS["format_figure_pct"]
maybe_series = HELPERS["maybe_series"]
maybe_numeric = HELPERS["maybe_numeric"]
first_eligible_admission_mask = HELPERS["first_eligible_admission_mask"]
select_first_eligible_admission_per_patient = HELPERS["select_first_eligible_admission_per_patient"]
build_timing_safeguard_cohort_membership = HELPERS[
    "build_timing_safeguard_cohort_membership"
]
summarize_timing_safeguard_denominators = HELPERS["summarize_timing_safeguard_denominators"]
summarize_timing_safeguard_rfv_prevalence = HELPERS[
    "summarize_timing_safeguard_rfv_prevalence"
]
compare_timing_safeguard_prevalence = HELPERS["compare_timing_safeguard_prevalence"]
build_timing_safeguard_top_category_flags = HELPERS[
    "build_timing_safeguard_top_category_flags"
]
build_timing_safeguard_interpretation_flags = HELPERS[
    "build_timing_safeguard_interpretation_flags"
]
validate_imv_timing_analysis_contract = HELPERS[
    "validate_imv_timing_analysis_contract"
]
prepare_imv_timing_gas_positive_frame = HELPERS[
    "prepare_imv_timing_gas_positive_frame"
]
build_imv_timing_group_yield = HELPERS["build_imv_timing_group_yield"]
build_imv_timing_group_characteristics = HELPERS[
    "build_imv_timing_group_characteristics"
]
summarize_imv_timing_rfv_prevalence = HELPERS[
    "summarize_imv_timing_rfv_prevalence"
]
paired_cluster_bootstrap_imv_no_prior_sensitivity = HELPERS[
    "paired_cluster_bootstrap_imv_no_prior_sensitivity"
]
assert_imv_timing_export_privacy = HELPERS["assert_imv_timing_export_privacy"]
build_imv_timing_manuscript_summary = HELPERS[
    "build_imv_timing_manuscript_summary"
]
classify_timing_group = HELPERS["classify_timing_group"]
select_analysis_ph_with_source = HELPERS["select_analysis_ph_with_source"]
select_analysis_ph = HELPERS["select_analysis_ph"]
select_paired_qualifying_ph_for_figure4 = HELPERS["select_paired_qualifying_ph_for_figure4"]
select_figure4_analysis_ph = HELPERS["select_figure4_analysis_ph"]
derive_ph_severity = HELPERS["derive_ph_severity"]
assign_frozen_ph_band = HELPERS["assign_frozen_ph_band"]
assign_frozen_hco3_band = HELPERS["assign_frozen_hco3_band"]
derive_acid_base_state = HELPERS["derive_acid_base_state"]
build_acid_base_source_missingness_table = HELPERS["build_acid_base_source_missingness_table"]
build_candidate_definition_membership = HELPERS["build_candidate_definition_membership"]
summarize_candidate_definition_yield = HELPERS["summarize_candidate_definition_yield"]
_administrative_only_rfv_mask = HELPERS["_administrative_only_rfv_mask"]
build_administrative_exclusion_sensitivity = HELPERS["build_administrative_exclusion_sensitivity"]
build_gas_source_sensitivity_summary = HELPERS["build_gas_source_sensitivity_summary"]
build_icd_era_sensitivity_summary = HELPERS["build_icd_era_sensitivity_summary"]
source_specific_pco2_threshold_mask = HELPERS["source_specific_pco2_threshold_mask"]
build_pco2_threshold_sensitivity_summary = HELPERS["build_pco2_threshold_sensitivity_summary"]
build_analytic_cohort_threshold_sensitivity_summary = HELPERS[
    "build_analytic_cohort_threshold_sensitivity_summary"
]
build_sensitivity_denominator_audit = HELPERS["build_sensitivity_denominator_audit"]
build_acidemia_ph_source_audit = HELPERS["build_acidemia_ph_source_audit"]
build_acidemia_severity_denominator_audit = HELPERS["build_acidemia_severity_denominator_audit"]
build_acidemia_timing_denominator_audit = HELPERS["build_acidemia_timing_denominator_audit"]
summarize_labels_per_encounter = HELPERS["summarize_labels_per_encounter"]
summarize_rfv_prevalence_by_comorbidity = HELPERS["summarize_rfv_prevalence_by_comorbidity"]


def test_ensure_required_columns_raises_informative_key_error() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})
    with pytest.raises(KeyError, match="Missing required columns"):
        resolve_required_columns(df, ["a", "c"])


def test_classify_icd_category_vectorized_precedence() -> None:
    df = pd.DataFrame(
        {
            "ICD10_J9602": [1, 0, 0, 0, 0, 0, 0],
            "ICD10_J9612": [1, 1, 0, 0, 0, 0, 0],
            "ICD10_J9622": [0, 0, 1, 0, 0, 0, 0],
            "ICD10_J9692": [0, 0, 0, 1, 0, 0, 0],
            "ICD10_E662": [0, 0, 0, 0, 1, 0, 0],
            "ICD9_27803": [0, 0, 0, 0, 0, 1, 0],
        }
    )

    out = classify_icd_category_vectorized(df)
    assert out.tolist() == [
        "Acute respiratory failure with hypercapnia",
        "Chronic respiratory failure with hypercapnia",
        "Acute and chronic respiratory failure with hypercapnia",
        "Respiratory failure, unspecified with hypercapnia",
        "Obesity hypoventilation syndrome",
        "Obesity hypoventilation syndrome",
        "Other / None",
    ]


def test_classify_inclusion_type_vectorized_mapping() -> None:
    any_icd = pd.Series([1, 1, 0, 0])
    gas_any = pd.Series([1, 0, 1, 0])
    out = classify_inclusion_type_vectorized(any_icd, gas_any)
    assert out.tolist() == ["Both", "ICD_only", "Gas_only", "Neither"]


def test_binary_crosstab_yes_no_has_stable_columns() -> None:
    df = pd.DataFrame(
        {
            "RFV1_name": ["Resp", "Resp", "Cardiac"],
            "flag": [1, 1, 1],
        }
    )
    out = binary_crosstab_yes_no(df, "RFV1_name", "flag")

    assert list(out.columns) == ["No", "Yes", "Percent_yes"]
    assert out["No"].sum() == 0
    assert out.loc["Resp", "Yes"] == 2
    assert out.loc["Cardiac", "Percent_yes"] == 100.0


def test_symptom_distribution_by_overlap_percent_sums() -> None:
    df = pd.DataFrame(
        {
            "overlap": ["ABG-only", "ABG-only", "ABG-only", "VBG-only", "VBG-only"],
            "RFV1_name": ["Resp", "Resp", "Neuro", "Cardiac", "Resp"],
        }
    )

    counts, pivot = symptom_distribution_by_overlap(
        df,
        group_col="overlap",
        symptom_col="RFV1_name",
        top_k=1,
    )

    summed = counts.groupby("overlap")["Percent"].sum().round(1)
    assert summed.to_dict() == {"ABG-only": 100.0, "VBG-only": 100.0}
    assert set(pivot.columns.tolist()) == {"ABG-only", "VBG-only"}


def test_classify_gas_source_overlap_includes_other_strata() -> None:
    labels = classify_gas_source_overlap(
        pd.Series([1, 1, 0, 0, 0]),
        pd.Series([0, 1, 1, 0, 0]),
        pd.Series([0, 1, 1, 1, 0]),
    )
    assert labels.tolist() == [
        "ABG-only",
        "ABG+VBG+UNKNOWN",
        "VBG+UNKNOWN",
        "UNKNOWN-only",
        "No-gas",
    ]


def test_get_rfv_name_cols_respects_slot_order_and_missing_columns() -> None:
    df = pd.DataFrame(columns=["RFV3_name", "RFV1_name", "RFV5_name", "not_rfv"])
    assert get_rfv_name_cols(df) == ["RFV1_name", "RFV3_name", "RFV5_name"]


def test_resolve_encounter_id_prefers_unique_ed_stay_id_then_hadm_id() -> None:
    ed_df = pd.DataFrame({"ed_stay_id": [10, 11], "hadm_id": [1, 1]})
    hadm_df = pd.DataFrame({"hadm_id": [20, 21]})
    fallback_df = pd.DataFrame({"hadm_id": [30, 30]})

    ed_id = resolve_encounter_id(ed_df)
    hadm_id = resolve_encounter_id(hadm_df)
    fallback_id = resolve_encounter_id(fallback_df)

    assert ed_id.name == "ed_stay_id"
    assert ed_id.tolist() == [10, 11]
    assert hadm_id.name == "hadm_id"
    assert hadm_id.tolist() == [20, 21]
    assert fallback_id.name == "encounter_row_id"
    assert fallback_id.tolist() == [0, 1]


def test_normalize_rfv_group_is_deterministic_and_maps_unknown_to_others() -> None:
    assert normalize_rfv_group("Symptom – Respiratory") == "Respiratory"
    assert normalize_rfv_group("Injuries & adverse effects") == "Injuries & adverse effects"
    assert normalize_rfv_group("Uncodable/Unknown") == "Other grouped RFV categories"
    assert normalize_rfv_group("") == "Other grouped RFV categories"


def test_canonicalize_rfv_label_handles_aliases_and_unknown_labels() -> None:
    assert canonicalize_rfv_label("Diseases (patient-stated diagnosis)") == "Diseases (patient-stated)"
    assert canonicalize_rfv_label("Uncodable / Unknown") == "Uncodable/Unknown"
    with pytest.raises(ValueError, match="Unmapped canonical RFV label"):
        canonicalize_rfv_label("Totally Unknown Label")


def test_canonicalize_rfv_set_and_uncodable_policy_drop_mixed_uncodable() -> None:
    label_set = canonicalize_rfv_set(
        ["Uncodable/Unknown", "Symptom – Respiratory", "Symptom – Respiratory", ""]
    )
    assert label_set == frozenset({"Symptom – Respiratory", "Uncodable/Unknown"})
    assert apply_uncodable_policy(label_set) == frozenset({"Symptom – Respiratory"})


def test_build_rfv_membership_long_deduplicates_repeated_labels_within_encounter() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "RFV1_name": ["Symptom – Respiratory", "Uncodable/Unknown", "Uncodable/Unknown"],
            "RFV2_name": ["Symptom – Respiratory", "Symptom – Nervous", ""],
            "RFV3_name": ["Symptom – Nervous", "", ""],
        }
    )

    out = build_rfv_membership_long(df)
    grouped = build_rfv_membership_long(df, grouped=True)

    assert out.sort_values(["encounter_id", "category"]).reset_index(drop=True).to_dict("records") == [
        {"encounter_id": 1, "category": "Symptom – Nervous"},
        {"encounter_id": 1, "category": "Symptom – Respiratory"},
        {"encounter_id": 2, "category": "Symptom – Nervous"},
        {"encounter_id": 3, "category": "Uncodable/Unknown"},
    ]
    assert grouped.sort_values(["encounter_id", "category"]).reset_index(drop=True).to_dict("records") == [
        {"encounter_id": 1, "category": "Nervous"},
        {"encounter_id": 1, "category": "Respiratory"},
        {"encounter_id": 2, "category": "Nervous"},
    ]


def test_build_rfv_label_artifacts_surface_uncodable_diagnostics() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "RFV1_name": ["Symptom – Respiratory", "Uncodable/Unknown", "Uncodable/Unknown"],
            "RFV2_name": ["Symptom – Nervous", "Symptom – Nervous", ""],
        }
    )

    artifacts = build_rfv_label_artifacts(df)
    encounter_level = artifacts["encounter_level"]

    assert encounter_level.loc[0, "analytical_label_set"] == frozenset(
        {"Symptom – Respiratory", "Symptom – Nervous"}
    )
    assert encounter_level.loc[1, "analytical_label_set"] == frozenset({"Symptom – Nervous"})
    assert encounter_level.loc[1, "has_uncodable_any"] == 1
    assert encounter_level.loc[1, "has_uncodable_only"] == 0
    assert encounter_level.loc[2, "analytical_label_set"] == frozenset({"Uncodable/Unknown"})
    assert encounter_level.loc[2, "has_uncodable_only"] == 1
    assert encounter_level.loc[2, "semantic_group_set"] == frozenset()
    assert artifacts["label_coverage_audit"]["covered_by_mapping"].all()


def test_make_category_indicators_exclude_uncodable_from_grouped_semantics() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "RFV1_name": ["Symptom – Respiratory", "Uncodable/Unknown"],
            "RFV2_name": ["Symptom – Nervous", ""],
        }
    )

    detailed = make_category_indicators(df)
    grouped = make_category_indicators(df, grouped=True)

    assert detailed.loc[0, "Symptom – Respiratory"] == 1
    assert detailed.loc[0, "Symptom – Nervous"] == 1
    assert detailed.loc[1, "Uncodable/Unknown"] == 1
    assert grouped.loc[0, "Respiratory"] == 1
    assert "Others" not in grouped.columns


def test_summarize_multilabel_prevalence_is_ge_primary_label_prevalence() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "route": ["ABG", "ABG", "VBG"],
            "RFV1_name": ["Symptom – Respiratory", "Symptom – Digestive", "Symptom – Respiratory"],
            "RFV2_name": ["Symptom – Nervous", "Symptom – Respiratory", ""],
        }
    )

    multi = summarize_multilabel_prevalence(df, strata="route")
    primary = (
        df.assign(category=df["RFV1_name"])
        .groupby(["route", "category"], dropna=False)
        .size()
        .reset_index(name="n_with_category")
    )
    group_sizes = df.groupby("route").size().reset_index(name="N_group")
    primary = primary.merge(group_sizes, on="route", how="left")
    primary["pct_of_encounters"] = primary["n_with_category"] / primary["N_group"] * 100

    merged = multi.merge(
        primary[["route", "category", "pct_of_encounters"]],
        on=["route", "category"],
        how="left",
        suffixes=("_multi", "_primary"),
    )
    merged["pct_of_encounters_primary"] = merged["pct_of_encounters_primary"].fillna(0)
    assert (merged["pct_of_encounters_multi"] >= merged["pct_of_encounters_primary"]).all()


def test_clustered_bootstrap_ci_is_deterministic_and_matches_point_estimates() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [10, 10, 20, 30, 40],
            "hadm_id": [1, 2, 3, 4, 5],
            "route": ["ABG", "ABG", "ABG", "VBG", "VBG"],
            "RFV1_name": [
                "Symptom – Respiratory",
                "Symptom – Digestive",
                "Symptom – Respiratory",
                "Injuries & adverse effects",
                "Symptom – Respiratory",
            ],
            "RFV2_name": ["Symptom – Nervous", "", "", "", ""],
        }
    )

    with_ci = summarize_multilabel_prevalence_with_clustered_ci(
        df,
        strata="route",
        grouped=True,
        n_bootstrap=200,
        seed=20260607,
    )
    repeated = summarize_multilabel_prevalence_with_clustered_ci(
        df,
        strata="route",
        grouped=True,
        n_bootstrap=200,
        seed=20260607,
    )
    point = summarize_multilabel_prevalence(df, strata="route", grouped=True)

    compared = with_ci.merge(
        point,
        on=["route", "category", "N_group", "n_with_category"],
        suffixes=("_ci", "_point"),
        validate="one_to_one",
    )
    assert np.allclose(compared["pct_of_encounters_ci"], compared["pct_of_encounters_point"])
    assert with_ci["ci_lower"].between(0, 100).all()
    assert with_ci["ci_upper"].between(0, 100).all()
    assert (with_ci["ci_lower"] <= with_ci["pct_of_encounters"]).all()
    assert (with_ci["ci_upper"] >= with_ci["pct_of_encounters"]).all()
    cluster_counts = (
        with_ci.drop_duplicates(subset=["route"])
        .set_index("route")["n_clusters"]
        .to_dict()
    )
    assert cluster_counts == {"ABG": 2, "VBG": 2}
    assert set(cluster_counts.values()) != {df["subject_id"].nunique()}
    pd.testing.assert_frame_equal(
        with_ci.sort_index(axis=1),
        repeated.sort_index(axis=1),
        check_dtype=False,
    )


def test_clustered_bootstrap_ci_samples_repeated_admissions_by_patient_cluster() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [1, 1, 2, 3],
            "hadm_id": [11, 12, 21, 31],
            "route": ["All", "All", "All", "All"],
            "RFV1_name": [
                "Symptom – Respiratory",
                "Symptom – Respiratory",
                "Symptom – Digestive",
                "Symptom – Digestive",
            ],
        }
    )

    with_ci = summarize_multilabel_prevalence_with_clustered_ci(
        df,
        strata="route",
        grouped=True,
        n_bootstrap=100,
        seed=7,
    )
    respiratory = with_ci.loc[with_ci["category"].eq("Respiratory")].iloc[0]

    assert respiratory["N_group"] == 4
    assert respiratory["n_with_category"] == 2
    assert respiratory["pct_of_encounters"] == 50
    assert respiratory["n_clusters"] == 3
    assert respiratory["cluster_unit"] == "patient"


def test_clustered_bootstrap_ci_fails_closed_without_patient_cluster() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [1, None],
            "hadm_id": [11, 12],
            "route": ["All", "All"],
            "RFV1_name": ["Symptom – Respiratory", "Symptom – Digestive"],
        }
    )

    with pytest.raises(ValueError, match="missing subject_id"):
        summarize_multilabel_prevalence_with_clustered_ci(
            df,
            strata="route",
            grouped=True,
            n_bootstrap=10,
        )


def test_figure_formatters_and_rfv_order_are_manuscript_safe() -> None:
    assert format_figure_n(12345) == "12345"
    assert format_figure_n(np.nan) == "0"
    assert format_figure_pct(12.36) == "12.4%"
    assert format_figure_pct(pd.NA) == "0.0%"

    unordered = [
        "Nervous",
        "Other grouped RFV categories",
        "Respiratory",
        "Digestive",
        "Injuries & adverse effects",
    ]
    assert ordered_rfv_display_categories(unordered) == [
        "Respiratory",
        "Digestive",
        "Nervous",
        "Injuries & adverse effects",
        "Other grouped RFV categories",
    ]


def test_select_first_eligible_admission_per_patient_uses_deterministic_ties() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [10, 10, 20, 20, 30],
            "hadm_id": [102, 101, 202, 201, 301],
            "ed_stay_id": [1002, 1001, 2002, 2001, 3001],
            "ed_anchor_time": [
                "2020-01-02 00:00",
                "2020-01-01 00:00",
                "2020-02-01 00:00",
                "2020-02-01 00:00",
                pd.NaT,
            ],
            "admittime": [
                "2020-01-02 01:00",
                "2020-01-01 01:00",
                "2020-02-01 02:00",
                "2020-02-01 01:00",
                pd.NaT,
            ],
        }
    )

    mask = first_eligible_admission_mask(df)
    selected = select_first_eligible_admission_per_patient(df)

    assert mask.tolist() == [False, True, False, True, True]
    assert selected["hadm_id"].tolist() == [101, 201, 301]
    assert selected["subject_id"].is_unique


def test_select_first_eligible_admission_per_patient_requires_subject_id() -> None:
    df = pd.DataFrame({"hadm_id": [1, 2]})
    with pytest.raises(KeyError, match="subject_id"):
        select_first_eligible_admission_per_patient(df)

    df_missing_subject = pd.DataFrame({"subject_id": [1, pd.NA], "hadm_id": [1, 2]})
    with pytest.raises(ValueError, match="missing subject_id"):
        select_first_eligible_admission_per_patient(df_missing_subject)


def test_timing_safeguard_cohort_membership_uses_planned_windows() -> None:
    df = pd.DataFrame(
        {
            "any_hypercap_icd": [1, 0, 0, 0, 1, 1, 0],
            "pco2_threshold_any": [1, 1, 1, 1, 0, 1, 0],
            "dt_qualifying_hypercapnia_hours": [1, 6, 24, 25, np.nan, np.nan, np.nan],
        }
    )

    membership = build_timing_safeguard_cohort_membership(df)

    assert membership["broad_ehr_ascertained"].tolist() == [
        True,
        True,
        True,
        True,
        True,
        True,
        False,
    ]
    assert membership["first_gas_within_24h"].tolist() == [
        True,
        True,
        True,
        False,
        False,
        False,
        False,
    ]
    assert membership["first_gas_within_6h"].tolist() == [
        True,
        False,
        False,
        False,
        False,
        False,
        False,
    ]
    assert membership["icd_positive"].tolist() == [
        True,
        False,
        False,
        False,
        True,
        True,
        False,
    ]
    assert membership["icd_plus_24h_gas"].tolist() == [
        True,
        False,
        False,
        False,
        False,
        False,
        False,
    ]


def test_timing_safeguard_summary_is_aggregate_and_flags_drift() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3, 4],
            "any_hypercap_icd": [1, 0, 0, 1],
            "pco2_threshold_any": [1, 1, 1, 0],
            "dt_qualifying_hypercapnia_hours": [1, 24, 30, np.nan],
            "RFV1_name": [
                "Symptom – Respiratory",
                "Symptom – Digestive",
                "Symptom – Respiratory",
                "Symptom – Nervous",
            ],
        }
    )

    denominators = summarize_timing_safeguard_denominators(df)
    prevalence = summarize_timing_safeguard_rfv_prevalence(df, grouped=True)
    comparisons = compare_timing_safeguard_prevalence(
        prevalence,
        denominators,
        representation="Grouped RFV",
    )
    top_flags = build_timing_safeguard_top_category_flags(comparisons)
    interpretation = build_timing_safeguard_interpretation_flags(comparisons, top_flags)

    assert denominators.set_index("cohort_key")["N_admissions"].to_dict() == {
        "broad_ehr_ascertained": 4,
        "first_gas_within_24h": 2,
        "first_gas_within_6h": 1,
        "icd_positive": 2,
        "icd_plus_24h_gas": 1,
    }
    forbidden_columns = {"subject_id", "hadm_id", "ed_stay_id", "ed_triage_cc"}
    for aggregate_frame in (denominators, prevalence, comparisons, top_flags, interpretation):
        assert forbidden_columns.isdisjoint(aggregate_frame.columns)
    assert comparisons["over_2pp_threshold"].any()
    assert set(interpretation["similar_by_rule"]) == {False}


def test_timing_safeguard_comparison_preserves_denominators_for_absent_categories() -> None:
    df = pd.DataFrame(
        {
            "any_hypercap_icd": [1, 0, 1],
            "pco2_threshold_any": [1, 1, 0],
            "dt_qualifying_hypercapnia_hours": [1, 24, np.nan],
            "RFV1_name": [
                "Symptom – Respiratory",
                "Symptom – Digestive",
                "Symptom – Nervous",
            ],
        }
    )

    denominators = summarize_timing_safeguard_denominators(df)
    prevalence = summarize_timing_safeguard_rfv_prevalence(df, grouped=True)
    comparisons = compare_timing_safeguard_prevalence(
        prevalence,
        denominators,
        representation="Grouped RFV",
    )

    broad_only = comparisons.loc[
        comparisons["comparison_cohort_key"].eq("first_gas_within_24h")
        & comparisons["category"].eq("Nervous")
    ].squeeze()
    assert broad_only["N_reference"] == 3
    assert broad_only["n_reference"] == 1
    assert broad_only["pct_reference"] == pytest.approx(33.33333333333333)
    assert broad_only["N_comparison"] == 2
    assert broad_only["n_comparison"] == 0
    assert broad_only["pct_comparison"] == 0
    assert broad_only["absolute_difference_pp"] == pytest.approx(33.33333333333333)
    assert bool(broad_only["over_2pp_threshold"]) is True


def test_figure4_ph_rule_uses_paired_ph_and_keeps_source_priority_auditable() -> None:
    df = pd.DataFrame(
        {
            "lab_abg_ph": [7.11, np.nan, np.nan, np.nan],
            "lab_vbg_ph": [7.22, 7.23, np.nan, np.nan],
            "first_ph": [7.33, 7.34, 7.31, np.nan],
            "min_ph_0_24h": [7.44, 7.45, 7.29, np.nan],
            "qualifying_ph": [7.10, 7.20, 7.30, np.nan],
        }
    )

    selected = select_analysis_ph_with_source(df)
    assert selected["analysis_ph"].tolist()[:3] == [7.11, 7.23, 7.31]
    assert selected["analysis_ph_source"].tolist() == [
        "lab_abg_ph",
        "lab_vbg_ph",
        "first_ph_or_min_ph_0_24h",
        "missing",
    ]
    assert select_analysis_ph(df).tolist()[:3] == [7.11, 7.23, 7.31]
    assert select_paired_qualifying_ph_for_figure4(df).tolist()[:3] == [7.10, 7.20, 7.30]
    assert select_figure4_analysis_ph(df).tolist()[:3] == [7.10, 7.20, 7.30]

    with pytest.raises(KeyError, match="qualifying_ph is absent"):
        select_paired_qualifying_ph_for_figure4(df.drop(columns=["qualifying_ph"]))

    assert derive_ph_severity(df).tolist() == [
        "Severe (pH <7.25)",
        "Severe (pH <7.25)",
        "Mild (7.30–7.34)",
        None,
    ]


def test_acidemia_denominator_audits_are_aggregate_only() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [10, 10, 11, 12],
            "hadm_id": [1, 2, 3, 4],
            "ed_stay_id": [101, 102, 103, 104],
            "chiefcomplaint": ["dyspnea", "fall", "confusion", "pain"],
            "lab_abg_ph": [7.40, np.nan, np.nan, np.nan],
            "lab_vbg_ph": [np.nan, 7.31, np.nan, np.nan],
            "first_ph": [np.nan, np.nan, 7.22, np.nan],
            "min_ph_0_24h": [7.40, 7.31, 7.20, np.nan],
            "min_ph_0_6h": [7.40, 7.36, np.nan, np.nan],
            "qualifying_ph": [7.39, 7.30, np.nan, np.nan],
            "qualifying_ph_pairing_status": [
                "paired_same_specimen_panel",
                "paired_same_time_panel",
                "qualifying_panel_missing_ph",
                "qualifying_panel_missing_ph",
            ],
        }
    )

    severity_denominators = build_acidemia_severity_denominator_audit(df)
    source_audit = build_acidemia_ph_source_audit(df)
    timing_denominators = build_acidemia_timing_denominator_audit(df)

    severity_counts = severity_denominators.set_index("audit_item")["n_admissions"].to_dict()
    assert severity_counts["blood_gas_subset_denominator"] == 4
    assert severity_counts["included_active_figure4_ph_available"] == 2
    assert severity_counts["excluded_active_figure4_ph_missing"] == 2
    assert severity_counts["included_source_priority_ph_available"] == 3
    assert severity_counts["excluded_source_priority_ph_missing"] == 1
    assert severity_counts["paired_qualifying_ph_available"] == 2
    assert severity_counts["paired_qualifying_ph_missing"] == 2

    pairing_status_counts = source_audit.loc[
        source_audit["audit_group"].eq("qualifying_ph_pairing_status")
    ].set_index("category")["n_admissions"].to_dict()
    assert pairing_status_counts == {
        "paired_same_specimen_panel": 1,
        "paired_same_time_panel": 1,
        "qualifying_panel_missing_ph": 2,
    }
    timing_counts = timing_denominators.set_index("audit_item")["n_admissions"].to_dict()
    assert timing_counts["included_timing_classifiable"] == 2
    assert timing_counts["excluded_no_ph_within_24h"] == 1
    assert timing_counts["excluded_acidemia_within_24h_missing_0_6h_ph"] == 1

    forbidden_columns = {"subject_id", "hadm_id", "ed_stay_id", "chiefcomplaint", "chief_complaint"}
    for aggregate_frame in (severity_denominators, source_audit, timing_denominators):
        assert forbidden_columns.isdisjoint(aggregate_frame.columns)


def test_summarize_rfv_prevalence_by_comorbidity_uses_multilabel_sets() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2, 3],
            "RFV1_name": ["Symptom – Respiratory", "Symptom – Digestive", "Symptom – Respiratory"],
            "RFV2_name": ["Symptom – Nervous", "Symptom – Respiratory", ""],
            "flag_copd": [1, 0, 1],
            "flag_osa_ohs": [0, 1, 0],
            "flag_chf": [1, 0, 0],
            "flag_neuromuscular": [0, 1, 0],
            "flag_opioid_substance": [0, 0, 1],
            "flag_pneumonia": [1, 0, 0],
        }
    )

    grouped, denominators = summarize_rfv_prevalence_by_comorbidity(df, grouped=True)
    copd_respiratory = grouped.loc[
        grouped["flag_column"].eq("flag_copd") & grouped["category"].eq("Respiratory")
    ].iloc[0]
    copd_nervous = grouped.loc[
        grouped["flag_column"].eq("flag_copd") & grouped["category"].eq("Nervous")
    ].iloc[0]

    assert copd_respiratory["pct_flag_encounters_with_category"] == 100.0
    assert copd_nervous["pct_flag_encounters_with_category"] == 50.0
    assert grouped["non_mutually_exclusive"].all()
    assert set(denominators["flag_column"]) == {
        "flag_copd",
        "flag_osa_ohs",
        "flag_chf",
        "flag_neuromuscular",
        "flag_opioid_substance",
        "flag_pneumonia",
    }


def test_summarize_labels_per_encounter_caps_at_five_and_supports_strata() -> None:
    df = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            "route": ["ABG", "VBG"],
            "RFV1_name": ["Symptom – Respiratory", "Symptom – General"],
            "RFV2_name": ["Symptom – Digestive", "Symptom – Psychological"],
            "RFV3_name": ["Symptom – Nervous", "Treatment/Medication"],
            "RFV4_name": ["Symptom – Circulatory", "Administrative"],
            "RFV5_name": ["Injuries & adverse effects", "Diseases (patient-stated)"],
        }
    )

    summary = summarize_labels_per_encounter(df, strata="route")
    assert summary["label_count"].max() <= 5
    assert set(summary["route"]) == {"ABG", "VBG"}
    assert summary["n_encounters"].sum() == 2


def test_frozen_acid_base_bands_and_states_are_deterministic() -> None:
    ph_values = pd.Series([7.19, 7.20, 7.249, 7.25, 7.299, 7.30, 7.349, 7.35, 7.449, 7.45, np.nan])
    hco3_values = pd.Series([21.9, 22.0, 27.9, 28.0, 33.9, 34.0, np.nan])

    assert assign_frozen_ph_band(ph_values).astype("string").fillna("<NA>").tolist() == [
        "<7.20",
        "7.20-7.24",
        "7.20-7.24",
        "7.25-7.29",
        "7.25-7.29",
        "7.30-7.34",
        "7.30-7.34",
        "7.35-7.44",
        "7.35-7.44",
        ">=7.45",
        "<NA>",
    ]
    assert assign_frozen_hco3_band(hco3_values).astype("string").fillna("<NA>").tolist() == [
        "<22",
        "22-27",
        "22-27",
        "28-33",
        "28-33",
        ">=34",
        "<NA>",
    ]

    states = derive_acid_base_state(
        pd.Series([7.20, 7.30, 7.25, 7.40, 7.46, 7.40, np.nan]),
        pd.Series([21, 24, 30, 30, 35, 24, 30]),
    )
    assert states.astype("string").tolist() == [
        "acidemic with low HCO3",
        "acidemic with reference HCO3",
        "acidemic with elevated HCO3",
        "non-acidemic with elevated HCO3",
        "alkalemic with elevated HCO3",
        "non-acidemic without elevated HCO3",
        "indeterminate",
    ]


def test_candidate_definition_and_sensitivity_summaries_are_aggregate_and_source_aware() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [1, 2, 3, 4],
            "any_hypercap_icd": [1, 0, 0, 1],
            "pco2_threshold_any": [0, 1, 1, 1],
            "dt_qualifying_hypercapnia_hours": [np.nan, 5.0, 30.0, 20.0],
            "qualifying_ph": [np.nan, 7.20, 7.40, 7.40],
            "first_hco3": [np.nan, 30.0, 24.0, 35.0],
            "first_hco3_qc_flag": [0, 1, 1, 1],
            "time_to_imv_hrs": [2.0, np.nan, np.nan, np.nan],
            "time_to_niv_hrs": [np.nan, np.nan, 10.0, np.nan],
            "qualifying_pco2_mmhg": [np.nan, 55.0, 52.0, 61.0],
            "qualifying_site_group": ["UNKNOWN", "VBG", "UNKNOWN", "ABG"],
            "abg_hypercap_threshold": [0, 0, 0, 1],
            "vbg_hypercap_threshold": [0, 1, 0, 0],
            "unknown_hypercap_threshold": [0, 0, 1, 0],
            "has_group_respiratory": [0, 1, 0, 1],
            "has_group_nervous": [0, 0, 1, 0],
            "has_group_injuries_adverse_effects": [0, 0, 0, 0],
            "has_rfv_administrative": [1, 0, 0, 0],
            "has_rfv_respiratory": [0, 1, 0, 1],
            "has_rfv_nervous": [0, 0, 1, 0],
            "imv_flag": [0, 0, 0, 0],
            "niv_flag": [0, 0, 1, 0],
            "death_in_hosp": [0, 0, 0, 1],
            "hosp_los_days": [2.0, 3.0, 4.0, 5.0],
            "admittime": pd.to_datetime(["2014-01-01", "2016-01-01", "2016-02-01", "2013-01-01"]),
        }
    )

    membership = build_candidate_definition_membership(df)
    assert membership["Broad EHR hypercapnia cohort"].tolist() == [True, True, True, True]
    assert membership["Any qualifying gas"].tolist() == [False, True, True, True]
    assert membership["Gas-only"].tolist() == [False, True, True, False]
    assert membership["ICD-only"].tolist() == [True, False, False, False]
    assert membership["Both ICD + gas"].tolist() == [False, False, False, True]
    assert membership["Early gas <=6h"].tolist() == [False, True, False, False]
    assert membership["ICD + early gas"].tolist() == [False, False, False, True]
    assert membership["Exclude post-ventilation-only gas"].tolist() == [True, True, False, True]

    yield_summary = summarize_candidate_definition_yield(df, membership)
    retained = yield_summary.set_index("candidate_definition")["N_retained"].to_dict()
    assert retained["Early gas <=24h"] == 2
    assert retained["Exclude post-ventilation-only gas"] == 3

    gas_source_summary = build_gas_source_sensitivity_summary(df).set_index("sensitivity")
    assert gas_source_summary.loc["Exclude any UNKNOWN-source gas evidence", "N_admissions"] == 3
    assert gas_source_summary.loc["Gas-positive excluding UNKNOWN-source gas", "N_admissions"] == 2
    assert gas_source_summary.loc["Any qualifying gas", "N_admissions"] == 3

    admin_summary = build_administrative_exclusion_sensitivity(df)
    admin_exclusion = admin_summary.loc[
        admin_summary["sensitivity"].eq("Exclude administrative-only RFV")
        & admin_summary["grouped_rfv_category"].eq("Respiratory")
    ].squeeze()
    assert admin_exclusion["excluded_administrative_only_n"] == 1
    assert admin_exclusion["N_admissions"] == 3
    assert admin_exclusion["percent_with_category"] == pytest.approx(66.67)

    threshold_summary = build_pco2_threshold_sensitivity_summary(df).set_index("threshold_sensitivity")
    assert threshold_summary.loc["Primary thresholds", "N_denominator"] == 3
    assert threshold_summary.loc["Primary thresholds", "N_retained"] == 3
    assert threshold_summary.loc["Exclude any UNKNOWN-source gas evidence", "N_retained"] == 2

    analytic_threshold_summary = build_analytic_cohort_threshold_sensitivity_summary(df).set_index(
        "threshold_sensitivity"
    )
    assert analytic_threshold_summary.loc["Primary thresholds", "N_denominator"] == 4
    assert analytic_threshold_summary.loc["Primary thresholds", "N_retained"] == 4

    denominator_audit = build_sensitivity_denominator_audit(df).set_index("denominator_label")
    assert denominator_audit.loc["Blood-gas-positive", "N_admissions"] == 3
    assert denominator_audit.loc["ICD-positive", "N_admissions"] == 2
    assert denominator_audit.loc["Gas-only", "N_admissions"] == 2
    assert denominator_audit.loc["ICD-only", "N_admissions"] == 1
    assert denominator_audit.loc["Both ICD + gas", "N_admissions"] == 1
    assert denominator_audit.loc["First admission per patient", "N_admissions"] == 4
    assert denominator_audit.loc["Exclude administrative-only RFV", "N_admissions"] == 3
    assert denominator_audit.loc["Exclude any UNKNOWN-source gas evidence", "N_admissions"] == 3

    icd_era_summary = build_icd_era_sensitivity_summary(df)
    assert set(icd_era_summary["icd_era"]) == {"pre_ICD10", "ICD10_era"}


def test_pco2_threshold_sensitivity_uses_overlapping_source_specific_evidence() -> None:
    df = pd.DataFrame(
        {
            "any_hypercap_icd": [0, 0, 1],
            "pco2_threshold_any": [1, 1, 0],
            "qualifying_pco2_mmhg": [49.0, 52.0, np.nan],
            "qualifying_site_group": ["ABG", "VBG", "UNKNOWN"],
            "first_abg_hypercap_pco2_mmhg": [49.0, np.nan, np.nan],
            "first_vbg_hypercap_pco2_mmhg": [56.0, 52.0, np.nan],
            "first_other_pco2": [np.nan, 58.0, np.nan],
            "unknown_hypercap_threshold": [0, 1, 0],
            "has_group_respiratory": [1, 0, 1],
        }
    )

    stricter_mask = source_specific_pco2_threshold_mask(
        df,
        abg_threshold=50.0,
        vbg_threshold=55.0,
        unknown_threshold=55.0,
    )
    assert stricter_mask.tolist() == [True, True, False]

    threshold_summary = build_pco2_threshold_sensitivity_summary(df).set_index("threshold_sensitivity")
    assert threshold_summary.loc["ABG>=50 VBG>=55 UNKNOWN>=55", "N_retained"] == 2
    assert (
        threshold_summary.loc["ABG>=50 VBG>=55 UNKNOWN>=55", "pco2_evidence_rule"]
        == "source-specific exported PCO2 evidence"
    )

    analytic_summary = build_analytic_cohort_threshold_sensitivity_summary(df).set_index(
        "threshold_sensitivity"
    )
    assert analytic_summary.loc["ABG>=50 VBG>=55 UNKNOWN>=55", "N_retained"] == 3


def test_administrative_only_mask_handles_grouped_and_canonical_indicators() -> None:
    common = {
        "subject_id": [1, 2, 3],
        "any_hypercap_icd": [1, 1, 0],
        "pco2_threshold_any": [0, 0, 1],
        "dt_qualifying_hypercapnia_hours": [np.nan, np.nan, 5.0],
        "ed_anchor_time": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
        "admittime": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
        "hadm_id": [11, 12, 13],
        "ed_stay_id": [21, 22, 23],
        "unknown_hypercap_threshold": [0, 0, 0],
    }
    grouped_only_df = pd.DataFrame(
        {
            **common,
            "has_group_administrative": [1, 1, 0],
            "has_group_respiratory": [1, 0, 1],
        }
    )

    assert _administrative_only_rfv_mask(grouped_only_df).tolist() == [False, True, False]
    admin_summary = build_administrative_exclusion_sensitivity(grouped_only_df)
    admin_exclusion = admin_summary.loc[
        admin_summary["sensitivity"].eq("Exclude administrative-only RFV")
        & admin_summary["grouped_rfv_category"].eq("Respiratory")
    ].squeeze()
    assert admin_exclusion["excluded_administrative_only_n"] == 1
    assert admin_exclusion["N_admissions"] == 2
    assert admin_exclusion["n_with_category"] == 2

    denominator_audit = build_sensitivity_denominator_audit(grouped_only_df).set_index("denominator_label")
    assert denominator_audit.loc["Exclude administrative-only RFV", "N_admissions"] == 2

    canonical_only_df = pd.DataFrame(
        {
            **common,
            "has_rfv_administrative": [1, 1, 0],
            "has_rfv_respiratory": [1, 0, 1],
            "has_group_respiratory": [1, 0, 1],
        }
    )
    assert _administrative_only_rfv_mask(canonical_only_df).tolist() == [False, True, False]
    canonical_admin_summary = build_administrative_exclusion_sensitivity(canonical_only_df)
    canonical_admin_exclusion = canonical_admin_summary.loc[
        canonical_admin_summary["sensitivity"].eq("Exclude administrative-only RFV")
        & canonical_admin_summary["grouped_rfv_category"].eq("Respiratory")
    ].squeeze()
    assert canonical_admin_exclusion["excluded_administrative_only_n"] == 1
    assert canonical_admin_exclusion["N_admissions"] == 2


def test_sensitivity_denominator_audit_requires_subject_id_for_first_admission() -> None:
    df = pd.DataFrame(
        {
            "any_hypercap_icd": [1, 0],
            "pco2_threshold_any": [0, 1],
            "dt_qualifying_hypercapnia_hours": [np.nan, 4.0],
            "unknown_hypercap_threshold": [0, 0],
            "has_rfv_administrative": [0, 0],
        }
    )

    with pytest.raises(KeyError, match="subject_id"):
        build_sensitivity_denominator_audit(df)

    df_with_missing_subject = df.assign(subject_id=[1, pd.NA])
    with pytest.raises(ValueError, match="missing subject_id"):
        build_sensitivity_denominator_audit(df_with_missing_subject)


def test_acid_base_source_missingness_output_is_aggregate_only() -> None:
    df = pd.DataFrame(
        {
            "subject_id": [1, 2, 3],
            "hadm_id": [10, 20, 30],
            "ed_stay_id": [100, 200, 300],
            "chiefcomplaint": ["dyspnea", "fall", "weakness"],
            "qualifying_pco2_mmhg": [60.0, 55.0, np.nan],
            "qualifying_ph": [7.20, np.nan, np.nan],
            "first_hco3": [31.0, 24.0, np.nan],
            "first_hco3_qc_flag": [1, 1, 0],
            "dt_qualifying_hypercapnia_hours": [5.0, 30.0, np.nan],
            "abg_hypercap_threshold": [1, 0, 0],
            "vbg_hypercap_threshold": [0, 1, 0],
            "unknown_hypercap_threshold": [0, 0, 0],
        }
    )

    out = build_acid_base_source_missingness_table(df)
    counts = out.set_index("measure")["n_admissions"].to_dict()
    assert counts["Any qualifying PCO2"] == 2
    assert counts["pH available"] == 1
    assert counts["HCO3 available"] == 2
    assert counts["First qualifying gas within 6h"] == 1
    forbidden_columns = {"subject_id", "hadm_id", "ed_stay_id", "chiefcomplaint", "chief_complaint"}
    assert forbidden_columns.isdisjoint(out.columns)
    assert out["row_level_identifiers_exported"].eq(False).all()


def _synthetic_imv_timing_frame() -> pd.DataFrame:
    gas_time = pd.Timestamp("2026-01-01 12:00:00")
    rows: list[dict[str, object]] = []
    stratum_specs = (
        {
            "order": "no_observed_imv",
            "robust": 0,
            "source": "missing",
            "imv_time": pd.NaT,
            "derived_time": pd.NaT,
            "intubation_time": pd.NaT,
            "ventilation_procedure_time": pd.NaT,
            "preceded": False,
            "no_prior": True,
            "hours": np.nan,
            "rfv": "Symptom – Respiratory",
        },
        {
            "order": "qualifying_gas_before_imv",
            "robust": 1,
            "source": "derived_ventilation_episode",
            "imv_time": gas_time + pd.Timedelta(hours=1),
            "derived_time": gas_time + pd.Timedelta(hours=1),
            "intubation_time": pd.NaT,
            "ventilation_procedure_time": pd.NaT,
            "preceded": False,
            "no_prior": True,
            "hours": -1.0,
            "rfv": "Symptom – Respiratory",
        },
        {
            "order": "imv_before_qualifying_gas",
            "robust": 1,
            "source": "intubation_procedure",
            "imv_time": gas_time - pd.Timedelta(hours=1),
            "derived_time": pd.NaT,
            "intubation_time": gas_time - pd.Timedelta(hours=1),
            "ventilation_procedure_time": pd.NaT,
            "preceded": True,
            "no_prior": False,
            "hours": 1.0,
            "rfv": "Injuries & adverse effects",
        },
        {
            "order": "timing_indeterminate",
            "robust": 1,
            "source": "invasive_ventilation_procedure",
            "imv_time": gas_time,
            "derived_time": pd.NaT,
            "intubation_time": pd.NaT,
            "ventilation_procedure_time": gas_time,
            "preceded": pd.NA,
            "no_prior": pd.NA,
            "hours": 0.0,
            "rfv": "Injuries & adverse effects",
        },
    )
    for spec in stratum_specs:
        for _ in range(2):
            row_number = len(rows) + 1
            rows.append(
                {
                    "subject_id": row_number,
                    "hadm_id": 100 + row_number,
                    "age": 40 + row_number,
                    "death_in_hosp": int(row_number % 3 == 0),
                    "imv_flag": int(bool(spec["robust"])),
                    "first_imv_time": spec["imv_time"],
                    "abg_hypercap_threshold": int(row_number % 2 == 0),
                    "vbg_hypercap_threshold": int(row_number % 2 == 1),
                    "any_hypercap_icd": int(row_number % 3 == 0),
                    "pco2_threshold_any": 1,
                    "qualifying_pco2_time": gas_time,
                    "first_derived_imv_starttime": spec["derived_time"],
                    "first_intubation_procedure_time": spec["intubation_time"],
                    "first_invasive_ventilation_procedure_time": spec[
                        "ventilation_procedure_time"
                    ],
                    "first_observed_imv_time": spec["imv_time"],
                    "first_observed_imv_source": spec["source"],
                    "robust_imv_observed": spec["robust"],
                    "imv_qualifying_gas_order": spec["order"],
                    "imv_preceded_qualifying_gas": spec["preceded"],
                    "no_prior_observed_imv": spec["no_prior"],
                    "hours_from_imv_to_qualifying_gas": spec["hours"],
                    "legacy_imv_timing_discordant": 0,
                    "RFV1_name": spec["rfv"],
                    "RFV2_name": pd.NA,
                    "RFV3_name": pd.NA,
                    "RFV4_name": pd.NA,
                    "RFV5_name": pd.NA,
                }
            )

    rows.append(
        {
            "subject_id": 9,
            "hadm_id": 109,
            "age": 72,
            "death_in_hosp": 0,
            "imv_flag": 0,
            "first_imv_time": pd.NaT,
            "abg_hypercap_threshold": 0,
            "vbg_hypercap_threshold": 0,
            "any_hypercap_icd": 1,
            "pco2_threshold_any": 0,
            "qualifying_pco2_time": pd.NaT,
            "first_derived_imv_starttime": pd.NaT,
            "first_intubation_procedure_time": pd.NaT,
            "first_invasive_ventilation_procedure_time": pd.NaT,
            "first_observed_imv_time": pd.NaT,
            "first_observed_imv_source": "missing",
            "robust_imv_observed": 0,
            "imv_qualifying_gas_order": "not_applicable_no_qualifying_gas",
            "imv_preceded_qualifying_gas": pd.NA,
            "no_prior_observed_imv": pd.NA,
            "hours_from_imv_to_qualifying_gas": np.nan,
            "legacy_imv_timing_discordant": 0,
            "RFV1_name": "Symptom – Digestive",
            "RFV2_name": pd.NA,
            "RFV3_name": pd.NA,
            "RFV4_name": pd.NA,
            "RFV5_name": pd.NA,
        }
    )
    return pd.DataFrame(rows)


def test_imv_timing_analysis_partition_prevalence_and_paired_sensitivity() -> None:
    frame = _synthetic_imv_timing_frame()
    contract = validate_imv_timing_analysis_contract(frame)
    assert contract["analytic_admissions"] == 9
    assert contract["gas_positive_admissions"] == 8
    assert contract["non_gas_admissions"] == 1
    assert contract["gas_strata_reconciled_admissions"] == 8
    assert contract["no_prior_observed_imv_admissions"] == 4

    gas_frame = prepare_imv_timing_gas_positive_frame(frame)
    group_yield = build_imv_timing_group_yield(gas_frame)
    assert group_yield["admissions"].tolist() == [2, 2, 2, 2]
    assert group_yield["admissions"].sum() == len(gas_frame)
    characteristics = build_imv_timing_group_characteristics(gas_frame)
    assert characteristics["admissions"].tolist() == [2, 2, 2, 2]

    grouped_prevalence = summarize_imv_timing_rfv_prevalence(
        gas_frame,
        grouped=True,
    )
    assert len(grouped_prevalence) == 24
    assert grouped_prevalence["cluster_unit"].eq("patient").all()
    assert grouped_prevalence[["ci_lower", "ci_upper"]].notna().all().all()

    comparison = paired_cluster_bootstrap_imv_no_prior_sensitivity(
        gas_frame,
        grouped=True,
        n_bootstrap=400,
        seed=17,
    )
    respiratory = comparison.loc[comparison["category"].eq("Respiratory")].iloc[0]
    assert respiratory["all_gas_positive_prevalence_percent"] == pytest.approx(50.0)
    assert respiratory["no_prior_observed_imv_prevalence_percent"] == pytest.approx(
        100.0
    )
    assert respiratory["difference_pp_no_prior_minus_all"] == pytest.approx(50.0)
    assert respiratory["absolute_difference_pp"] == pytest.approx(50.0)
    assert comparison["cluster_unit"].eq("patient").all()
    assert comparison["nested_paired_comparison"].eq(True).all()
    assert comparison["null_hypothesis_test_performed"].eq(False).all()

    summary = build_imv_timing_manuscript_summary(
        group_yield,
        grouped_prevalence,
        comparison,
    )
    assert "largest absolute grouped-RFV contrast was for Respiratory" in summary
    assert "50.0% among all gas-positive admissions versus 100.0%" in summary
    assert "+50.0 percentage points" in summary
    assert "does not establish that ventilation caused hypercapnia" in summary


def test_imv_timing_analysis_accepts_untimed_official_source_handoff() -> None:
    frame = _synthetic_imv_timing_frame()
    untimed_source_row = frame.index[0]
    frame.loc[untimed_source_row, "imv_qualifying_gas_order"] = "timing_indeterminate"
    frame.loc[untimed_source_row, "imv_preceded_qualifying_gas"] = pd.NA
    frame.loc[untimed_source_row, "no_prior_observed_imv"] = pd.NA

    contract = validate_imv_timing_analysis_contract(frame)

    assert contract["timing_indeterminate_admissions"] == 3
    assert contract["no_prior_observed_imv_admissions"] == 3


def test_imv_timing_analysis_rejects_reconstructable_order_mismatch() -> None:
    frame = _synthetic_imv_timing_frame()
    imv_before_row = frame.index[4]
    frame.loc[imv_before_row, "imv_qualifying_gas_order"] = "no_observed_imv"
    frame.loc[imv_before_row, "imv_preceded_qualifying_gas"] = False
    frame.loc[imv_before_row, "no_prior_observed_imv"] = True

    with pytest.raises(ValueError, match="strict cohort fields"):
        validate_imv_timing_analysis_contract(frame)


def test_imv_timing_analysis_rejects_robust_flag_not_derived_from_sources() -> None:
    frame = _synthetic_imv_timing_frame()
    gas_before_row = frame.index[2]
    frame.loc[gas_before_row, "robust_imv_observed"] = 0

    with pytest.raises(ValueError, match="robust_imv_observed must equal presence"):
        validate_imv_timing_analysis_contract(frame)


def test_imv_timing_analysis_rejects_self_consistent_nonminimum_anchor() -> None:
    frame = _synthetic_imv_timing_frame()
    gas_before_row = frame.index[2]
    gas_time = frame.loc[gas_before_row, "qualifying_pco2_time"]
    frame.loc[gas_before_row, "first_observed_imv_time"] = gas_time + pd.Timedelta(
        hours=2
    )
    frame.loc[gas_before_row, "hours_from_imv_to_qualifying_gas"] = -2.0

    with pytest.raises(ValueError, match="exact earliest reliable source"):
        validate_imv_timing_analysis_contract(frame)


def test_imv_timing_analysis_rejects_incorrect_tied_source_label() -> None:
    frame = _synthetic_imv_timing_frame()
    gas_before_row = frame.index[2]
    frame.loc[gas_before_row, "first_intubation_procedure_time"] = frame.loc[
        gas_before_row, "first_derived_imv_starttime"
    ]

    with pytest.raises(
        ValueError,
        match=r"source_violations=1",
    ):
        validate_imv_timing_analysis_contract(frame)


def test_imv_timing_analysis_rejects_same_sign_inexact_hours() -> None:
    frame = _synthetic_imv_timing_frame()
    gas_before_row = frame.index[2]
    frame.loc[gas_before_row, "hours_from_imv_to_qualifying_gas"] = -0.5

    with pytest.raises(ValueError, match="exact source-derived timestamp difference"):
        validate_imv_timing_analysis_contract(frame)


def test_imv_timing_analysis_rejects_incorrect_legacy_discordance() -> None:
    frame = _synthetic_imv_timing_frame()
    gas_before_row = frame.index[2]
    frame.loc[gas_before_row, "imv_flag"] = 0

    with pytest.raises(ValueError, match="source-derived presence/order rule"):
        validate_imv_timing_analysis_contract(frame)


@pytest.mark.parametrize(
    "field_name",
    [
        "first_derived_imv_starttime",
        "first_observed_imv_time",
        "first_imv_time",
    ],
)
def test_imv_timing_analysis_rejects_unparseable_nonmissing_timestamps(
    field_name: str,
) -> None:
    frame = _synthetic_imv_timing_frame()
    no_observed_row = frame.index[0]
    frame[field_name] = frame[field_name].astype("object")
    frame.loc[no_observed_row, field_name] = "not-a-time"

    with pytest.raises(ValueError, match="nonmissing unparseable timestamps"):
        validate_imv_timing_analysis_contract(frame)


@pytest.mark.parametrize("invalid_hours", ["bad", np.inf, -np.inf])
def test_imv_timing_analysis_rejects_malformed_or_nonfinite_hours(
    invalid_hours: object,
) -> None:
    frame = _synthetic_imv_timing_frame()
    no_observed_row = frame.index[0]
    frame["hours_from_imv_to_qualifying_gas"] = frame[
        "hours_from_imv_to_qualifying_gas"
    ].astype("object")
    frame.loc[no_observed_row, "hours_from_imv_to_qualifying_gas"] = invalid_hours

    with pytest.raises(ValueError, match="unparseable or nonfinite values"):
        validate_imv_timing_analysis_contract(frame)


def test_imv_timing_aggregate_export_privacy_rejects_identifier_columns() -> None:
    safe_sheet = pd.DataFrame({"temporal_stratum": ["No observed IMV"], "admissions": [2]})
    assert_imv_timing_export_privacy({"Group_Yield": safe_sheet})

    with pytest.raises(AssertionError, match="restricted columns"):
        assert_imv_timing_export_privacy(
            {"Unsafe": pd.DataFrame({"hadm_id": [100], "admissions": [1]})}
        )
