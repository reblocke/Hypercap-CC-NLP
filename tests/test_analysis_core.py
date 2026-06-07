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
    "classify_timing_group",
    "select_analysis_ph_with_source",
    "select_analysis_ph",
    "select_paired_qualifying_ph_for_figure4",
    "select_figure4_analysis_ph",
    "derive_ph_severity",
    "build_acidemia_ph_source_audit",
    "build_acidemia_severity_denominator_audit",
    "build_acidemia_timing_denominator_audit",
    "summarize_labels_per_encounter",
    "_ordered_categories_for_comorbidity_summary",
    "summarize_rfv_prevalence_by_comorbidity",
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
    "FIGURE4_USE_PAIRED_QUALIFYING_PH",
    "PREVALENCE_CI_BOOTSTRAP_REPLICATES",
    "PREVALENCE_CI_BOOTSTRAP_SEED",
    "PREVALENCE_CI_CLUSTER_UNIT",
    "PREVALENCE_CI_LEVEL",
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
classify_timing_group = HELPERS["classify_timing_group"]
select_analysis_ph_with_source = HELPERS["select_analysis_ph_with_source"]
select_analysis_ph = HELPERS["select_analysis_ph"]
select_paired_qualifying_ph_for_figure4 = HELPERS["select_paired_qualifying_ph_for_figure4"]
select_figure4_analysis_ph = HELPERS["select_figure4_analysis_ph"]
derive_ph_severity = HELPERS["derive_ph_severity"]
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
    assert respiratory["cluster_unit"] == "subject_id"


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
