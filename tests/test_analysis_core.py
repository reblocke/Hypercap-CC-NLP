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
