from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import pandas as pd


WORK_DIR = Path(__file__).resolve().parents[1]
COHORT_NOTEBOOK = WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd"
HELPER_NAMES = {
    "normalize_mimic_item_label",
    "select_first_observed_imv",
    "classify_imv_qualifying_gas_order",
    "legacy_imv_timing_discordant",
}


def _load_imv_helpers() -> dict[str, object]:
    chunks = re.findall(
        r"```\{python\}\n(.*?)```", COHORT_NOTEBOOK.read_text(), flags=re.S
    )
    tree = ast.parse("\n\n".join(chunks))
    selected = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in HELPER_NAMES
    ]
    namespace = {"Any": Any, "pd": pd, "re": re}
    exec(
        compile(
            ast.Module(body=selected, type_ignores=[]),
            str(COHORT_NOTEBOOK),
            "exec",
        ),
        namespace,
    )
    missing = HELPER_NAMES.difference(namespace)
    assert not missing, f"Missing notebook-local IMV helpers: {sorted(missing)}"
    return namespace


HELPERS = _load_imv_helpers()
select_first_observed_imv = HELPERS["select_first_observed_imv"]
classify_imv_qualifying_gas_order = HELPERS[
    "classify_imv_qualifying_gas_order"
]
legacy_imv_timing_discordant = HELPERS["legacy_imv_timing_discordant"]
normalize_mimic_item_label = HELPERS["normalize_mimic_item_label"]


def _classify(
    *,
    gas_positive: bool = True,
    gas_time: str | None = "2026-01-01 12:00:00",
    robust_observed: bool = False,
    imv_time: str | None = None,
    legacy_flag: bool = False,
) -> str:
    return classify_imv_qualifying_gas_order(
        gas_positive=gas_positive,
        qualifying_pco2_time=pd.Timestamp(gas_time) if gas_time else pd.NaT,
        robust_imv_observed=robust_observed,
        first_observed_imv_time=pd.Timestamp(imv_time) if imv_time else pd.NaT,
        legacy_imv_flag=legacy_flag,
    )


def test_icd_only_admission_is_not_applicable() -> None:
    assert _classify(gas_positive=False, gas_time=None) == (
        "not_applicable_no_qualifying_gas"
    )


def test_gas_positive_without_imv_evidence_has_no_observed_imv() -> None:
    assert _classify() == "no_observed_imv"


def test_qualifying_gas_one_second_before_imv_uses_strict_ordering() -> None:
    assert _classify(
        robust_observed=True,
        imv_time="2026-01-01 12:00:01",
    ) == "qualifying_gas_before_imv"


def test_imv_one_second_before_qualifying_gas_uses_strict_ordering() -> None:
    assert _classify(
        robust_observed=True,
        imv_time="2026-01-01 11:59:59",
    ) == "imv_before_qualifying_gas"


def test_exact_timestamp_tie_is_indeterminate() -> None:
    assert _classify(
        robust_observed=True,
        imv_time="2026-01-01 12:00:00",
    ) == "timing_indeterminate"


def test_robust_evidence_with_missing_time_is_defensively_indeterminate() -> None:
    assert _classify(robust_observed=True, imv_time=None) == "timing_indeterminate"


def test_official_source_record_without_usable_time_is_indeterminate() -> None:
    assert classify_imv_qualifying_gas_order(
        gas_positive=True,
        qualifying_pco2_time=pd.Timestamp("2026-01-01 12:00:00"),
        robust_imv_observed=False,
        first_observed_imv_time=pd.NaT,
        legacy_imv_flag=False,
        source_record_imv_evidence_without_reliable_timestamp=True,
    ) == "timing_indeterminate"


def test_legacy_only_imv_evidence_is_indeterminate() -> None:
    assert _classify(legacy_flag=True) == "timing_indeterminate"


def test_earliest_reliable_imv_source_wins() -> None:
    timestamp, source = select_first_observed_imv(
        first_intubation_procedure_time=pd.Timestamp("2026-01-01 12:00:00"),
        first_invasive_ventilation_procedure_time=pd.Timestamp(
            "2026-01-01 12:05:00"
        ),
        first_derived_imv_starttime=pd.Timestamp("2026-01-01 11:55:00"),
    )
    assert timestamp == pd.Timestamp("2026-01-01 11:55:00")
    assert source == "derived_ventilation_episode"


def test_multiple_sources_tied_at_earliest_time_are_labeled_explicitly() -> None:
    timestamp, source = select_first_observed_imv(
        first_intubation_procedure_time=pd.Timestamp("2026-01-01 12:00:00"),
        first_invasive_ventilation_procedure_time=pd.Timestamp(
            "2026-01-01 12:00:00"
        ),
        first_derived_imv_starttime=pd.Timestamp("2026-01-01 12:10:00"),
    )
    assert timestamp == pd.Timestamp("2026-01-01 12:00:00")
    assert source == "multiple_sources_same_time"


def test_missing_qualifying_gas_time_is_indeterminate_for_gas_positive_row() -> None:
    assert _classify(gas_time=None) == "timing_indeterminate"


def test_no_reliable_source_returns_missing_enum() -> None:
    timestamp, source = select_first_observed_imv(
        first_intubation_procedure_time=pd.NaT,
        first_invasive_ventilation_procedure_time=None,
        first_derived_imv_starttime=pd.NaT,
    )
    assert pd.isna(timestamp)
    assert source == "missing"


def test_legacy_discordance_distinguishes_presence_and_ordering() -> None:
    common = {
        "gas_positive": True,
        "qualifying_pco2_time": pd.Timestamp("2026-01-01 12:00:00"),
    }
    assert legacy_imv_timing_discordant(
        **common,
        robust_imv_observed=True,
        first_observed_imv_time=pd.Timestamp("2026-01-01 11:00:00"),
        legacy_imv_flag=False,
        legacy_first_imv_time=pd.NaT,
    ) == 1
    assert legacy_imv_timing_discordant(
        **common,
        robust_imv_observed=True,
        first_observed_imv_time=pd.Timestamp("2026-01-01 11:00:00"),
        legacy_imv_flag=True,
        legacy_first_imv_time=pd.Timestamp("2026-01-01 13:00:00"),
    ) == 1
    assert legacy_imv_timing_discordant(
        **common,
        robust_imv_observed=True,
        first_observed_imv_time=pd.Timestamp("2026-01-01 11:00:00"),
        legacy_imv_flag=True,
        legacy_first_imv_time=pd.NaT,
    ) == 0


def test_expected_procedure_labels_normalize_to_contract_values() -> None:
    assert normalize_mimic_item_label("Intubation") == "intubation"
    assert normalize_mimic_item_label(" Invasive-Ventilation ") == (
        "invasive ventilation"
    )
