from __future__ import annotations

import ast
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest


WORK_DIR = Path(__file__).resolve().parents[1]
COHORT_NOTEBOOK = WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd"
HELPER_NAMES = {
    "normalize_mimic_item_label",
    "prepare_archive_export_frame",
    "select_first_observed_imv",
    "classify_imv_qualifying_gas_order",
    "legacy_imv_timing_discordant",
    "imv_source_projection_sha256",
    "build_imv_source_provenance",
    "write_imv_source_provenance",
}
HELPER_CONSTANTS = {
    "IMV_SOURCE_RECORD_NO_TIME_COLUMN",
    "IMV_SOURCE_PROVENANCE_FILENAME",
    "IMV_SOURCE_PROVENANCE_SCHEMA_VERSION",
    "IMV_SOURCE_PROJECTION_COLUMNS",
}


def _load_imv_helpers() -> dict[str, object]:
    chunks = re.findall(
        r"```\{python\}\n(.*?)```", COHORT_NOTEBOOK.read_text(), flags=re.S
    )
    tree = ast.parse("\n\n".join(chunks))
    selected = [
        node
        for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in HELPER_NAMES)
        or (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id in HELPER_CONSTANTS
                for target in node.targets
            )
        )
    ]
    namespace = {
        "Any": Any,
        "Path": Path,
        "hashlib": hashlib,
        "json": json,
        "np": np,
        "os": os,
        "pd": pd,
        "re": re,
    }
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
prepare_archive_export_frame = HELPERS["prepare_archive_export_frame"]
build_imv_source_provenance = HELPERS["build_imv_source_provenance"]
imv_source_projection_sha256 = HELPERS["imv_source_projection_sha256"]
write_imv_source_provenance = HELPERS["write_imv_source_provenance"]


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


def test_archive_export_sanitizer_drops_internal_imv_flag_without_mutation() -> None:
    internal_column = "_imv_source_record_evidence_without_reliable_timestamp"
    source = pd.DataFrame(
        {
            "hadm_id": [1, 2],
            internal_column: [False, True],
            "value": ["a", "b"],
        }
    )
    original = source.copy(deep=True)

    exported = prepare_archive_export_frame(source, export_name="test_archive")

    assert internal_column not in exported.columns
    assert list(exported.columns) == ["hadm_id", "value"]
    assert exported is not source
    pd.testing.assert_frame_equal(source, original)


def test_archive_export_sanitizer_returns_detached_copy_when_flag_absent() -> None:
    source = pd.DataFrame({"value": [1]})

    exported = prepare_archive_export_frame(source, export_name="test_archive")
    exported.loc[0, "value"] = 99

    assert source.loc[0, "value"] == 1


def _synthetic_source_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "subject_id": [1, 1, 2],
            "hadm_id": [101, 102, 103],
            "pco2_threshold_any": [1, 1, 0],
            "imv_flag": [0, 0, 0],
            "qualifying_pco2_time": [pd.Timestamp("2026-01-01 12:00:00"), pd.NaT, pd.NaT],
            "first_derived_imv_starttime": [pd.NaT, pd.NaT, pd.NaT],
            "first_intubation_procedure_time": [pd.NaT, pd.NaT, pd.NaT],
            "first_invasive_ventilation_procedure_time": [pd.NaT, pd.NaT, pd.NaT],
            "first_imv_time": [pd.NaT, pd.NaT, pd.NaT],
            HELPERS["IMV_SOURCE_RECORD_NO_TIME_COLUMN"]: [1, 0, 1],
        }
    )
    return frame


def test_imv_provenance_membership_comes_only_from_source_evidence_flag() -> None:
    source = _synthetic_source_frame()
    source["imv_qualifying_gas_order"] = "no_observed_imv"
    original = source.copy(deep=True)

    payload = build_imv_source_provenance(source)

    assert payload["untimed_evidence_admissions"] == [
        {"subject_id": 1, "hadm_id": 101},
        {"subject_id": 2, "hadm_id": 103},
    ]
    assert payload["admission_count"] == 3
    source["imv_qualifying_gas_order"] = "timing_indeterminate"
    assert build_imv_source_provenance(source) == payload
    pd.testing.assert_frame_equal(source.drop(columns="imv_qualifying_gas_order"), original.drop(columns="imv_qualifying_gas_order"))


def test_imv_provenance_projection_is_stable_across_excel_pass_through(tmp_path: Path) -> None:
    source = _synthetic_source_frame()
    source.loc[1, "first_imv_time"] = pd.Timestamp("2026-01-01 12:01:02")
    expected = imv_source_projection_sha256(source)
    cohort_path = tmp_path / "cohort.xlsx"
    classified_path = tmp_path / "classified.xlsx"
    exported = prepare_archive_export_frame(source, export_name="synthetic canonical")
    exported.to_excel(cohort_path, index=False)
    loaded = pd.read_excel(cohort_path)
    assert imv_source_projection_sha256(loaded) == expected
    loaded["RFV1_name"] = "Synthetic category"
    loaded.iloc[::-1].to_excel(classified_path, index=False)

    assert imv_source_projection_sha256(pd.read_excel(classified_path)) == expected
    assert HELPERS["IMV_SOURCE_RECORD_NO_TIME_COLUMN"] not in loaded.columns


@pytest.mark.parametrize("invalid_flag", [pd.NA, -1, 2, "bad"])
def test_imv_provenance_requires_unambiguous_internal_source_flag(invalid_flag: object) -> None:
    source = _synthetic_source_frame()
    flag_column = HELPERS["IMV_SOURCE_RECORD_NO_TIME_COLUMN"]
    source[flag_column] = source[flag_column].astype("object")
    source.loc[0, flag_column] = invalid_flag

    with pytest.raises(ValueError, match="nonmissing binary source-evidence flag"):
        build_imv_source_provenance(source)


def test_imv_provenance_cannot_be_built_from_export_without_source_flag() -> None:
    source = _synthetic_source_frame().drop(columns=HELPERS["IMV_SOURCE_RECORD_NO_TIME_COLUMN"])

    with pytest.raises(KeyError, match="in-memory source-evidence flag"):
        build_imv_source_provenance(source)


def test_imv_provenance_rejects_untimed_flag_with_reliable_source_timestamp() -> None:
    source = _synthetic_source_frame()
    source.loc[0, "first_derived_imv_starttime"] = pd.Timestamp("2026-01-01")

    with pytest.raises(ValueError, match="contradicts a reliable source timestamp"):
        build_imv_source_provenance(source)


def test_imv_provenance_writer_is_private_and_does_not_mutate_source(tmp_path: Path) -> None:
    source = _synthetic_source_frame()
    original = source.copy(deep=True)
    payload = build_imv_source_provenance(source)
    path = tmp_path / HELPERS["IMV_SOURCE_PROVENANCE_FILENAME"]

    write_imv_source_provenance(payload, path)

    assert json.loads(path.read_text()) == payload
    assert path.stat().st_mode & 0o077 == 0
    assert sorted(item.name for item in tmp_path.iterdir()) == [path.name]
    pd.testing.assert_frame_equal(source, original)
