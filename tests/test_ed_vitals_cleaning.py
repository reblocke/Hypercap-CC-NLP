from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd


WORK_DIR = Path(__file__).resolve().parents[1]
COHORT_NOTEBOOK = WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd"


def _extract_function_source(notebook_text: str, function_name: str) -> str:
    pattern = re.compile(rf"^def {function_name}\(", flags=re.MULTILINE)
    match = pattern.search(notebook_text)
    if not match:
        raise AssertionError(f"Function {function_name} not found in cohort notebook.")
    start = match.start()
    next_def = re.search(r"^def [A-Za-z_][A-Za-z0-9_]*\(", notebook_text[match.end() :], flags=re.MULTILINE)
    end = match.end() + next_def.start() if next_def else len(notebook_text)
    return notebook_text[start:end]


def _load_notebook_functions() -> dict[str, object]:
    text = COHORT_NOTEBOOK.read_text()
    namespace: dict[str, object] = {"np": np, "pd": pd}
    for name in (
        "normalize_temperature_to_f",
        "clean_pain_score",
        "clean_bp",
        "clean_o2sat",
    ):
        source = _extract_function_source(text, name)
        exec(source, namespace)
    return namespace


def test_normalize_temperature_to_f() -> None:
    funcs = _load_notebook_functions()
    normalize_temperature_to_f = funcs["normalize_temperature_to_f"]
    values = pd.Series([36.5, 98.6, 6.0, 200.0], dtype="float64")
    result = normalize_temperature_to_f(values)

    assert round(float(result.loc[0, "temp_f_clean"]), 1) == 97.7
    assert bool(result.loc[0, "temp_was_celsius_like"]) is True
    assert bool(result.loc[0, "temp_out_of_range"]) is False

    assert round(float(result.loc[1, "temp_f_clean"]), 1) == 98.6
    assert bool(result.loc[1, "temp_was_celsius_like"]) is False
    assert bool(result.loc[1, "temp_out_of_range"]) is False

    assert pd.isna(result.loc[2, "temp_f_clean"])
    assert bool(result.loc[2, "temp_was_celsius_like"]) is False
    assert bool(result.loc[2, "temp_out_of_range"]) is True

    assert pd.isna(result.loc[3, "temp_f_clean"])
    assert bool(result.loc[3, "temp_was_celsius_like"]) is False
    assert bool(result.loc[3, "temp_out_of_range"]) is True


def test_clean_pain_score() -> None:
    funcs = _load_notebook_functions()
    clean_pain_score = funcs["clean_pain_score"]
    values = pd.Series([0, 5, 10, 13, -1, 11], dtype="float64")
    result = clean_pain_score(values)

    assert result["pain_clean"].tolist()[:3] == [0.0, 5.0, 10.0]
    assert pd.isna(result.loc[3, "pain_clean"])
    assert bool(result.loc[3, "pain_is_sentinel_13"]) is True
    assert bool(result.loc[3, "pain_out_of_range"]) is False

    assert pd.isna(result.loc[4, "pain_clean"])
    assert bool(result.loc[4, "pain_out_of_range"]) is True
    assert pd.isna(result.loc[5, "pain_clean"])
    assert bool(result.loc[5, "pain_out_of_range"]) is True


def test_clean_bp() -> None:
    funcs = _load_notebook_functions()
    clean_bp = funcs["clean_bp"]
    sbp = pd.Series([120, 10, 400], dtype="float64")
    dbp = pd.Series([70, 5, 250], dtype="float64")
    result = clean_bp(sbp, dbp)

    assert result.loc[0, "sbp_clean"] == 120.0
    assert pd.isna(result.loc[1, "sbp_clean"])
    assert pd.isna(result.loc[2, "sbp_clean"])
    assert bool(result.loc[1, "sbp_out_of_range"]) is True
    assert bool(result.loc[2, "sbp_out_of_range"]) is True

    assert result.loc[0, "dbp_clean"] == 70.0
    assert pd.isna(result.loc[1, "dbp_clean"])
    assert pd.isna(result.loc[2, "dbp_clean"])
    assert bool(result.loc[1, "dbp_out_of_range"]) is True
    assert bool(result.loc[2, "dbp_out_of_range"]) is True


def test_clean_o2sat() -> None:
    funcs = _load_notebook_functions()
    clean_o2sat = funcs["clean_o2sat"]
    values = pd.Series([98, 100, 101, -1, 0], dtype="float64")
    result = clean_o2sat(values)

    assert result.loc[0, "o2sat_clean"] == 98.0
    assert result.loc[1, "o2sat_clean"] == 100.0
    assert pd.isna(result.loc[2, "o2sat_clean"])
    assert bool(result.loc[2, "o2sat_gt_100"]) is True

    assert pd.isna(result.loc[3, "o2sat_clean"])
    assert bool(result.loc[3, "o2sat_out_of_range"]) is True

    assert result.loc[4, "o2sat_clean"] == 0.0
    assert bool(result.loc[4, "o2sat_zero"]) is True
