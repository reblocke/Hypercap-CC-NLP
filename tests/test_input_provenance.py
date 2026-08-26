from __future__ import annotations

import ast
import hashlib
import io
import os
import re
from pathlib import Path

import pandas as pd
import pytest


WORK_DIR = Path(__file__).resolve().parents[1]
NOTEBOOKS = (
    "Hypercap CC NLP Classifier.qmd",
    "Hypercap CC NLP Analysis.qmd",
)


def _input_helper(notebook_name: str):
    notebook = WORK_DIR / notebook_name
    chunks = re.findall(r"```\{python\}\n(.*?)```", notebook.read_text(), flags=re.S)
    tree = ast.parse("\n\n".join(chunks))
    selected = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "read_excel_with_sha256"
    ]
    assert len(selected) == 1
    namespace = {
        "Path": Path,
        "pd": pd,
        "hashlib": hashlib,
        "io": io,
        "BytesIO": io.BytesIO,
    }
    exec(
        compile(ast.Module(body=selected, type_ignores=[]), str(notebook), "exec"),
        namespace,
    )
    return namespace["read_excel_with_sha256"]


def _workbook_bytes(value: int) -> bytes:
    stream = io.BytesIO()
    pd.DataFrame({"synthetic_value": [value]}).to_excel(stream, index=False)
    return stream.getvalue()


@pytest.mark.parametrize("notebook_name", NOTEBOOKS)
def test_input_manifest_hash_binds_exact_parsed_bytes(
    notebook_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    helper = _input_helper(notebook_name)
    path = tmp_path / "synthetic.xlsx"
    original = _workbook_bytes(1)
    replacement = _workbook_bytes(2)
    path.write_bytes(original)
    read_excel = pd.read_excel

    def replace_disk_input_before_parse(source, **kwargs):
        path.write_bytes(replacement)
        return read_excel(source, **kwargs)

    monkeypatch.setattr(pd, "read_excel", replace_disk_input_before_parse)
    frame, digest = helper(path)

    assert frame["synthetic_value"].tolist() == [1]
    assert digest == hashlib.sha256(original).hexdigest()
    assert digest != hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    ("notebook_name", "production_assignment", "manifest_assignment"),
    (
        (
            NOTEBOOKS[0],
            "df, cohort_input_sha256 = read_excel_with_sha256(file_path)",
            '"cohort_sha256": cohort_input_sha256',
        ),
        (
            NOTEBOOKS[1],
            "input_df, analysis_input_sha256 = read_excel_with_sha256(ANALYSIS_INPUT_PATH)",
            'analysis_manifest["analysis_input_sha256"] = analysis_input_sha256',
        ),
    ),
)
def test_production_input_uses_the_parsed_bytes_digest(
    notebook_name: str, production_assignment: str, manifest_assignment: str
) -> None:
    text = (WORK_DIR / notebook_name).read_text()
    assert production_assignment in text
    assert manifest_assignment in text


@pytest.mark.parametrize(
    ("notebook_name", "manifest_name", "path_name", "stage"),
    (
        ("MIMICIV_hypercap_EXT_cohort.qmd", "cohort_manifest", "cohort_manifest_path", "cohort"),
        (NOTEBOOKS[0], "stage_manifest", "stage_manifest_path", "classifier"),
        (NOTEBOOKS[1], "analysis_manifest", "analysis_manifest_path", "analysis"),
    ),
)
def test_producer_manifest_uses_frozen_results_date(
    notebook_name: str,
    manifest_name: str,
    path_name: str,
    stage: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frozen_date = "2001-02-03"
    monkeypatch.setenv("RESULTS_DATE", frozen_date)
    notebook = WORK_DIR / notebook_name
    chunks = re.findall(r"```\{python\}\n(.*?)```", notebook.read_text(), flags=re.S)
    tree = ast.parse("\n\n".join(chunks))
    selected = []
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id in {"manifest_results_date", path_name}:
            selected.append(node)
        elif (
            isinstance(target, ast.Subscript)
            and isinstance(target.value, ast.Name)
            and target.value.id == manifest_name
            and isinstance(target.slice, ast.Constant)
            and target.slice.value == "results_date"
        ):
            selected.append(node)
    namespace = {
        "os": os,
        "RESULTS_DATE": frozen_date,
        "results_date": frozen_date,
        "out_date": "2099-12-31",
        "run_date": "2099-12-31",
        "PRIOR_RUNS_DIR": tmp_path,
        "prior_runs_dir": tmp_path,
        "archive_output_path": tmp_path / "synthetic.xlsx",
        manifest_name: {},
    }
    exec(
        compile(ast.Module(body=selected, type_ignores=[]), str(notebook), "exec"),
        namespace,
    )
    assert namespace[manifest_name]["results_date"] == frozen_date
    assert namespace[path_name] == tmp_path / f"{frozen_date} {stage}_run_manifest.json"
