"""End-to-end pipeline audit helpers for reliability and reproducibility."""

from __future__ import annotations

import json
import math
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from dotenv import dotenv_values

from .workflow_contracts import CANONICAL_COHORT_FILENAME, CANONICAL_NLP_FILENAME

PIPELINE_STAGE_COMMANDS: tuple[tuple[str, str], ...] = (
    ("01_cohort", "make notebook-cohort"),
    ("02_classifier", "make notebook-classifier"),
    ("03_rater", "make notebook-rater"),
    ("04_analysis", "make notebook-analysis"),
)

ANALYSIS_EXPORT_FILENAMES: tuple[str, ...] = (
    "Symptom_Composition_by_Hypercapnia_Definition.xlsx",
    "Symptom_Composition_Pivot_ChartReady.xlsx",
    "Symptom_Composition_by_ABG_VBG_Overlap.xlsx",
    "Symptom_Composition_by_ICD_Gas_Overlap.xlsx",
)

KEY_QA_METRICS: tuple[str, ...] = (
    "icu_link_rate",
    "pct_any_gas_0_6h",
    "pct_any_gas_0_24h",
    "gas_source_other_rate",
)

REQUIRED_ENV_KEYS: tuple[str, ...] = (
    "MIMIC_BACKEND",
    "WORK_PROJECT",
    "BQ_PHYSIONET_PROJECT",
    "BQ_DATASET_HOSP",
    "BQ_DATASET_ICU",
    "BQ_DATASET_ED",
)

ENV_KNOBS: tuple[str, ...] = (
    "WORK_DIR",
    "CLASSIFIER_INPUT_FILENAME",
    "RATER_NLP_INPUT_FILENAME",
    "RATER_ANNOTATION_PATH",
    "ANALYSIS_INPUT_FILENAME",
    "BQ_QUERY_TIMEOUT_SECS",
    "WRITE_ARCHIVE_XLSX_EXPORTS",
    "COHORT_FAIL_ON_ALL_OTHER_SOURCE",
    "COHORT_FAIL_ON_OMR_ATTACH_INCONSISTENCY",
    "COHORT_ALLOW_EMPTY_OMR",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "HF_TOKEN",
)

LOG_PATTERNS: tuple[tuple[str, str, str], ...] = (
    ("P0", r"Traceback \(most recent call last\)", "traceback"),
    ("P0", r"\bERROR\b", "error_token"),
    ("P0", r"\bException\b", "exception_token"),
    ("P1", r"RuntimeWarning", "runtime_warning"),
    ("P1", r"ConvergenceWarning", "convergence_warning"),
    ("P1", r"overflow", "overflow"),
    ("P1", r"invalid value encountered", "invalid_value"),
)

DEFAULT_ALLOWLIST_PATTERNS: tuple[str, ...] = (
    r"Skipping .*archive .* export",
)


@dataclass(frozen=True)
class DriftRule:
    """Threshold rule for a metric drift check."""

    compare: str
    warn: float
    fail: float


DRIFT_RULES: dict[str, DriftRule] = {
    "cohort_rows": DriftRule(compare="relative", warn=0.05, fail=0.10),
    "classifier_rows": DriftRule(compare="relative", warn=0.05, fail=0.10),
    "icu_link_rate": DriftRule(compare="absolute", warn=0.04, fail=0.08),
    "pct_any_gas_0_6h": DriftRule(compare="absolute", warn=0.04, fail=0.08),
    "pct_any_gas_0_24h": DriftRule(compare="absolute", warn=0.04, fail=0.08),
    "gas_source_other_rate": DriftRule(compare="absolute", warn=0.04, fail=0.08),
    "first_other_pco2_pct_eq_160_poc": DriftRule(compare="absolute", warn=0.05, fail=0.10),
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _safe_env_value(name: str, value: str | None) -> str:
    if value is None or value == "":
        return "<unset>"
    if any(token in name.upper() for token in ("TOKEN", "KEY", "SECRET", "PASSWORD")):
        return "<set>"
    return value


def _run_text_command(command: list[str], cwd: Path | None = None) -> str:
    completed = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()


def collect_run_manifest(
    work_dir: Path,
    run_id: str,
    *,
    selected_env_vars: Iterable[str] = ENV_KNOBS,
) -> dict[str, Any]:
    """Collect immutable runtime metadata for reproducibility."""
    package_names = ("pandas", "numpy", "spacy", "torch", "sentence-transformers")
    package_versions: dict[str, str] = {}
    for package_name in package_names:
        try:
            package_versions[package_name] = version(package_name)
        except PackageNotFoundError:
            package_versions[package_name] = "<missing>"

    branch = _run_text_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=work_dir)
    commit = _run_text_command(["git", "rev-parse", "HEAD"], cwd=work_dir)
    dirty = bool(_run_text_command(["git", "status", "--porcelain"], cwd=work_dir))

    return {
        "run_id": run_id,
        "generated_utc": _utc_now_iso(),
        "work_dir": str(work_dir),
        "python_version": sys.version,
        "uv_version": _run_text_command(["uv", "--version"], cwd=work_dir),
        "git": {
            "branch": branch,
            "commit": commit,
            "dirty": dirty,
        },
        "package_versions": package_versions,
        "env_knobs": {
            key: _safe_env_value(key, os.getenv(key))
            for key in selected_env_vars
        },
    }


def run_preflight_checks(work_dir: Path) -> list[dict[str, Any]]:
    """Run reproducibility preflight checks before full execution."""
    findings: list[dict[str, Any]] = []
    env_path = work_dir / ".env"
    if not env_path.exists():
        findings.append(
            {
                "severity": "P1",
                "category": "preflight",
                "code": "missing_dotenv_file",
                "message": "Missing .env file in repo root.",
            }
        )
    else:
        values = dotenv_values(env_path)
        missing_keys = sorted(key for key in REQUIRED_ENV_KEYS if not values.get(key))
        if missing_keys:
            findings.append(
                {
                    "severity": "P1",
                    "category": "preflight",
                    "code": "missing_required_env_keys",
                    "message": f"Missing required .env keys: {missing_keys}",
                }
            )
        if not values.get("WORK_DIR"):
            findings.append(
                {
                    "severity": "P2",
                    "category": "preflight",
                    "code": "missing_work_dir_env_key",
                    "message": "Optional .env key WORK_DIR is unset; reproducibility manifest will rely on cwd.",
                }
            )
        work_dir_value = values.get("WORK_DIR")
        if work_dir_value and Path(str(work_dir_value)).expanduser().resolve() != work_dir:
            findings.append(
                {
                    "severity": "P2",
                    "category": "preflight",
                    "code": "work_dir_mismatch",
                    "message": ".env WORK_DIR does not match current repository path.",
                }
            )

    try:
        adc_check = subprocess.run(
            ["gcloud", "auth", "application-default", "print-access-token"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if adc_check.returncode != 0:
            findings.append(
                {
                    "severity": "P1",
                    "category": "preflight",
                    "code": "adc_unavailable",
                    "message": "Google ADC check failed for current environment.",
                }
            )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        findings.append(
            {
                "severity": "P1",
                "category": "preflight",
                "code": "gcloud_unavailable",
                "message": "gcloud is unavailable or ADC check timed out.",
            }
        )

    try:
        import spacy  # type: ignore

        spacy.load("en_core_web_sm")
    except Exception:
        findings.append(
            {
                "severity": "P1",
                "category": "preflight",
                "code": "missing_spacy_model",
                "message": "spaCy model en_core_web_sm is not loadable.",
            }
        )

    return findings


def _run_stage_command(
    command: str,
    *,
    cwd: Path,
    log_path: Path,
) -> dict[str, Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )

    line_count = 0
    with log_path.open("w") as handle:
        assert process.stdout is not None
        for line in process.stdout:
            line_count += 1
            handle.write(line)
            print(line, end="")

    return_code = process.wait()
    ended = datetime.now(timezone.utc)
    return {
        "command": command,
        "returncode": int(return_code),
        "started_utc": started.isoformat(),
        "ended_utc": ended.isoformat(),
        "duration_s": float((ended - started).total_seconds()),
        "line_count": line_count,
        "log_path": str(log_path),
    }


def run_pipeline_with_logs(
    work_dir: Path,
    logs_dir: Path,
    *,
    stage_commands: tuple[tuple[str, str], ...] = PIPELINE_STAGE_COMMANDS,
    run_consistency_check: bool = False,
) -> dict[str, Any]:
    """Execute pipeline stages in order and capture isolated logs."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    stage_results: list[dict[str, Any]] = []
    encountered_failure = False

    for stage_id, command in stage_commands:
        if encountered_failure:
            stage_results.append(
                {
                    "stage_id": stage_id,
                    "command": command,
                    "status": "skipped",
                    "returncode": None,
                    "log_path": str(logs_dir / f"{stage_id}.log"),
                }
            )
            continue

        result = _run_stage_command(
            command,
            cwd=work_dir,
            log_path=logs_dir / f"{stage_id}.log",
        )
        result["stage_id"] = stage_id
        result["status"] = "ok" if result["returncode"] == 0 else "failed"
        stage_results.append(result)
        if result["returncode"] != 0:
            encountered_failure = True

    if run_consistency_check and not encountered_failure:
        result = _run_stage_command(
            "make notebook-pipeline",
            cwd=work_dir,
            log_path=logs_dir / "05_consistency.log",
        )
        result["stage_id"] = "05_consistency"
        result["status"] = "ok" if result["returncode"] == 0 else "failed"
        stage_results.append(result)
        if result["returncode"] != 0:
            encountered_failure = True

    ended = datetime.now(timezone.utc)
    return {
        "started_utc": started.isoformat(),
        "ended_utc": ended.isoformat(),
        "duration_s": float((ended - started).total_seconds()),
        "success": not encountered_failure,
        "stages": stage_results,
    }


def _parse_run_start_timestamp(run_started_at_utc: str) -> float:
    return datetime.fromisoformat(run_started_at_utc).timestamp()


def _artifact_status(path: Path, run_start_ts: float) -> dict[str, Any]:
    exists = path.exists()
    if not exists:
        return {"path": str(path), "exists": False, "fresh": False, "size_bytes": 0}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "fresh": stat.st_mtime >= run_start_ts,
        "size_bytes": int(stat.st_size),
        "mtime_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }


def _find_first_other_pco2_pct_eq_160(qa_summary: dict[str, Any]) -> float | None:
    records = qa_summary.get("first_other_pco2_audit")
    if not isinstance(records, list):
        return None
    for record in records:
        if str(record.get("source", "")).upper() == "POC":
            value = record.get("pct_eq_160")
            if value is None:
                return None
            return float(value)
    return None


def _check_numeric_infinity(df: pd.DataFrame, frame_name: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    numeric = df.select_dtypes(include=[np.number])
    if numeric.empty:
        return findings
    array = numeric.to_numpy(dtype="float64", copy=True)
    inf_mask = np.isinf(array)
    inf_count = int(inf_mask.sum())
    if inf_count > 0:
        findings.append(
            {
                "severity": "P0",
                "category": "numeric",
                "code": "infinite_values_detected",
                "message": f"{frame_name} contains {inf_count} infinite values in numeric columns.",
            }
        )
    return findings


def load_and_validate_artifacts(
    work_dir: Path,
    *,
    run_started_at_utc: str,
) -> dict[str, Any]:
    """Validate output artifacts, schema contracts, and hard-fail checks."""
    run_start_ts = _parse_run_start_timestamp(run_started_at_utc)
    findings: list[dict[str, Any]] = []

    data_dir = work_dir / "MIMIC tabular data"
    cohort_path = data_dir / CANONICAL_COHORT_FILENAME
    classifier_path = data_dir / CANONICAL_NLP_FILENAME
    qa_path = work_dir / "qa_summary.json"
    rater_dir = work_dir / "annotation_agreement_outputs_nlp"
    rater_join_path = rater_dir / "R3_vs_NLP_join_audit.json"
    rater_summary_path = rater_dir / "R3_vs_NLP_summary.txt"
    analysis_paths = [work_dir / name for name in ANALYSIS_EXPORT_FILENAMES]

    artifact_paths = {
        "cohort_workbook": cohort_path,
        "classifier_workbook": classifier_path,
        "qa_summary": qa_path,
        "rater_join_audit": rater_join_path,
        "rater_summary": rater_summary_path,
        **{f"analysis_export_{index + 1}": path for index, path in enumerate(analysis_paths)},
    }
    artifact_checks = {
        name: _artifact_status(path, run_start_ts)
        for name, path in artifact_paths.items()
    }

    for name, status in artifact_checks.items():
        if not status["exists"]:
            findings.append(
                {
                    "severity": "P0",
                    "category": "artifact",
                    "code": "missing_artifact",
                    "message": f"Missing required artifact: {name}",
                }
            )
            continue
        if not status["fresh"]:
            findings.append(
                {
                    "severity": "P1",
                    "category": "artifact",
                    "code": "stale_artifact",
                    "message": f"Artifact was not refreshed in this run: {name}",
                }
            )
        if status["size_bytes"] <= 0:
            findings.append(
                {
                    "severity": "P1",
                    "category": "artifact",
                    "code": "empty_artifact",
                    "message": f"Artifact is empty: {name}",
                }
            )

    qa_summary: dict[str, Any] | None = None
    rater_join_audit: dict[str, Any] | None = None
    cohort_df = pd.DataFrame()
    classifier_df = pd.DataFrame()

    if qa_path.exists():
        try:
            qa_summary = _read_json(qa_path)
        except json.JSONDecodeError:
            findings.append(
                {
                    "severity": "P0",
                    "category": "artifact",
                    "code": "invalid_qa_summary_json",
                    "message": "qa_summary.json is not valid JSON.",
                }
            )

    if rater_join_path.exists():
        try:
            rater_join_audit = _read_json(rater_join_path)
        except json.JSONDecodeError:
            findings.append(
                {
                    "severity": "P0",
                    "category": "artifact",
                    "code": "invalid_rater_join_json",
                    "message": "R3_vs_NLP_join_audit.json is not valid JSON.",
                }
            )

    if cohort_path.exists():
        try:
            cohort_df = pd.read_excel(cohort_path, sheet_name=0, engine="openpyxl")
        except Exception:
            findings.append(
                {
                    "severity": "P0",
                    "category": "artifact",
                    "code": "cohort_read_failure",
                    "message": "Unable to read canonical cohort workbook.",
                }
            )
    if classifier_path.exists():
        try:
            classifier_df = pd.read_excel(classifier_path, sheet_name=0, engine="openpyxl")
        except Exception:
            findings.append(
                {
                    "severity": "P0",
                    "category": "artifact",
                    "code": "classifier_read_failure",
                    "message": "Unable to read canonical NLP workbook.",
                }
            )

    if not cohort_df.empty:
        required_cohort_columns = {
            "hadm_id",
            "subject_id",
            "ed_stay_id",
            "anthro_timing_tier",
            "anthro_days_offset",
            "anthro_chartdate",
            "anthro_timing_uncertain",
        }
        missing = sorted(required_cohort_columns.difference(cohort_df.columns))
        if missing:
            findings.append(
                {
                    "severity": "P0",
                    "category": "schema",
                    "code": "cohort_missing_columns",
                    "message": f"Cohort workbook missing columns: {missing}",
                }
            )
        findings.extend(_check_numeric_infinity(cohort_df, "cohort workbook"))

    if not classifier_df.empty:
        required_classifier_columns = {
            "hadm_id",
            "subject_id",
            "RFV1",
            "RFV2",
            "RFV3",
            "RFV4",
            "RFV5",
        }
        missing = sorted(required_classifier_columns.difference(classifier_df.columns))
        if missing:
            findings.append(
                {
                    "severity": "P0",
                    "category": "schema",
                    "code": "classifier_missing_columns",
                    "message": f"Classifier workbook missing columns: {missing}",
                }
            )
        findings.extend(_check_numeric_infinity(classifier_df, "classifier workbook"))

    if qa_summary:
        gas_source_audit = qa_summary.get("gas_source_audit", {})
        if bool(gas_source_audit.get("all_other_or_unknown", False)):
            findings.append(
                {
                    "severity": "P0",
                    "category": "quality",
                    "code": "all_other_or_unknown_gas_source",
                    "message": "Gas source attribution collapsed to all other/unknown.",
                }
            )

        paco2_checks = (
            qa_summary.get("uom_expectation_checks", {})
            .get("paco2_uom_checks", {})
        )
        for uom_field, check in paco2_checks.items():
            if check.get("present") and not bool(check.get("passes", False)):
                findings.append(
                    {
                        "severity": "P0",
                        "category": "quality",
                        "code": "uom_paco2_failure",
                        "message": f"pCO2 UOM validation failed for {uom_field}.",
                    }
                )

        omr = qa_summary.get("omr_diagnostics", {})
        pre_rows = int(omr.get("pre_window_candidate_rows", 0) or 0)
        post_rows = int(omr.get("post_window_candidate_rows", 0) or 0)
        attached = omr.get("attached_non_null_counts", {})
        attached_total = int(sum(int(value or 0) for value in attached.values()))
        if pre_rows + post_rows > 0 and attached_total == 0:
            findings.append(
                {
                    "severity": "P0",
                    "category": "quality",
                    "code": "omr_attach_inconsistency",
                    "message": "OMR had in-window candidates but attached anthropometrics are all null.",
                }
            )

        vitals_audit = qa_summary.get("vitals_outlier_audit")
        if isinstance(vitals_audit, list):
            for row in vitals_audit:
                outlier_pct = row.get("out_of_range_pct")
                if outlier_pct is None:
                    findings.append(
                        {
                            "severity": "P1",
                            "category": "quality",
                            "code": "missing_vitals_outlier_pct",
                            "message": "Vitals outlier audit row missing out_of_range_pct.",
                        }
                    )
                    continue
                value = float(outlier_pct)
                if not math.isfinite(value) or value < 0:
                    findings.append(
                        {
                            "severity": "P1",
                            "category": "quality",
                            "code": "invalid_vitals_outlier_pct",
                            "message": "Vitals outlier audit has invalid out_of_range_pct.",
                        }
                    )
        else:
            findings.append(
                {
                    "severity": "P1",
                    "category": "quality",
                    "code": "missing_vitals_outlier_audit",
                    "message": "qa_summary.json missing vitals_outlier_audit records.",
                }
            )

        if _find_first_other_pco2_pct_eq_160(qa_summary) is None:
            findings.append(
                {
                    "severity": "P1",
                    "category": "quality",
                    "code": "missing_first_other_pco2_audit",
                    "message": "qa_summary.json missing first_other_pco2 POC cap metric.",
                }
            )

    if rater_join_audit:
        if int(rater_join_audit.get("matched_rows", 0) or 0) == 0:
            findings.append(
                {
                    "severity": "P0",
                    "category": "quality",
                    "code": "rater_zero_matches",
                    "message": "Rater join audit reported zero matched rows.",
                }
            )

    current_metrics: dict[str, float] = {}
    if not cohort_df.empty:
        current_metrics["cohort_rows"] = float(len(cohort_df))
    if not classifier_df.empty:
        current_metrics["classifier_rows"] = float(len(classifier_df))
    if qa_summary:
        for metric in KEY_QA_METRICS:
            value = qa_summary.get(metric)
            if value is not None:
                current_metrics[metric] = float(value)
        first_other = _find_first_other_pco2_pct_eq_160(qa_summary)
        if first_other is not None:
            current_metrics["first_other_pco2_pct_eq_160_poc"] = float(first_other)

    return {
        "artifact_checks": artifact_checks,
        "qa_summary": qa_summary,
        "rater_join_audit": rater_join_audit,
        "current_metrics": current_metrics,
        "findings": findings,
    }


def _resolve_latest_prior_date(prior_runs_dir: Path) -> str | None:
    pattern = re.compile(r"^(\d{4}-\d{2}-\d{2}) MIMICIV all with CC\.xlsx$")
    dates: list[datetime] = []
    for file_path in prior_runs_dir.glob("* MIMICIV all with CC.xlsx"):
        match = pattern.match(file_path.name)
        if not match:
            continue
        dates.append(datetime.strptime(match.group(1), "%Y-%m-%d"))
    if not dates:
        return None
    return max(dates).strftime("%Y-%m-%d")


def resolve_baseline_metrics(
    work_dir: Path,
    baseline: str,
    *,
    pre_run_qa_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve baseline metrics from latest (or explicit) historical artifacts."""
    prior_runs_dir = work_dir / "MIMIC tabular data" / "prior runs"
    baseline_date = baseline
    if baseline == "latest":
        baseline_date = _resolve_latest_prior_date(prior_runs_dir) or "unavailable"

    metrics: dict[str, float] = {}
    sources: dict[str, str] = {}

    if baseline_date != "unavailable":
        cohort_path = prior_runs_dir / f"{baseline_date} {CANONICAL_COHORT_FILENAME}"
        if cohort_path.exists():
            baseline_cohort = pd.read_excel(cohort_path, sheet_name=0, engine="openpyxl")
            metrics["cohort_rows"] = float(len(baseline_cohort))
            sources["cohort_rows"] = str(cohort_path)

        nlp_path = prior_runs_dir / f"{baseline_date} {CANONICAL_NLP_FILENAME}"
        if nlp_path.exists():
            baseline_nlp = pd.read_excel(nlp_path, sheet_name=0, engine="openpyxl")
            metrics["classifier_rows"] = float(len(baseline_nlp))
            sources["classifier_rows"] = str(nlp_path)

        gas_source_path = prior_runs_dir / f"{baseline_date} gas_source_audit.json"
        if gas_source_path.exists():
            gas_audit = _read_json(gas_source_path)
            source_rate = gas_audit.get("source_rates", {}).get("other")
            if source_rate is not None:
                metrics["gas_source_other_rate"] = float(source_rate)
                sources["gas_source_other_rate"] = str(gas_source_path)

        first_other_path = prior_runs_dir / f"{baseline_date} first_other_pco2_audit.csv"
        if first_other_path.exists():
            baseline_first_other = pd.read_csv(first_other_path)
            poc_rows = baseline_first_other.loc[
                baseline_first_other["source"].astype(str).str.upper().eq("POC")
            ]
            if not poc_rows.empty and "pct_eq_160" in poc_rows.columns:
                metrics["first_other_pco2_pct_eq_160_poc"] = float(
                    poc_rows["pct_eq_160"].iloc[0]
                )
                sources["first_other_pco2_pct_eq_160_poc"] = str(first_other_path)

    if pre_run_qa_summary:
        for metric in KEY_QA_METRICS:
            value = pre_run_qa_summary.get(metric)
            if value is not None and metric not in metrics:
                metrics[metric] = float(value)
                sources[metric] = "pre_run_qa_summary.json"
        first_other = _find_first_other_pco2_pct_eq_160(pre_run_qa_summary)
        if (
            first_other is not None
            and "first_other_pco2_pct_eq_160_poc" not in metrics
        ):
            metrics["first_other_pco2_pct_eq_160_poc"] = float(first_other)
            sources["first_other_pco2_pct_eq_160_poc"] = "pre_run_qa_summary.json"

    return {
        "baseline_mode": baseline,
        "baseline_date": baseline_date,
        "metrics": metrics,
        "sources": sources,
    }


def compute_metric_drift(
    current_metrics: dict[str, float],
    baseline_metrics: dict[str, float],
) -> pd.DataFrame:
    """Compute warn/fail drift levels for configured metrics."""
    rows: list[dict[str, Any]] = []
    for metric, current_value in current_metrics.items():
        if metric not in baseline_metrics:
            continue
        if metric not in DRIFT_RULES:
            continue
        baseline_value = baseline_metrics[metric]
        rule = DRIFT_RULES[metric]
        delta = float(current_value - baseline_value)
        abs_delta = float(abs(delta))
        relative_delta = (
            float(abs_delta / abs(baseline_value))
            if baseline_value not in (0, 0.0)
            else None
        )
        comparator = relative_delta if rule.compare == "relative" else abs_delta
        if comparator is None:
            severity = "warning"
        elif comparator > rule.fail:
            severity = "fail"
        elif comparator > rule.warn:
            severity = "warning"
        else:
            severity = "ok"

        rows.append(
            {
                "metric": metric,
                "current_value": float(current_value),
                "baseline_value": float(baseline_value),
                "delta": delta,
                "abs_delta": abs_delta,
                "relative_delta": relative_delta,
                "compare_type": rule.compare,
                "warn_threshold": rule.warn,
                "fail_threshold": rule.fail,
                "severity": severity,
            }
        )
    return pd.DataFrame(rows).sort_values("metric").reset_index(drop=True)


def scan_logs_for_findings(
    stage_results: list[dict[str, Any]],
    *,
    allowlist_patterns: Iterable[str] = DEFAULT_ALLOWLIST_PATTERNS,
) -> pd.DataFrame:
    """Scan stage logs for known reliability/numerical-warning signatures."""
    compiled_allowlist = [re.compile(pattern, re.IGNORECASE) for pattern in allowlist_patterns]
    compiled_patterns = [
        (severity, re.compile(pattern, re.IGNORECASE), pattern_name)
        for severity, pattern, pattern_name in LOG_PATTERNS
    ]
    findings: list[dict[str, Any]] = []

    for stage in stage_results:
        log_path = Path(str(stage.get("log_path", "")))
        stage_id = str(stage.get("stage_id", "unknown"))
        if not log_path.exists():
            continue
        with log_path.open() as handle:
            for line_number, line in enumerate(handle, start=1):
                if any(pattern.search(line) for pattern in compiled_allowlist):
                    continue
                for severity, regex, pattern_name in compiled_patterns:
                    if regex.search(line):
                        findings.append(
                            {
                                "stage_id": stage_id,
                                "log_path": str(log_path),
                                "line_number": line_number,
                                "severity": severity,
                                "pattern": pattern_name,
                                "message": line.rstrip(),
                            }
                        )
                        break
    return pd.DataFrame(findings)


def build_audit_report(
    *,
    run_id: str,
    manifest: dict[str, Any],
    pipeline_run: dict[str, Any],
    preflight_findings: list[dict[str, Any]],
    artifact_result: dict[str, Any],
    drift_df: pd.DataFrame,
    log_findings_df: pd.DataFrame,
    strictness: str,
    baseline_info: dict[str, Any],
) -> dict[str, Any]:
    """Combine stage, artifact, drift, and log checks into final audit status."""
    findings: list[dict[str, Any]] = []
    findings.extend(preflight_findings)
    findings.extend(artifact_result["findings"])

    for stage in pipeline_run["stages"]:
        returncode = stage.get("returncode")
        if returncode is None:
            continue
        if int(returncode) != 0:
            findings.append(
                {
                    "severity": "P0",
                    "category": "pipeline",
                    "code": "stage_failed",
                    "message": f"{stage['stage_id']} failed with return code {returncode}.",
                }
            )

    if not drift_df.empty:
        for row in drift_df.to_dict(orient="records"):
            if row["severity"] == "fail":
                findings.append(
                    {
                        "severity": "P1",
                        "category": "drift",
                        "code": "metric_drift_fail",
                        "message": (
                            f"{row['metric']} drift exceeded fail threshold "
                            f"({row['abs_delta']:.4f})."
                        ),
                    }
                )
            elif row["severity"] == "warning":
                findings.append(
                    {
                        "severity": "P2",
                        "category": "drift",
                        "code": "metric_drift_warning",
                        "message": (
                            f"{row['metric']} drift exceeded warning threshold "
                            f"({row['abs_delta']:.4f})."
                        ),
                    }
                )

    if not log_findings_df.empty:
        findings.extend(log_findings_df.to_dict(orient="records"))

    p0_count = sum(1 for finding in findings if finding.get("severity") == "P0")
    p1_count = sum(1 for finding in findings if finding.get("severity") == "P1")
    p2_count = sum(1 for finding in findings if finding.get("severity") == "P2")
    if strictness == "fail_on_key_anomalies" and (p0_count > 0 or p1_count > 0):
        status = "fail"
    elif p2_count > 0:
        status = "warning"
    else:
        status = "pass"

    return {
        "run_id": run_id,
        "generated_utc": _utc_now_iso(),
        "strictness": strictness,
        "status": status,
        "summary": {
            "p0_count": p0_count,
            "p1_count": p1_count,
            "p2_count": p2_count,
            "total_findings": len(findings),
        },
        "manifest": manifest,
        "baseline": baseline_info,
        "pipeline_run": pipeline_run,
        "artifact_checks": artifact_result["artifact_checks"],
        "current_metrics": artifact_result["current_metrics"],
        "metric_drift": drift_df.to_dict(orient="records"),
        "findings": findings,
    }


def write_audit_summary_markdown(report: dict[str, Any], path: Path) -> None:
    """Write concise human-readable markdown summary for the audit report."""
    summary = report["summary"]
    lines = [
        f"# Pipeline Audit Summary ({report['run_id']})",
        "",
        f"- Status: **{report['status']}**",
        f"- Generated UTC: `{report['generated_utc']}`",
        f"- Strictness: `{report['strictness']}`",
        f"- Findings: P0={summary['p0_count']}, P1={summary['p1_count']}, P2={summary['p2_count']}",
        "",
        "## Stage Health",
    ]

    for stage in report["pipeline_run"]["stages"]:
        stage_id = stage.get("stage_id", "unknown")
        status = stage.get("status", "unknown")
        duration = stage.get("duration_s")
        returncode = stage.get("returncode")
        lines.append(
            f"- `{stage_id}`: status={status}, returncode={returncode}, duration_s={duration}"
        )

    lines.append("")
    lines.append("## Drift Summary")
    if not report["metric_drift"]:
        lines.append("- No drift-comparable metrics were available.")
    else:
        for row in report["metric_drift"]:
            lines.append(
                "- "
                f"{row['metric']}: current={row['current_value']}, "
                f"baseline={row['baseline_value']}, severity={row['severity']}"
            )

    lines.append("")
    lines.append("## Findings")
    if not report["findings"]:
        lines.append("- None.")
    else:
        for finding in report["findings"]:
            severity = finding.get("severity", "NA")
            code = finding.get("code", finding.get("pattern", "finding"))
            message = finding.get("message", "")
            lines.append(f"- [{severity}] `{code}` {message}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
