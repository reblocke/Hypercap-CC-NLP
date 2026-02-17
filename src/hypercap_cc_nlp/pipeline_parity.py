"""Baseline snapshot and parity checks for notebook/Quarto pipeline outputs."""

from __future__ import annotations

import json
import math
import re
import shutil
from hashlib import sha256
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .pipeline_audit import ANALYSIS_EXPORT_FILENAMES, KEY_QA_METRICS
from .workflow_contracts import CANONICAL_COHORT_FILENAME, CANONICAL_NLP_FILENAME

BASELINE_ROOT = Path("artifacts") / "baselines" / "jupyter"
PARITY_ROOT = Path("debug") / "pipeline_parity"

REQUIRED_RELATIVE_ARTIFACTS: tuple[Path, ...] = (
    Path("MIMIC tabular data") / CANONICAL_COHORT_FILENAME,
    Path("MIMIC tabular data") / CANONICAL_NLP_FILENAME,
    Path("qa_summary.json"),
    Path("annotation_agreement_outputs_nlp") / "R3_vs_NLP_join_audit.json",
    Path("annotation_agreement_outputs_nlp") / "R3_vs_NLP_summary.txt",
    *(Path(name) for name in ANALYSIS_EXPORT_FILENAMES),
)


@dataclass(frozen=True)
class MetricThreshold:
    """Absolute delta thresholds for warning and failure."""

    warn: float
    fail: float


PARITY_THRESHOLDS: dict[str, MetricThreshold] = {
    "icu_link_rate": MetricThreshold(warn=0.04, fail=0.08),
    "pct_any_gas_0_6h": MetricThreshold(warn=0.04, fail=0.08),
    "pct_any_gas_0_24h": MetricThreshold(warn=0.04, fail=0.08),
    "gas_source_other_rate": MetricThreshold(warn=0.04, fail=0.08),
    "first_other_pco2_pct_eq_160_poc": MetricThreshold(warn=0.05, fail=0.10),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _safe_label(value: str | None) -> str:
    if not value:
        return ""
    normalized = re.sub(r"[^0-9A-Za-z._-]+", "-", value.strip()).strip("-")
    return f"_{normalized}" if normalized else ""


def _timestamp_id() -> str:
    return _utc_now().strftime("%Y%m%d_%H%M%S")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _latest_pipeline_audit_report(work_dir: Path) -> Path | None:
    audit_root = work_dir / "debug" / "pipeline_audit"
    if not audit_root.exists():
        return None
    candidates: list[Path] = []
    for run_dir in audit_root.iterdir():
        candidate = run_dir / "audit_report.json"
        if candidate.exists():
            candidates.append(candidate)
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _required_artifact_map(work_dir: Path) -> dict[str, Path]:
    return {str(path): work_dir / path for path in REQUIRED_RELATIVE_ARTIFACTS}


def _extract_metrics_from_qa(qa_summary: dict[str, Any]) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for metric in KEY_QA_METRICS:
        value = qa_summary.get(metric)
        if value is not None:
            metrics[metric] = float(value)
    first_other_records = qa_summary.get("first_other_pco2_audit")
    if isinstance(first_other_records, list):
        for record in first_other_records:
            if str(record.get("source", "")).upper() != "POC":
                continue
            value = record.get("pct_eq_160")
            if value is not None:
                metrics["first_other_pco2_pct_eq_160_poc"] = float(value)
                break
    return metrics


def _cohort_or_classifier_signature(path: Path) -> dict[str, Any]:
    frame = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    required_cols = {"hadm_id", "subject_id"}
    missing = sorted(required_cols.difference(frame.columns))
    if missing:
        raise KeyError(f"{path.name} missing required ID columns: {missing}")

    hadm_ids = pd.to_numeric(frame["hadm_id"], errors="coerce").dropna().astype("int64")
    subject_ids = pd.to_numeric(frame["subject_id"], errors="coerce").dropna().astype("int64")
    id_pairs = (
        frame.loc[:, ["hadm_id", "subject_id"]]
        .assign(
            hadm_id=lambda df: pd.to_numeric(df["hadm_id"], errors="coerce"),
            subject_id=lambda df: pd.to_numeric(df["subject_id"], errors="coerce"),
        )
        .dropna(subset=["hadm_id", "subject_id"])
        .astype({"hadm_id": "int64", "subject_id": "int64"})
        .drop_duplicates()
    )

    hadm_id_values = hadm_ids.drop_duplicates().sort_values().tolist()
    subject_id_values = subject_ids.drop_duplicates().sort_values().tolist()
    id_pair_values = sorted(
        zip(id_pairs["hadm_id"], id_pairs["subject_id"], strict=False)
    )

    return {
        "rows": int(len(frame)),
        "columns": sorted(frame.columns.astype(str).tolist()),
        "hadm_id_unique_count": int(hadm_ids.nunique()),
        "subject_id_unique_count": int(subject_ids.nunique()),
        "id_pair_unique_count": int(len(id_pairs)),
        "hadm_id_hash": sha256(
            "|".join(str(value) for value in hadm_id_values).encode("utf-8")
        ).hexdigest(),
        "subject_id_hash": sha256(
            "|".join(str(value) for value in subject_id_values).encode("utf-8")
        ).hexdigest(),
        "id_pair_hash": sha256(
            "|".join(f"{left}:{right}" for left, right in id_pair_values).encode("utf-8")
        ).hexdigest(),
    }


def _analysis_file_signature(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".xlsx":
        workbook = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        sheet_signatures: dict[str, dict[str, Any]] = {}
        for sheet_name, frame in workbook.items():
            numeric_sum = 0.0
            if not frame.empty:
                numeric_frame = frame.select_dtypes(include="number")
                if not numeric_frame.empty:
                    numeric_sum = float(numeric_frame.fillna(0).to_numpy().sum())
            sheet_signatures[sheet_name] = {
                "rows": int(frame.shape[0]),
                "cols": int(frame.shape[1]),
                "numeric_sum": numeric_sum,
            }
        return {"kind": "xlsx", "sheets": sheet_signatures}

    payload = path.read_bytes()
    return {
        "kind": "binary",
        "size_bytes": int(path.stat().st_size),
        "sha256": sha256(payload).hexdigest(),
    }


def _build_snapshot(work_dir: Path) -> dict[str, Any]:
    artifact_map = _required_artifact_map(work_dir)
    missing = [relative for relative, path in artifact_map.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required artifacts: {missing}")

    qa_summary = _read_json(work_dir / "qa_summary.json")
    cohort_signature = _cohort_or_classifier_signature(
        work_dir / "MIMIC tabular data" / CANONICAL_COHORT_FILENAME
    )
    classifier_signature = _cohort_or_classifier_signature(
        work_dir / "MIMIC tabular data" / CANONICAL_NLP_FILENAME
    )

    analysis_signatures = {
        name: _analysis_file_signature(work_dir / name)
        for name in ANALYSIS_EXPORT_FILENAMES
    }
    rater_join_audit = _read_json(
        work_dir / "annotation_agreement_outputs_nlp" / "R3_vs_NLP_join_audit.json"
    )

    return {
        "artifacts": sorted(artifact_map.keys()),
        "metrics": _extract_metrics_from_qa(qa_summary),
        "cohort_signature": cohort_signature,
        "classifier_signature": classifier_signature,
        "analysis_signatures": analysis_signatures,
        "rater_join_audit": rater_join_audit,
        "qa_summary_excerpt": {
            key: qa_summary.get(key)
            for key in (
                *KEY_QA_METRICS,
                "gas_source_audit",
                "uom_expectation_checks",
                "omr_diagnostics",
                "first_other_pco2_audit",
            )
        },
    }


def capture_jupyter_baseline(
    work_dir: Path,
    *,
    baseline_id: str | None = None,
    label: str | None = None,
) -> dict[str, Any]:
    """Copy current Jupyter artifacts into a baseline snapshot directory."""
    baseline_name = baseline_id or _timestamp_id()
    baseline_dir = work_dir / BASELINE_ROOT / f"{baseline_name}{_safe_label(label)}"
    baseline_dir.mkdir(parents=True, exist_ok=True)

    copied_files: list[str] = []
    missing_files: list[str] = []

    for relative, source_path in _required_artifact_map(work_dir).items():
        target_path = baseline_dir / relative
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not source_path.exists():
            missing_files.append(relative)
            continue
        shutil.copy2(source_path, target_path)
        copied_files.append(relative)

    latest_audit = _latest_pipeline_audit_report(work_dir)
    if latest_audit:
        audit_target = baseline_dir / "debug" / "pipeline_audit" / "latest_audit_report.json"
        audit_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(latest_audit, audit_target)
        copied_files.append("debug/pipeline_audit/latest_audit_report.json")

    snapshot = _build_snapshot(work_dir)
    snapshot["copied_files"] = copied_files
    snapshot["missing_files"] = missing_files
    snapshot["captured_utc"] = _utc_now_iso()
    snapshot["baseline_id"] = baseline_name
    snapshot["label"] = label

    _write_json(baseline_dir / "baseline_manifest.json", snapshot)
    return {
        "baseline_id": baseline_name,
        "baseline_dir": str(baseline_dir),
        "copied_files": copied_files,
        "missing_files": missing_files,
        "manifest_path": str(baseline_dir / "baseline_manifest.json"),
    }


def resolve_baseline_dir(work_dir: Path, baseline: str) -> Path:
    """Resolve an explicit baseline id/path or latest baseline snapshot."""
    root = work_dir / BASELINE_ROOT
    if baseline == "latest":
        if not root.exists():
            raise FileNotFoundError("No baseline directory exists under artifacts/baselines/jupyter.")
        candidates = [path for path in root.iterdir() if path.is_dir()]
        if not candidates:
            raise FileNotFoundError("No baseline snapshots found under artifacts/baselines/jupyter.")
        return max(candidates, key=lambda path: path.stat().st_mtime)

    explicit = Path(baseline)
    if explicit.is_absolute():
        if not explicit.exists():
            raise FileNotFoundError(f"Baseline path does not exist: {explicit}")
        return explicit

    candidate = root / baseline
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"Baseline snapshot not found: {candidate}")


def _metric_delta_findings(
    baseline_metrics: dict[str, float],
    current_metrics: dict[str, float],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    findings: list[dict[str, Any]] = []
    drift_rows: list[dict[str, Any]] = []
    for metric, threshold in PARITY_THRESHOLDS.items():
        if metric not in baseline_metrics or metric not in current_metrics:
            continue
        current = float(current_metrics[metric])
        baseline = float(baseline_metrics[metric])
        delta = current - baseline
        abs_delta = abs(delta)
        severity = "ok"
        if metric == "gas_source_other_rate" and delta < 0:
            drift_rows.append(
                {
                    "metric": metric,
                    "baseline_value": baseline,
                    "current_value": current,
                    "delta": delta,
                    "abs_delta": abs_delta,
                    "warn_threshold": threshold.warn,
                    "fail_threshold": threshold.fail,
                    "severity": "ok_improved",
                }
            )
            continue
        if abs_delta > threshold.fail:
            severity = "fail"
            findings.append(
                {
                    "severity": "P1",
                    "code": "metric_delta_fail",
                    "message": (
                        f"{metric} absolute delta {abs_delta:.6f} exceeded fail threshold "
                        f"{threshold.fail:.6f}."
                    ),
                }
            )
        elif abs_delta > threshold.warn:
            severity = "warning"
            findings.append(
                {
                    "severity": "P2",
                    "code": "metric_delta_warning",
                    "message": (
                        f"{metric} absolute delta {abs_delta:.6f} exceeded warning threshold "
                        f"{threshold.warn:.6f}."
                    ),
                }
            )
        drift_rows.append(
            {
                "metric": metric,
                "baseline_value": baseline,
                "current_value": current,
                "delta": delta,
                "abs_delta": abs_delta,
                "warn_threshold": threshold.warn,
                "fail_threshold": threshold.fail,
                "severity": severity,
            }
        )
    return findings, drift_rows


def _id_contract_findings(
    baseline_signature: dict[str, Any],
    current_signature: dict[str, Any],
    *,
    label: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if baseline_signature["rows"] != current_signature["rows"]:
        findings.append(
            {
                "severity": "P1",
                "code": "row_count_mismatch",
                "message": (
                    f"{label} row count mismatch: baseline={baseline_signature['rows']} "
                    f"current={current_signature['rows']}"
                ),
            }
        )

    for field in ("hadm_id_hash", "subject_id_hash", "id_pair_hash"):
        if baseline_signature[field] != current_signature[field]:
            findings.append(
                {
                    "severity": "P0",
                    "code": "id_contract_mismatch",
                    "message": f"{label} {field} differs between baseline and current run.",
                }
            )
    return findings


def _analysis_signature_findings(
    baseline_analysis: dict[str, Any],
    current_analysis: dict[str, Any],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for file_name in ANALYSIS_EXPORT_FILENAMES:
        baseline_present = file_name in baseline_analysis
        current_present = file_name in current_analysis
        if not baseline_present and current_present:
            findings.append(
                {
                    "severity": "P2",
                    "code": "analysis_export_added",
                    "message": (
                        f"Current run includes new analysis export not present in baseline: "
                        f"{file_name}."
                    ),
                }
            )
            continue
        if baseline_present and not current_present:
            findings.append(
                {
                    "severity": "P0",
                    "code": "analysis_export_missing",
                    "message": f"Missing analysis export signature for {file_name}.",
                }
            )
            continue
        if not baseline_present and not current_present:
            findings.append(
                {
                    "severity": "P0",
                    "code": "analysis_export_missing",
                    "message": (
                        f"Missing analysis export signature in both baseline and current run "
                        f"for {file_name}."
                    ),
                }
            )
            continue
        baseline_signature = baseline_analysis[file_name]
        current_signature = current_analysis[file_name]
        baseline_kind = baseline_signature.get("kind", "xlsx")
        current_kind = current_signature.get("kind", "xlsx")
        if baseline_kind != current_kind:
            findings.append(
                {
                    "severity": "P0",
                    "code": "analysis_kind_mismatch",
                    "message": (
                        f"Analysis export type mismatch for {file_name}: "
                        f"baseline={baseline_kind}, current={current_kind}."
                    ),
                }
            )
            continue

        if current_kind == "xlsx":
            base_sheets = baseline_signature["sheets"]
            curr_sheets = current_signature["sheets"]
            if set(base_sheets) != set(curr_sheets):
                findings.append(
                    {
                        "severity": "P0",
                        "code": "analysis_sheet_mismatch",
                        "message": f"Sheet names differ for {file_name}.",
                    }
                )
                continue
            for sheet_name in base_sheets:
                base_sheet = base_sheets[sheet_name]
                curr_sheet = curr_sheets[sheet_name]
                if (base_sheet["rows"], base_sheet["cols"]) != (
                    curr_sheet["rows"],
                    curr_sheet["cols"],
                ):
                    findings.append(
                        {
                            "severity": "P2",
                            "code": "analysis_shape_changed",
                            "message": (
                                f"{file_name}::{sheet_name} shape changed: "
                                f"baseline=({base_sheet['rows']}, {base_sheet['cols']}) "
                                f"current=({curr_sheet['rows']}, {curr_sheet['cols']})."
                            ),
                        }
                    )
                numeric_delta = abs(
                    float(curr_sheet["numeric_sum"]) - float(base_sheet["numeric_sum"])
                )
                if numeric_delta > 1e-6 and math.isfinite(numeric_delta):
                    findings.append(
                        {
                            "severity": "P2",
                            "code": "analysis_numeric_sum_drift",
                            "message": (
                                f"{file_name}::{sheet_name} numeric sum drift {numeric_delta:.6f}."
                            ),
                        }
                    )
        else:
            baseline_hash = baseline_signature.get("sha256")
            current_hash = current_signature.get("sha256")
            if baseline_hash != current_hash:
                findings.append(
                    {
                        "severity": "P2",
                        "code": "analysis_binary_hash_drift",
                        "message": f"{file_name} binary hash changed between baseline and current run.",
                    }
                )
    return findings


def compare_current_to_baseline(
    work_dir: Path,
    *,
    baseline_dir: Path,
) -> dict[str, Any]:
    """Compare current pipeline outputs against a captured baseline snapshot."""
    manifest_path = baseline_dir / "baseline_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Baseline manifest missing: {manifest_path}")

    baseline_manifest = _read_json(manifest_path)
    try:
        current_snapshot = _build_snapshot(work_dir)
    except FileNotFoundError as exc:
        return {
            "generated_utc": _utc_now_iso(),
            "status": "fail",
            "summary": {
                "p0_count": 1,
                "p1_count": 0,
                "p2_count": 0,
                "total_findings": 1,
            },
            "baseline_dir": str(baseline_dir),
            "baseline_manifest_path": str(manifest_path),
            "findings": [
                {
                    "severity": "P0",
                    "code": "missing_artifact",
                    "message": str(exc),
                }
            ],
            "metric_drift": [],
            "current_metrics": {},
        }

    findings: list[dict[str, Any]] = []
    findings.extend(
        _id_contract_findings(
            baseline_manifest["cohort_signature"],
            current_snapshot["cohort_signature"],
            label="cohort",
        )
    )
    findings.extend(
        _id_contract_findings(
            baseline_manifest["classifier_signature"],
            current_snapshot["classifier_signature"],
            label="classifier",
        )
    )
    findings.extend(
        _analysis_signature_findings(
            baseline_manifest["analysis_signatures"],
            current_snapshot["analysis_signatures"],
        )
    )

    baseline_rater = baseline_manifest.get("rater_join_audit", {})
    current_rater = current_snapshot.get("rater_join_audit", {})
    if int(current_rater.get("matched_rows", 0)) == 0:
        findings.append(
            {
                "severity": "P0",
                "code": "rater_zero_matches",
                "message": "Current rater join audit reports matched_rows == 0.",
            }
        )
    if baseline_rater.get("matched_rows") != current_rater.get("matched_rows"):
        findings.append(
            {
                "severity": "P2",
                "code": "rater_match_count_delta",
                "message": (
                    f"Rater matched_rows changed: baseline={baseline_rater.get('matched_rows')} "
                    f"current={current_rater.get('matched_rows')}."
                ),
            }
        )

    metric_findings, drift_rows = _metric_delta_findings(
        baseline_manifest.get("metrics", {}),
        current_snapshot.get("metrics", {}),
    )
    findings.extend(metric_findings)

    p0_count = sum(1 for finding in findings if finding["severity"] == "P0")
    p1_count = sum(1 for finding in findings if finding["severity"] == "P1")
    p2_count = sum(1 for finding in findings if finding["severity"] == "P2")
    if p0_count > 0 or p1_count > 0:
        status = "fail"
    elif p2_count > 0:
        status = "warning"
    else:
        status = "pass"

    return {
        "generated_utc": _utc_now_iso(),
        "status": status,
        "summary": {
            "p0_count": p0_count,
            "p1_count": p1_count,
            "p2_count": p2_count,
            "total_findings": len(findings),
        },
        "baseline_dir": str(baseline_dir),
        "baseline_manifest_path": str(manifest_path),
        "findings": findings,
        "metric_drift": drift_rows,
        "current_metrics": current_snapshot.get("metrics", {}),
    }


def write_parity_summary_markdown(report: dict[str, Any], output_path: Path) -> None:
    """Write concise parity summary markdown."""
    summary = report["summary"]
    lines = [
        "# Pipeline Parity Summary",
        "",
        f"- Status: **{report['status']}**",
        f"- Generated UTC: `{report['generated_utc']}`",
        f"- Baseline: `{report['baseline_dir']}`",
        (
            f"- Findings: P0={summary['p0_count']}, P1={summary['p1_count']}, "
            f"P2={summary['p2_count']}"
        ),
        "",
        "## Metric Drift",
    ]
    if not report["metric_drift"]:
        lines.append("- No comparable metric pairs were available.")
    else:
        for row in report["metric_drift"]:
            lines.append(
                "- "
                f"{row['metric']}: baseline={row['baseline_value']}, "
                f"current={row['current_value']}, severity={row['severity']}"
            )
    lines.append("")
    lines.append("## Findings")
    if not report["findings"]:
        lines.append("- None.")
    else:
        for finding in report["findings"]:
            lines.append(
                f"- [{finding['severity']}] `{finding['code']}` {finding['message']}"
            )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n")


def write_parity_report(
    report: dict[str, Any],
    *,
    output_dir: Path,
) -> dict[str, str]:
    """Persist parity report json + markdown summary."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "parity_report.json"
    summary_path = output_dir / "parity_summary.md"
    json_path.write_text(json.dumps(report, indent=2))
    write_parity_summary_markdown(report, summary_path)
    return {
        "parity_report_path": str(json_path),
        "parity_summary_path": str(summary_path),
    }
