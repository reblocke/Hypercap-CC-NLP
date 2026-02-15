#!/usr/bin/env python3
"""Run end-to-end pipeline audit with reproducibility and drift checks."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def _bootstrap_src_path() -> None:
    root = Path(__file__).resolve().parents[1]
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_src_path()

from hypercap_cc_nlp.pipeline_audit import (  # noqa: E402
    build_audit_report,
    collect_run_manifest,
    compute_metric_drift,
    load_and_validate_artifacts,
    resolve_baseline_metrics,
    run_pipeline_with_logs,
    run_preflight_checks,
    scan_logs_for_findings,
    write_audit_summary_markdown,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline",
        default="latest",
        help="Baseline for drift comparison: latest or YYYY-MM-DD.",
    )
    parser.add_argument(
        "--strictness",
        default="fail_on_key_anomalies",
        choices=("fail_on_key_anomalies",),
        help="Audit strictness policy.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier (defaults to UTC timestamp).",
    )
    parser.add_argument(
        "--consistency-check",
        action="store_true",
        help="Also run make notebook-pipeline after stage-isolated execution.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    work_dir = Path.cwd().resolve()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    audit_dir = work_dir / "debug" / "pipeline_audit" / run_id
    logs_dir = audit_dir / "logs"
    audit_dir.mkdir(parents=True, exist_ok=True)

    pre_run_qa_summary: dict | None = None
    qa_path = work_dir / "qa_summary.json"
    if qa_path.exists():
        try:
            pre_run_qa_summary = json.loads(qa_path.read_text())
        except json.JSONDecodeError:
            pre_run_qa_summary = None

    manifest = collect_run_manifest(work_dir, run_id)
    preflight_findings = run_preflight_checks(work_dir)
    pipeline_run = run_pipeline_with_logs(
        work_dir,
        logs_dir,
        run_consistency_check=args.consistency_check,
    )
    artifact_result = load_and_validate_artifacts(
        work_dir,
        run_started_at_utc=pipeline_run["started_utc"],
    )
    baseline_info = resolve_baseline_metrics(
        work_dir,
        args.baseline,
        pre_run_qa_summary=pre_run_qa_summary,
    )
    drift_df = compute_metric_drift(
        artifact_result["current_metrics"],
        baseline_info["metrics"],
    )
    log_findings_df = scan_logs_for_findings(pipeline_run["stages"])
    report = build_audit_report(
        run_id=run_id,
        manifest=manifest,
        pipeline_run=pipeline_run,
        preflight_findings=preflight_findings,
        artifact_result=artifact_result,
        drift_df=drift_df,
        log_findings_df=log_findings_df,
        strictness=args.strictness,
        baseline_info=baseline_info,
    )

    (audit_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2))
    (audit_dir / "audit_report.json").write_text(json.dumps(report, indent=2))
    drift_df.to_csv(audit_dir / "metric_drift.csv", index=False)
    log_findings_df.to_csv(audit_dir / "warnings_index.csv", index=False)
    write_audit_summary_markdown(report, audit_dir / "audit_summary.md")

    print(f"Pipeline audit status: {report['status']}")
    print(f"Audit directory: {audit_dir}")
    print(f"Findings summary: {report['summary']}")
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
