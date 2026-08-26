#!/usr/bin/env python3
"""Capture or compare the private semantic baseline for the IMV timing ticket."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


def _bootstrap_src_path() -> None:
    root = Path(__file__).resolve().parents[1]
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_src_path()

from hypercap_cc_nlp.imv_ticket_parity import (  # noqa: E402
    capture_imv_ticket_baseline,
    compare_imv_ticket_baseline,
    resolve_imv_capture_baseline_dir,
    write_imv_ticket_parity_report,
)
from hypercap_cc_nlp.pipeline_parity import resolve_baseline_dir  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("capture", "compare"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument(
            "--baseline",
            required=True,
            help=(
                "New explicit baseline ID or absolute path under artifacts/qa/baselines."
                if command == "capture"
                else "Existing captured baseline ID or path."
            ),
        )
        subparser.add_argument(
            "--results-date",
            required=True,
            help="Results folder date in YYYY-MM-DD format.",
        )
    subparsers.choices["capture"].add_argument(
        "--source-commit",
        default=None,
        help=(
            "Producer Git revision (defaults to HEAD); resolved and checked against "
            "clean, hash-linked cohort/classifier/analysis stage manifests."
        ),
    )
    subparsers.choices["compare"].add_argument(
        "--output",
        default=None,
        help="Optional aggregate-only JSON report path.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    work_dir = Path.cwd().resolve()
    if args.command == "capture":
        baseline_dir = resolve_imv_capture_baseline_dir(work_dir, args.baseline)
        result = capture_imv_ticket_baseline(
            work_dir,
            baseline_dir=baseline_dir,
            results_date=args.results_date,
            source_commit=args.source_commit,
        )
        print(f"Semantic baseline: {result['manifest_path']}")
        return 0

    baseline_dir = resolve_baseline_dir(work_dir, args.baseline)
    report = compare_imv_ticket_baseline(
        work_dir,
        baseline_dir=baseline_dir,
        results_date=args.results_date,
    )
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = (
        Path(args.output).expanduser()
        if args.output
        else work_dir
        / "debug"
        / "pipeline_parity"
        / "imv_ticket"
        / run_id
        / "semantic_report.json"
    )
    write_imv_ticket_parity_report(report, output_path)
    print(f"Semantic parity status: {report['status']}")
    print(f"Report: {output_path}")
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
