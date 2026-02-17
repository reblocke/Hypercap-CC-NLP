#!/usr/bin/env python3
"""Compare current pipeline outputs to a captured Jupyter baseline."""

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

from hypercap_cc_nlp.pipeline_parity import (  # noqa: E402
    PARITY_ROOT,
    compare_current_to_baseline,
    resolve_baseline_dir,
    write_parity_report,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline",
        default="latest",
        help="Baseline snapshot id/path, or 'latest'.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run id for parity output directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    work_dir = Path.cwd().resolve()
    baseline_dir = resolve_baseline_dir(work_dir, args.baseline)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = work_dir / PARITY_ROOT / run_id

    report = compare_current_to_baseline(
        work_dir,
        baseline_dir=baseline_dir,
    )
    output_paths = write_parity_report(report, output_dir=output_dir)

    print(f"Parity status: {report['status']}")
    print(f"Baseline: {baseline_dir}")
    print(f"Report: {output_paths['parity_report_path']}")
    print(f"Summary: {output_paths['parity_summary_path']}")
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
