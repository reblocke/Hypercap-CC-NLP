#!/usr/bin/env python
"""Run pipeline contract checks on canonical cohort/classifier outputs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

DEFAULT_WORK_DIR = Path(os.getenv("WORK_DIR", Path.cwd())).expanduser().resolve()
SRC_DIR = DEFAULT_WORK_DIR / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from hypercap_cc_nlp.pipeline_contracts import (
    build_pipeline_contract_report_for_stages,
    write_contract_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=DEFAULT_WORK_DIR,
    )
    parser.add_argument(
        "--mode",
        choices=("fail", "warn"),
        default=os.getenv("PIPELINE_CONTRACT_MODE", "fail").strip().lower(),
        help="fail: non-zero exit on contract errors, warn: always exit zero.",
    )
    parser.add_argument(
        "--stage",
        choices=("all", "cohort", "classifier"),
        default="all",
        help="Which stage contracts to evaluate.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.stage == "all":
        stages = ("cohort", "classifier")
    else:
        stages = (args.stage,)
    report = build_pipeline_contract_report_for_stages(
        args.work_dir,
        stages=stages,
    )
    output_paths = write_contract_report(report, work_dir=args.work_dir)

    print(json.dumps(report, indent=2))
    print("Wrote:", output_paths["contract_report_path"])
    if output_paths["failed_contract_path"].exists():
        print("Wrote:", output_paths["failed_contract_path"])

    if report["status"] == "fail" and args.mode == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
