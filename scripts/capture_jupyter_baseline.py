#!/usr/bin/env python3
"""Capture current Jupyter pipeline artifacts as a parity baseline snapshot."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _bootstrap_src_path() -> None:
    root = Path(__file__).resolve().parents[1]
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_bootstrap_src_path()

from hypercap_cc_nlp.pipeline_parity import capture_jupyter_baseline  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-id",
        default=None,
        help="Optional explicit baseline identifier (defaults to UTC timestamp).",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Optional suffix label for baseline directory naming.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    work_dir = Path.cwd().resolve()
    result = capture_jupyter_baseline(
        work_dir,
        baseline_id=args.baseline_id,
        label=args.label,
    )
    print(f"Baseline snapshot: {result['baseline_dir']}")
    print(f"Manifest: {result['manifest_path']}")
    if result["missing_files"]:
        print(f"Missing files: {result['missing_files']}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
