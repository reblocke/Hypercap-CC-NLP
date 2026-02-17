#!/usr/bin/env python
"""Verify required classifier resources before running NLP notebook stages."""

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

from hypercap_cc_nlp.classifier_quality import verify_classifier_resources


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=DEFAULT_WORK_DIR,
        help="Repository root path (defaults to WORK_DIR or cwd).",
    )
    parser.add_argument(
        "--appendix-relpath",
        default="Annotation/nhamcs_rvc_2022_appendixII_codes.csv",
    )
    parser.add_argument(
        "--summary-relpath",
        default="Annotation/nhamcs_rvc_2022_summary_by_top_level_17.csv",
    )
    parser.add_argument(
        "--manifest-relpath",
        default="Annotation/resource_manifest.json",
    )
    parser.add_argument(
        "--strict-hash",
        action="store_true",
        default=os.getenv("CLASSIFIER_STRICT_RESOURCE_HASH", "1").strip() == "1",
        help="Fail when manifest hash checks mismatch (default true).",
    )
    parser.add_argument(
        "--warn-only-hash",
        action="store_true",
        help="Ignore hash mismatches as warnings (overrides --strict-hash).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    strict_hash = args.strict_hash and not args.warn_only_hash
    report = verify_classifier_resources(
        args.work_dir,
        appendix_relpath=args.appendix_relpath,
        summary_relpath=args.summary_relpath,
        manifest_path=args.manifest_relpath,
        strict_hash=strict_hash,
    )
    print(json.dumps(report, indent=2))
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
