#!/usr/bin/env python3
"""Audit repeated aggregate numeric claims against the accepted run ledger."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _bootstrap_src_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    return root


WORK_DIR = _bootstrap_src_path()

from hypercap_cc_nlp.numeric_claims import (  # noqa: E402
    NumericClaimsError,
    run_audit,
    write_reports,
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=Path("docs/NUMERIC_CLAIMS.yml"),
        help="JSON-compatible YAML claim ledger.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        help="Accepted Results directory; supplying it enables live aggregate-source checks.",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=Path("artifacts/qa"),
        help="Aggregate QA root used by live mode.",
    )
    parser.add_argument(
        "--require-sources",
        action="store_true",
        help="Fail when any canonical aggregate source is unavailable.",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        help="Optional ignored output directory for YAML and Markdown reports.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit nonzero when the audit reports a failing finding.",
    )
    return parser.parse_args()


def _resolve(path: Path | None) -> Path | None:
    if path is None:
        return None
    return path if path.is_absolute() else WORK_DIR / path


def main() -> int:
    args = arguments()
    try:
        report = run_audit(
            repo_root=WORK_DIR,
            ledger_path=_resolve(args.ledger),
            results_dir=_resolve(args.results_dir),
            artifacts_dir=_resolve(args.artifacts_dir),
            require_sources=args.require_sources,
        )
    except NumericClaimsError as exc:
        print(f"Numeric claims audit configuration error: {exc}", file=sys.stderr)
        return 2
    if args.report_dir is not None:
        yaml_path, markdown_path = write_reports(report, _resolve(args.report_dir))
        print(f"Reports: {yaml_path.relative_to(WORK_DIR)}, {markdown_path.relative_to(WORK_DIR)}")
    summary = report["summary"]
    print(
        "Numeric claims audit: "
        f"{summary['status']} ({summary['mode']}; "
        f"{summary['claims']} claims; {summary['failing_findings']} failing findings)"
    )
    return 1 if args.check and summary["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
