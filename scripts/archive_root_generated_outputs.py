#!/usr/bin/env python3
"""Move non-input root-level generated artifacts into a dated archive folder."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import shutil


ROOT_GENERATED_FILENAMES = (
    "MIMICIV_hypercap_EXT_cohort.pdf",
    "Hypercap CC NLP Classifier.pdf",
    "Rater Agreement Analysis.pdf",
    "Hypercap CC NLP Analysis.pdf",
    "Ascertainment_Overlap_Intersections.xlsx",
    "Ascertainment_Overlap_UpSet.png",
    "ICD_Positive_Subset_Breakdown.xlsx",
    "ICD_vs_Gas_Performance.xlsx",
    "Symptom_Composition_Pivot_ChartReady.xlsx",
    "Symptom_Composition_by_ABG_VBG_Overlap.xlsx",
    "Symptom_Composition_by_Gas_Source_Overlap.xlsx",
    "Symptom_Composition_by_Hypercapnia_Definition.xlsx",
    "Symptom_Composition_by_ICD_Gas_Overlap.xlsx",
)


def archive_generated_outputs(work_dir: Path, execute: bool, stamp: str | None = None) -> list[tuple[Path, Path]]:
    """Return planned (or executed) moves from root to archive directory."""
    archive_stamp = stamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_dir = work_dir / "artifacts" / "archived_root_outputs" / archive_stamp
    planned_moves: list[tuple[Path, Path]] = []

    for filename in ROOT_GENERATED_FILENAMES:
        source = work_dir / filename
        if not source.exists():
            continue
        destination = archive_dir / filename
        planned_moves.append((source, destination))

    if execute and planned_moves:
        archive_dir.mkdir(parents=True, exist_ok=True)
        for source, destination in planned_moves:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(destination))

    return planned_moves


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path.cwd(),
        help="Repository root directory (defaults to current working directory).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform file moves. Without this flag, prints a dry-run plan only.",
    )
    parser.add_argument(
        "--stamp",
        type=str,
        default=None,
        help="Optional timestamp folder name override.",
    )
    args = parser.parse_args()

    moves = archive_generated_outputs(args.work_dir.resolve(), args.execute, args.stamp)
    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"[{mode}] planned moves: {len(moves)}")
    for source, destination in moves:
        print(f"{source} -> {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
