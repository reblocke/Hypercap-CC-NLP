#!/usr/bin/env python3
"""Move fresh direct-render outputs from the repo root into Results/<date>/."""

from __future__ import annotations

import os
import shutil
import time
from datetime import datetime
from pathlib import Path


KNOWN_RENDER_OUTPUTS: tuple[str, ...] = (
    "MIMICIV_hypercap_EXT_cohort.pdf",
    "Hypercap CC NLP Classifier.pdf",
    "Rater Agreement Analysis.pdf",
    "Hypercap CC NLP Analysis.pdf",
    "Chart Review Sample Calc.html",
    "Chart Review Sample Calc_files",
)
FRESHNESS_WINDOW_SECS = 15 * 60


def resolve_results_dir(work_dir: Path) -> Path:
    """Return flat results directory based on env override or local date."""
    results_date = os.getenv("RESULTS_DATE", "").strip() or datetime.now().strftime("%Y-%m-%d")
    return work_dir / "Results" / results_date


def _is_fresh(path: Path, *, now_ts: float) -> bool:
    return (now_ts - path.stat().st_mtime) <= FRESHNESS_WINDOW_SECS


def move_fresh_render_outputs(work_dir: Path) -> list[tuple[Path, Path]]:
    """Move recent known root render outputs into Results/<date>/."""
    now_ts = time.time()
    results_dir = resolve_results_dir(work_dir)
    moves: list[tuple[Path, Path]] = []

    for relative_name in KNOWN_RENDER_OUTPUTS:
        source = work_dir / relative_name
        if not source.exists() or not _is_fresh(source, now_ts=now_ts):
            continue
        destination = results_dir / source.name
        moves.append((source, destination))

    if not moves:
        return []

    results_dir.mkdir(parents=True, exist_ok=True)
    for source, destination in moves:
        if destination.exists():
            if destination.is_dir():
                shutil.rmtree(destination)
            else:
                destination.unlink()
        shutil.move(str(source), str(destination))

    return moves


def main() -> int:
    work_dir = Path.cwd().resolve()
    moves = move_fresh_render_outputs(work_dir)
    for source, destination in moves:
        print(f"Moved {source.relative_to(work_dir)} -> {destination.relative_to(work_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
