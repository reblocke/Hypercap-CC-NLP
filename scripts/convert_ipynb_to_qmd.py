#!/usr/bin/env python3
"""Convert selected Jupyter notebooks into Quarto qmd notebooks."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import nbformat

NOTEBOOK_MAP: dict[str, str] = {
    "MIMICIV_hypercap_EXT_cohort.ipynb": "MIMICIV_hypercap_EXT_cohort.qmd",
    "Hypercap CC NLP Classifier.ipynb": "Hypercap CC NLP Classifier.qmd",
    "Rater Agreement Analysis.ipynb": "Rater Agreement Analysis.qmd",
    "Hypercap CC NLP Analysis.ipynb": "Hypercap CC NLP Analysis.qmd",
}


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "-", value.lower()).strip("-")
    return normalized or "notebook"


def _header(title: str) -> str:
    return "\n".join(
        [
            "---",
            f'title: "{title}"',
            "jupyter: python3",
            "format:",
            "  pdf:",
            "    toc: true",
            "    number-sections: true",
            "    code-overflow: wrap",
            "execute:",
            "  echo: true",
            "  warning: false",
            "  message: false",
            "  error: false",
            "---",
            "",
        ]
    )


def convert_notebook(source_path: Path, target_path: Path) -> None:
    notebook = nbformat.read(source_path, as_version=4)
    title = source_path.stem
    stem_slug = _slugify(source_path.stem)
    parts: list[str] = [_header(title)]

    chunk_index = 0
    for cell in notebook.cells:
        cell_type = cell.get("cell_type", "")
        source = (cell.get("source", "") or "").rstrip()
        if cell_type == "markdown":
            if source:
                parts.append(source)
                parts.append("")
            continue
        if cell_type == "code":
            chunk_index += 1
            label = f"{stem_slug}-cell-{chunk_index:03d}"
            parts.extend(
                [
                    "```{python}",
                    f"#| label: {label}",
                    source,
                    "```",
                    "",
                ]
            )
            continue
        if source:
            parts.extend(
                [
                    "::: {.callout-note}",
                    f"Unsupported cell type `{cell_type}` was preserved as text.",
                    "",
                    source,
                    ":::",
                    "",
                ]
            )

    target_path.write_text("\n".join(parts).rstrip() + "\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path.cwd(),
        help="Repository root containing notebook files.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    work_dir = args.work_dir.resolve()

    for source_name, target_name in NOTEBOOK_MAP.items():
        source_path = work_dir / source_name
        target_path = work_dir / target_name
        if not source_path.exists():
            raise FileNotFoundError(f"Notebook not found: {source_path}")
        convert_notebook(source_path, target_path)
        print(f"Converted {source_path.name} -> {target_path.name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
