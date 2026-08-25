# AGENTS

## Project Purpose

This repository contains the sanitized Quarto/Python code release for a
MIMIC-IV study of chief complaint profiles in hypercapnic respiratory failure. It
supports the current medRxiv manuscript preprint and the ATS 2026 abstract, not a
final accepted or published journal article.

## Public And Data-Safety Rules

- Treat this repository as public. Do not add row-level MIMIC data,
  patient or encounter identifiers, annotation workbooks, generated results,
  credentials, private reviewer material, or publisher text.
- Keep `MIMIC tabular data/`, `Results/`, `artifacts/`, `debug/`, `Drafts/`,
  and local `.env` files ignored and untracked.
- Cite and link DOI, PhysioNet, MIMIC, and journal records rather than mirroring
  copyrighted or restricted source material.
- Keep manuscript status conservative: medRxiv preprint posted and ATS abstract
  published; no final journal article has been accepted or published.

## Orientation

- Start with `README.md` and `llms.txt` for scope, citation, run order, and data
  restrictions.
- `docs/SPEC.md` is the pipeline contract; `docs/DATA_ACCESS.md` is the
  restricted-data statement; `docs/MANUSCRIPT_MAPPING.md` maps manuscript assets
  to notebook stages.
- `data_dictionary.md` and `data_dictionary.csv` describe source, derived, and
  output fields without exposing row-level data.
- The canonical stages remain the four Quarto notebooks in the repository root.

Repository docs authority split:

- `README.md` = onboarding/runbook
- `docs/SPEC.md` = current normative contract
- `docs/DECISIONS.md` = dated rationale/history

Renderable pipeline notebooks must run without runtime imports from `src/` and
must remain self-contained at runtime. Keep execution-critical helpers in the
owning `.qmd` notebook unless `docs/SPEC.md` is deliberately changed.

## Workflow

Use the repository root:

```bash
uv sync --frozen
uv run pytest -q
uv run --with ruff ruff check src tests
make -n quarto-pipeline RESULTS_DATE=2026-05-30
```

Full rendering requires authorized MIMIC/BigQuery access and local private handoff
workbooks. Without those inputs, missing-data failures are expected and should be
documented rather than bypassed.

## Verification Before Publishing

- Validate `CITATION.cff` after citation edits.
- Run tests, Ruff, `quarto check`, and `git diff --check`.
- Search for stale "metadata pending" wording once abstract metadata is known.
- Confirm no tracked MIMIC workbooks, annotation workbooks, generated results,
  debug artifacts, manuscripts, PDFs, DOCX/PPTX, `.env`, or `CONTINUITY.md`.
- Verify release ZIP checksums using portable filenames, not local private paths.
