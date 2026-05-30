# Contributing

Contributions should improve reproducibility, portability, documentation clarity, or tests without introducing private data.

## Rules

- Do not submit PHI/PII, MIMIC-derived row-level data, annotation workbooks, generated results, draft manuscripts, credentials, or local environment files.
- Keep core analysis logic in the canonical `.qmd` notebooks unless a project maintainer explicitly approves a different architecture.
- Keep `src/hypercap_cc_nlp/` focused on QA, contracts, audits, parity checks, scripts, and tests.
- Use `uv` for dependency management and Ruff for linting/formatting.

## Local Checks

```bash
uv sync --frozen
uv run pytest -q
uv run --with ruff ruff check src tests
make -n quarto-pipeline
```

Data-dependent commands require credentialed MIMIC access and private input workbooks.

## Pull Requests

Before opening a PR:

- verify no restricted/generated files are tracked
- summarize what changed and why
- include the checks you ran
- note any commands that could not run because restricted data were unavailable
