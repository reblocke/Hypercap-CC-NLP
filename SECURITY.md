# Security

## Reporting

Do not open public issues or pull requests containing credentials, tokens, PHI/PII, MIMIC-derived row-level data, annotation workbooks, generated debug logs, or unpublished manuscript drafts.

Report security or data-exposure concerns privately to the repository maintainer.

## Supported Versions

Only the latest public release snapshot is supported for security and data-hygiene fixes.

## Data Hygiene Expectations

- `.env` and credential files are ignored and must remain local.
- `MIMIC tabular data/`, `Drafts/`, `Results/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, and `Legacy Code/` are ignored in public releases.
- Generated manuscript outputs should be distributed only as release assets after content and disclosure review.
