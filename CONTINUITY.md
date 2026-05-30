Goal (incl. success criteria):
- Implement the journal-submission repository cleanup plan for Hypercap-CC-NLP.
- Success criteria:
  - Current GitHub repo is contained/private before any public release work.
  - A private full backup of the pre-cleanup repository exists outside the public release surface.
  - Public release tree excludes restricted row-level data, drafts, generated outputs, debug logs, local metadata, Office/PDF/image binaries, and generated `.xlsx` products.
  - README and release docs follow the NRH-SCI-Vent style: citation, data access/ethics, reproducibility limits, expected outputs, and paper-code mapping.
  - Static tests enforce release hygiene and notebook self-containment.
  - Verification commands pass or failures are explicitly reported.

Constraints/Assumptions:
- Core pipeline logic remains in the four canonical `.qmd` notebooks.
- Journal target is journal-agnostic.
- Public generated manuscript outputs should be release/Zenodo assets, not tracked git files.
- Exact ATS abstract/proceedings citation metadata is UNCONFIRMED; `CITATION.cff` uses repository metadata and asks users to cite the abstract/manuscript once final metadata is available.
- Do not publish private backup refs into any repo that may later be public.

Key decisions:
- Use a clean orphan public-release history rather than trying to sanitize the existing public history in place.
- Keep the original full-history backup private and local unless the user explicitly requests a separate private archive remote.
- Treat `Results/2026-04-29/submission_assets/` as the source for a private release asset zip/checksum, but remove it from tracked git.

State:
- GitHub repo `reblocke/Hypercap-CC-NLP` is now PRIVATE.
- Current branch is orphan branch `codex/journal-public-release-v0.1.0`.
- Banned local surfaces were removed from this working tree after private backup.
- Clean release commit/tag/release were created and pushed to the private remote.

Done:
- Read previous ledger and memory notes.
- Confirmed `gh` auth is available for account `reblocke` with repo scope.
- Created private backup bundle and partial working-copy mirror under `../Hypercap-CC-NLP-private-backups/2026-05-30-pre-public-cleanup/`.
- Created release asset zip/checksum from `Results/2026-04-29/submission_assets/` under the private backup directory.
- Switched GitHub repo visibility from PUBLIC to PRIVATE.
- Restored only source/docs/tests/specs/public annotation resources onto the orphan release branch.
- Updated README, `.gitignore`, `pyproject.toml`, `Makefile`, `docs/SPEC.md`, and release documentation.
- Removed notebook compatibility targets from the public Makefile.
- Ran `uv sync --frozen`, `uv run pytest -q`, `uv run --with ruff ruff check src tests`, `git diff --cached --check`, public path hygiene scan, secret scan, and `make -n quarto-pipeline`.
- Committed the orphan public-release snapshot and tagged it `v0.1.0`.
- Force-updated private remote `main` to the clean orphan history and pushed `v0.1.0`.
- Created GitHub release `v0.1.0 - journal-submission code snapshot` and uploaded the submission-assets zip/checksum.
- Fresh-clone smoke test passed: path/history hygiene, `uv sync --frozen`, `make test`, `make lint`, `make -n quarto-pipeline`, and clear missing-private-workbook failure for `make quarto-classifier`.
- Removed local stale refs/stashes that still referenced pre-cleanup history; only the clean orphan branch remains locally.

Now:
- Final status and hygiene checks before reporting back.

Next:
- Report completion and remaining manual release step: review exact abstract citation metadata before making the repo public.

Open questions (UNCONFIRMED if needed):
- Exact published ATS abstract citation metadata.
- Whether to publish the sanitized branch/tag/release immediately after review.

Working set (files/ids/commands):
- `README.md`
- `docs/SPEC.md`
- `docs/DATA_ACCESS.md`
- `docs/MANUSCRIPT_MAPPING.md`
- `CITATION.cff`
- `CHANGELOG.md`
- `CONTRIBUTING.md`
- `CODE_OF_CONDUCT.md`
- `SECURITY.md`
- `.gitignore`
- `pyproject.toml`
- `Makefile`
- `scripts/run_pipeline_audit.py`
- `src/hypercap_cc_nlp/pipeline_audit.py`
- `tests/test_notebook_output_contracts.py`
- `tests/test_pipeline_audit.py`
