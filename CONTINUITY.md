Goal (incl. success criteria):
- Switch repo to uv/pyproject/uv.lock workflow; update README with correct paths, BigQuery methods summary, and repo layout notes; refactor notebooks to use .env workflow.

Constraints/Assumptions:
- Follow AGENTS.md instructions; Python-first; prioritize readability/reproducibility.
- Use Ruff only if lint/format needed; avoid pip/conda in committed docs.
- No Windows support required in README.

Key decisions:
- Use .env for configuration (BigQuery + working directory) across notebooks.
- Pin Python to 3.11 in pyproject for uv lock; move ydata-profiling to optional extra due to pandas compatibility.
- Remove legacy conda environment.yml.

State:
- Done: Updated README, created pyproject.toml and uv.lock, refactored notebooks for .env + WORK_DIR paths, removed environment.yml.
- Now: Report changes and note uv lock warnings/conflict resolution.
- Next: Apply any requested tweaks or run tests if asked.

Done:
- README.md updated for uv workflow, BigQuery methods summary, corrected notebook ordering, and accurate paths.
- pyproject.toml added; uv.lock generated via uv for Python 3.11.
- MIMICIV_hypercap_EXT_cohort.ipynb, Hypercap CC NLP Classifier.ipynb, Hypercap CC NLP Analysis.ipynb, Rater Agreement Analysis.ipynb refactored to load .env and use WORK_DIR/DATA_DIR.
- environment.yml removed; README layout updated.

Now:
- Prepare final response with summary and any follow-ups.

Next:
- If requested: adjust dependencies or clean notebook outputs.

Open questions (UNCONFIRMED if needed):
- Whether to update Drafts notebooks to .env workflow.

Working set (files/ids/commands):
- CONTINUITY.md
- README.md
- pyproject.toml
- uv.lock
- MIMICIV_hypercap_EXT_cohort.ipynb
- Hypercap CC NLP Classifier.ipynb
- Hypercap CC NLP Analysis.ipynb
- Rater Agreement Analysis.ipynb
- Command: uv lock --python 3.11
