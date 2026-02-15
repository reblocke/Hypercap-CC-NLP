Goal (incl. success criteria):
- Implement an end-to-end pipeline reliability + reproducibility deep-dive workflow.
- Success criteria:
  - New codified audit command runs full notebook pipeline with isolated stage logs.
  - Audit validates contracts, numerical sanity, drift against latest prior run, and log findings.
  - Audit emits machine-readable and human-readable reports.
  - Critical P0/P1 findings can be remediated and verified in the same run cycle.
  - README documents deep-dive runbook and reproducibility checklist.

Constraints/Assumptions:
- No scientific threshold/estimand changes.
- No dependency additions unless strictly necessary.
- Baseline drift reference defaults to latest successful dated prior run.
- Existing dirty workspace state is preserved.

Key decisions:
- Implement new audit module under `src/hypercap_cc_nlp/pipeline_audit.py`.
- Add CLI entry script `scripts/run_pipeline_audit.py`.
- Add Make target `notebook-pipeline-audit`.
- Fail on key anomalies (P0/P1), keep lower-severity issues as warnings.
- Include preflight manifest capture (env/tooling/git versions and selected env knobs).

State:
- Done: deep-dive audit module, CLI, tests, Make target, and README runbook are implemented.
- Now: final handoff summary with verification artifacts.
- Next: optional follow-up to clear non-blocking P2 warning by setting `WORK_DIR` in `.env`.

Done:
- Confirmed current executable pipeline targets in `Makefile`.
- Confirmed current canonical outputs and dated baselines in `MIMIC tabular data/prior runs/`.
- Confirmed current QA artifacts (`qa_summary.json`, rater join audit artifacts) and current metrics availability.
- Added `src/hypercap_cc_nlp/pipeline_audit.py` with:
  - stage-isolated run execution/log capture
  - artifact/schema/hard-fail validations
  - drift calculations and log pattern scanning
  - consolidated report + markdown summary output
- Added `scripts/run_pipeline_audit.py` CLI entrypoint.
- Added `tests/test_pipeline_audit.py` for audit contracts and status logic.
- Added `make notebook-pipeline-audit` target.
- Remediated false-blocking preflight finding by reclassifying missing `.env` `WORK_DIR` from P1 to P2 advisory.
- Completed rerun: `make notebook-pipeline-audit` finished with `status=warning`, `P0=0`, `P1=0`, `P2=1`.
- Updated `README.md` with deep-dive audit runbook and reproducibility checklist.

Now:
- Prepare final implementation summary with paths, checks, and remaining advisory.

Next:
- Optional: set `WORK_DIR` in `.env` to remove advisory `missing_work_dir_env_key`.
- Optional: archive `debug/pipeline_audit/20260214_230044/` with run artifacts.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/CONTINUITY.md`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/pipeline_audit.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/scripts/run_pipeline_audit.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_pipeline_audit.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Makefile`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/README.md`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/debug/pipeline_audit/20260214_230044/audit_report.json`
- Commands:
  - `uv run pytest -q tests/test_pipeline_audit.py`
  - `uv run --with ruff ruff check src tests`
  - `make notebook-pipeline-audit`
