Goal (incl. success criteria):
- Integrate `Rater Agreement Analysis.ipynb` into the canonical execution chain:
  `MIMICIV_hypercap_EXT_cohort.ipynb -> Hypercap CC NLP Classifier.ipynb -> Rater Agreement Analysis.ipynb -> Hypercap CC NLP Analysis.ipynb`.
- Success criteria:
  - Rater notebook uses canonical classifier output by default, with env override support.
  - Rater join tolerates partial overlap and emits explicit unmatched/audit artifacts.
  - Makefile includes `notebook-rater` and pipeline ordering includes rater before analysis.
  - README documents updated order, overrides, and join-audit behavior.
  - Repo checks pass.

Constraints/Assumptions:
- No changes to scientific estimands; metrics remain computed on matched rows.
- No dependency additions.
- Existing unrelated dirty files are not reverted.
- Full pipeline notebook execution can be environment/runtime constrained.

Key decisions:
- Unmatched policy: warn + continue when matched rows > 0; fail when matched rows == 0.
- Analysis stage remains ordered-independent from rater artifacts.
- Canonical rater NLP input defaults to `MIMICIV all with CC_with_NLP.xlsx`.

State:
- Done: implementation and validation completed.
- Now: final handoff summary.
- Next: optional full `make notebook-pipeline` run in local environment if desired.

Done:
- Added `resolve_rater_nlp_input_path` to `src/hypercap_cc_nlp/workflow_contracts.py`.
- Added `src/hypercap_cc_nlp/rater_core.py`:
  - `normalize_join_keys`
  - `build_r3_nlp_join_audit`
- Exported new helpers in `src/hypercap_cc_nlp/__init__.py`.
- Added tests:
  - `tests/test_rater_core.py`
  - updated `tests/test_workflow_contracts.py` for rater resolver.
- Refactored `Rater Agreement Analysis.ipynb`:
  - canonical NLP resolver usage with `RATER_NLP_INPUT_FILENAME`
  - optional `RATER_ANNOTATION_PATH`
  - `src` bootstrap for imports
  - helper-based join audit with explicit warnings
  - new outputs:
    - `annotation_agreement_outputs_nlp/R3_vs_NLP_unmatched_adjudicated_keys.csv`
    - `annotation_agreement_outputs_nlp/R3_vs_NLP_unmatched_nlp_keys.csv`
    - `annotation_agreement_outputs_nlp/R3_vs_NLP_join_audit.json`
- Updated `Makefile`:
  - new target `notebook-rater`
  - `notebook-pipeline` now runs cohort -> classifier -> rater -> analysis.
- Updated `README.md` pipeline order, overrides, and result mapping docs.
- Validation:
  - `uv run pytest -q tests/test_rater_core.py tests/test_workflow_contracts.py` (pass)
  - `uv run --with ruff ruff check src tests` (pass)
  - `make test` (pass, 25 tests)
  - `make lint` (pass)
  - `make notebook-rater` (pass; artifacts written)
  - `make -n notebook-pipeline` confirms updated order.

Now:
- Deliver implementation summary and verification results.

Next:
- Optional: run full `make notebook-pipeline` end-to-end in user environment.

Open questions (UNCONFIRMED if needed):
- UNCONFIRMED: long-term preferred default annotation workbook filename/versioning policy.

Working set (files/ids/commands):
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/CONTINUITY.md`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/workflow_contracts.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/rater_core.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/__init__.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_workflow_contracts.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_rater_core.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Rater Agreement Analysis.ipynb`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Makefile`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/README.md`
- Commands: `make test`, `make lint`, `make notebook-rater`, `make -n notebook-pipeline`
