# Maintained convenience targets for the uv-based workflow.

.PHONY: setup spacy-model kernel-install bq-auth test lint format smoke notebook-cohort notebook-classifier notebook-rater notebook-analysis notebook-pipeline notebook-pipeline-audit

setup:
	uv sync

spacy-model:
	./.venv/bin/python -m spacy download en_core_web_sm

kernel-install:
	./.venv/bin/python -m ipykernel install --user --name hypercap-cc-nlp --display-name "Python (hypercap-cc-nlp)"

bq-auth:
	gcloud auth application-default login

test:
	uv run pytest -q

lint:
	uv run --with ruff ruff check src tests

format:
	uv run --with ruff ruff format src tests

smoke: test

notebook-cohort:
	JUPYTER_PATH="$$PWD/.jupyter" ./.venv/bin/python -m jupyter nbconvert --to notebook --execute --inplace --ClearOutputPreprocessor.enabled=True --ExecutePreprocessor.timeout=0 --ExecutePreprocessor.kernel_name=hypercap-cc-nlp "MIMICIV_hypercap_EXT_cohort.ipynb"
	@test -f "MIMIC tabular data/MIMICIV all with CC.xlsx" || (echo "Missing canonical cohort workbook after cohort notebook run." >&2; exit 1)

notebook-classifier:
	@test -f "MIMIC tabular data/MIMICIV all with CC.xlsx" || (echo "Missing classifier input workbook. Run make notebook-cohort first." >&2; exit 1)
	JUPYTER_PATH="$$PWD/.jupyter" ./.venv/bin/python -m jupyter nbconvert --to notebook --execute --inplace --ClearOutputPreprocessor.enabled=True --ExecutePreprocessor.timeout=0 --ExecutePreprocessor.kernel_name=hypercap-cc-nlp "Hypercap CC NLP Classifier.ipynb"
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing canonical NLP workbook after classifier notebook run." >&2; exit 1)

notebook-rater:
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing rater NLP input workbook. Run make notebook-classifier first." >&2; exit 1)
	JUPYTER_PATH="$$PWD/.jupyter" ./.venv/bin/python -m jupyter nbconvert --to notebook --execute --inplace --ClearOutputPreprocessor.enabled=True --ExecutePreprocessor.timeout=0 --ExecutePreprocessor.kernel_name=hypercap-cc-nlp "Rater Agreement Analysis.ipynb"
	@test -f "annotation_agreement_outputs_nlp/R3_vs_NLP_summary.txt" || (echo "Missing R3_vs_NLP_summary.txt after rater notebook run." >&2; exit 1)

notebook-analysis:
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing analysis input workbook. Run make notebook-classifier first." >&2; exit 1)
	JUPYTER_PATH="$$PWD/.jupyter" ./.venv/bin/python -m jupyter nbconvert --to notebook --execute --inplace --ClearOutputPreprocessor.enabled=True --ExecutePreprocessor.timeout=0 --ExecutePreprocessor.kernel_name=hypercap-cc-nlp "Hypercap CC NLP Analysis.ipynb"

notebook-pipeline: notebook-cohort notebook-classifier notebook-rater notebook-analysis

notebook-pipeline-audit: lint test
	uv run python scripts/run_pipeline_audit.py --baseline latest --strictness fail_on_key_anomalies
