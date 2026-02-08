# Maintained convenience targets for the uv-based workflow.

.PHONY: setup spacy-model kernel-install bq-auth test lint format smoke notebook-cohort

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
