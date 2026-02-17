# Maintained convenience targets for the uv-based workflow.

QUARTO_PYTHON ?= $(PWD)/.venv/bin/python
REPORT_RUN_ID := $(shell date +%Y%m%d_%H%M%S)
REPORT_DIR ?= artifacts/reports/$(REPORT_RUN_ID)
BASELINE ?= latest
STAGE ?= all

.PHONY: setup spacy-model kernel-install bq-auth tinytex-install test lint format smoke baseline-capture-jupyter quarto-parity-check quarto-cohort quarto-classifier quarto-rater quarto-analysis quarto-pipeline quarto-pipeline-audit notebook-cohort notebook-classifier notebook-rater notebook-analysis notebook-pipeline notebook-pipeline-audit
.PHONY: check-resources clean-generated contracts-check

setup:
	uv sync

spacy-model:
	./.venv/bin/python -m spacy download en_core_web_sm

kernel-install:
	./.venv/bin/python -m ipykernel install --user --name hypercap-cc-nlp --display-name "Python (hypercap-cc-nlp)"

bq-auth:
	gcloud auth application-default login

tinytex-install:
	quarto install tinytex

test:
	uv run pytest -q

lint:
	uv run --with ruff ruff check src tests

format:
	uv run --with ruff ruff format src tests

smoke: test

check-resources:
	uv run python scripts/verify_classifier_resources.py

contracts-check:
	uv run python scripts/run_contract_checks.py --mode "$${PIPELINE_CONTRACT_MODE:-fail}" --stage "$(STAGE)"

clean-generated:
	rm -rf artifacts/reports/*
	rm -rf debug/pipeline_audit/*
	rm -rf debug/pipeline_parity/*
	rm -rf debug/contracts/*

baseline-capture-jupyter:
	uv run python scripts/capture_jupyter_baseline.py --label pre_quarto_migration

quarto-parity-check:
	uv run python scripts/compare_pipeline_baseline.py --baseline "$(BASELINE)"

quarto-cohort:
	QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "MIMICIV_hypercap_EXT_cohort.qmd" --to pdf --output-dir "$(REPORT_DIR)"
	@test -f "MIMIC tabular data/MIMICIV all with CC.xlsx" || (echo "Missing canonical cohort workbook after quarto cohort run." >&2; exit 1)
	@$(MAKE) contracts-check STAGE=cohort
	@test -f "$(REPORT_DIR)/MIMICIV_hypercap_EXT_cohort.pdf" || (echo "Missing cohort PDF output." >&2; exit 1)
	@cp -f "$(REPORT_DIR)/MIMICIV_hypercap_EXT_cohort.pdf" "./MIMICIV_hypercap_EXT_cohort.pdf"
	@test -f "./MIMICIV_hypercap_EXT_cohort.pdf" || (echo "Missing root cohort PDF output." >&2; exit 1)

quarto-classifier:
	@test -f "MIMIC tabular data/MIMICIV all with CC.xlsx" || (echo "Missing classifier input workbook. Run make quarto-cohort first." >&2; exit 1)
	QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "Hypercap CC NLP Classifier.qmd" --to pdf --output-dir "$(REPORT_DIR)"
	@if [ -f "$(REPORT_DIR)/Hypercap-CC-NLP-Classifier.pdf" ]; then mv -f "$(REPORT_DIR)/Hypercap-CC-NLP-Classifier.pdf" "$(REPORT_DIR)/Hypercap CC NLP Classifier.pdf"; fi
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing canonical NLP workbook after quarto classifier run." >&2; exit 1)
	@$(MAKE) contracts-check STAGE=classifier
	@test -f "$(REPORT_DIR)/Hypercap CC NLP Classifier.pdf" || (echo "Missing classifier PDF output." >&2; exit 1)
	@cp -f "$(REPORT_DIR)/Hypercap CC NLP Classifier.pdf" "./Hypercap CC NLP Classifier.pdf"
	@test -f "./Hypercap CC NLP Classifier.pdf" || (echo "Missing root classifier PDF output." >&2; exit 1)

quarto-rater:
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing rater NLP input workbook. Run make quarto-classifier first." >&2; exit 1)
	QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "Rater Agreement Analysis.qmd" --to pdf --output-dir "$(REPORT_DIR)"
	@if [ -f "$(REPORT_DIR)/Rater-Agreement-Analysis.pdf" ]; then mv -f "$(REPORT_DIR)/Rater-Agreement-Analysis.pdf" "$(REPORT_DIR)/Rater Agreement Analysis.pdf"; fi
	@$(MAKE) contracts-check STAGE=all
	@test -f "annotation_agreement_outputs_nlp/R3_vs_NLP_summary.txt" || (echo "Missing R3_vs_NLP_summary.txt after quarto rater run." >&2; exit 1)
	@test -f "$(REPORT_DIR)/Rater Agreement Analysis.pdf" || (echo "Missing rater PDF output." >&2; exit 1)
	@cp -f "$(REPORT_DIR)/Rater Agreement Analysis.pdf" "./Rater Agreement Analysis.pdf"
	@test -f "./Rater Agreement Analysis.pdf" || (echo "Missing root rater PDF output." >&2; exit 1)

quarto-analysis:
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing analysis input workbook. Run make quarto-classifier first." >&2; exit 1)
	QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "Hypercap CC NLP Analysis.qmd" --to pdf --output-dir "$(REPORT_DIR)"
	@if [ -f "$(REPORT_DIR)/Hypercap-CC-NLP-Analysis.pdf" ]; then mv -f "$(REPORT_DIR)/Hypercap-CC-NLP-Analysis.pdf" "$(REPORT_DIR)/Hypercap CC NLP Analysis.pdf"; fi
	@$(MAKE) contracts-check STAGE=all
	@test -f "$(REPORT_DIR)/Hypercap CC NLP Analysis.pdf" || (echo "Missing analysis PDF output." >&2; exit 1)
	@cp -f "$(REPORT_DIR)/Hypercap CC NLP Analysis.pdf" "./Hypercap CC NLP Analysis.pdf"
	@test -f "./Hypercap CC NLP Analysis.pdf" || (echo "Missing root analysis PDF output." >&2; exit 1)

quarto-pipeline: quarto-cohort quarto-classifier quarto-rater quarto-analysis

quarto-pipeline-audit: lint test
	uv run python scripts/run_pipeline_audit.py --pipeline-mode quarto --baseline "$(BASELINE)" --strictness fail_on_key_anomalies

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
