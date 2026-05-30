# Maintained convenience targets for the uv-based workflow.

QUARTO_PYTHON ?= $(PWD)/.venv/bin/python
RESULTS_DATE ?= $(shell date +%Y-%m-%d)
RESULTS_DIR ?= Results/$(RESULTS_DATE)
BASELINE ?= latest
STAGE ?= all

.PHONY: setup spacy-model kernel-install bq-auth tinytex-install test lint format smoke baseline-capture-jupyter quarto-parity-check quarto-cohort quarto-classifier quarto-rater quarto-analysis quarto-chart-review quarto-reyan-figures quarto-pipeline quarto-pipeline-audit
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
	rm -rf Results/*
	rm -rf debug/pipeline_audit/*
	rm -rf debug/pipeline_parity/*
	rm -rf debug/contracts/*

baseline-capture-jupyter:
	uv run python scripts/capture_jupyter_baseline.py --label pre_quarto_migration

quarto-parity-check:
	uv run python scripts/compare_pipeline_baseline.py --baseline "$(BASELINE)"

quarto-cohort:
	@mkdir -p "$(RESULTS_DIR)"
	RESULTS_DATE="$(RESULTS_DATE)" QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "MIMICIV_hypercap_EXT_cohort.qmd" --to pdf --output-dir "$(RESULTS_DIR)" --output "MIMICIV_hypercap_EXT_cohort.pdf"
	@test -f "MIMIC tabular data/MIMICIV all with CC.xlsx" || (echo "Missing canonical cohort workbook after quarto cohort run." >&2; exit 1)
	@test -f "$(RESULTS_DIR)/MIMICIV_hypercap_EXT_cohort.pdf" || (echo "Missing cohort PDF output." >&2; exit 1)

quarto-classifier:
	@test -f "MIMIC tabular data/MIMICIV all with CC.xlsx" || (echo "Missing classifier input workbook. Run make quarto-cohort first." >&2; exit 1)
	@mkdir -p "$(RESULTS_DIR)"
	RESULTS_DATE="$(RESULTS_DATE)" QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "Hypercap CC NLP Classifier.qmd" --to pdf --output-dir "$(RESULTS_DIR)" --output "Hypercap CC NLP Classifier.pdf"
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing canonical NLP workbook after quarto classifier run." >&2; exit 1)
	@test -f "$(RESULTS_DIR)/Hypercap CC NLP Classifier.pdf" || (echo "Missing classifier PDF output." >&2; exit 1)

quarto-rater:
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing rater NLP input workbook. Run make quarto-classifier first." >&2; exit 1)
	@mkdir -p "$(RESULTS_DIR)"
	RESULTS_DATE="$(RESULTS_DATE)" QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "Rater Agreement Analysis.qmd" --to pdf --output-dir "$(RESULTS_DIR)" --output "Rater Agreement Analysis.pdf"
	@test -f "artifacts/qa/rater_agreement/R3_vs_NLP_summary.txt" || (echo "Missing R3_vs_NLP_summary.txt after quarto rater run." >&2; exit 1)
	@test -f "$(RESULTS_DIR)/Rater Agreement Analysis.pdf" || (echo "Missing rater PDF output." >&2; exit 1)

quarto-analysis:
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing analysis input workbook. Run make quarto-classifier first." >&2; exit 1)
	@mkdir -p "$(RESULTS_DIR)"
	RESULTS_DATE="$(RESULTS_DATE)" QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "Hypercap CC NLP Analysis.qmd" --to pdf --output-dir "$(RESULTS_DIR)" --output "Hypercap CC NLP Analysis.pdf"
	@test -f "$(RESULTS_DIR)/Hypercap CC NLP Analysis.pdf" || (echo "Missing analysis PDF output." >&2; exit 1)

quarto-reyan-figures:
	@test -f "MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx" || (echo "Missing merged analysis input workbook. Run make quarto-classifier first." >&2; exit 1)
	@mkdir -p "$(RESULTS_DIR)"
	RESULTS_DATE="$(RESULTS_DATE)" QUARTO_PYTHON="$(QUARTO_PYTHON)" quarto render "Hypercap CC NLP Analysis.qmd" --to pdf --output-dir "$(RESULTS_DIR)" --output "Hypercap CC NLP Analysis.pdf"
	@test -f "$(RESULTS_DIR)/Figure 4.png" || (echo "Missing representative main-manuscript figure after merged analysis render." >&2; exit 1)
	@test -f "$(RESULTS_DIR)/Figure S1.png" || (echo "Missing representative supplement figure after merged analysis render." >&2; exit 1)
	@test -f "$(RESULTS_DIR)/Hypercap CC NLP Analysis.pdf" || (echo "Missing merged analysis PDF output." >&2; exit 1)

quarto-chart-review:
	@mkdir -p "$(RESULTS_DIR)"
	RESULTS_DATE="$(RESULTS_DATE)" quarto render "Chart Review Sample Calc.qmd"
	@test -f "$(RESULTS_DIR)/Chart Review Sample Calc.html" || (echo "Missing chart review HTML output." >&2; exit 1)

quarto-pipeline: quarto-cohort quarto-classifier quarto-rater quarto-analysis

quarto-pipeline-audit: lint test
	uv run python scripts/run_pipeline_audit.py --baseline "$(BASELINE)" --strictness fail_on_key_anomalies
