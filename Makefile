# Use one path: conda OR pure pip. Targets are idempotent.

# -------- Conda workflow --------
conda-create:
	conda env create -f environment.yml || conda env update -f environment.yml
	@echo "Activate: conda activate mimiciv-tabular"
	@echo "Install Jupyter kernel:"
	@echo "  python -m ipykernel install --user --name mimiciv-tabular --display-name 'Python (mimiciv-tabular)'"

# -------- Pure pip workflow --------
venv-create:
	python3 -m venv .venv
	. .venv/bin/activate && python -m pip install --upgrade pip pip-tools
	@echo "Activate: source .venv/bin/activate"

pip-compile:
	. .venv/bin/activate && pip-compile --generate-hashes -o requirements.txt requirements.in

pip-sync:
	. .venv/bin/activate && pip-sync requirements.txt
	. .venv/bin/activate && python -m ipykernel install --user --name mimiciv-tabular --display-name 'Python (mimiciv-tabular)'

# -------- Auth + tests --------
bq-auth:
	gcloud auth application-default login

smoke:
	. .venv/bin/activate 2>/dev/null || true; python smoke_test.py

# Phony
.PHONY: conda-create venv-create pip-compile pip-sync bq-auth smoke
