# Reproducible environment for MIMIC‑IV Tabular Build

Two supported setups: **Conda** or **pure pip + pip-tools**. Both use a `.env` file to configure backends (BigQuery/Postgres/DuckDB).

## 1) Configure environment variables
Copy the template and edit values:
```bash
cp .env.example .env
# edit .env with your project id and dataset names
```

## 2A) Conda workflow
```bash
conda env create -f environment.yml             # or: make conda-create
conda activate mimiciv-tabular
python -m ipykernel install --user --name mimiciv-tabular --display-name "Python (mimiciv-tabular)"
```

## 3) Authenticate BigQuery
```bash
gcloud auth application-default login            # or: make bq-auth
```

## 4) Smoke test
```bash
python smoke_test.py                             # or: make smoke
```

## 5) Use in Jupyter
Start JupyterLab, pick the kernel **Python (mimiciv-tabular)**, and run the provided notebooks.

Notes
- Set `MIMIC_BACKEND=bigquery` to use BigQuery. Keep dataset names exactly as shown in the BigQuery Explorer.
- Postgres is supported; set `MIMIC_BACKEND=postgres` and `MIMIC_PG_CONN_STR`.
- DuckDB is supported for local Parquet/CSV; set `MIMIC_BACKEND=duckdb` and register your files as views before running SQL.
