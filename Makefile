# Atalhos (Linux/macOS/WSL). No Windows, use diretamente os comandos python indicados no README.
PY ?= python
export PYTHONPATH := src

.PHONY: install install-databricks data data-small test test-spark test-delta test-databricks \
        pipeline-local pipeline-databricks benchmark sql-checks dashboard

install:            ## ambiente LOCAL (pyspark + delta-spark)
	$(PY) -m pip install -r requirements.txt

install-databricks: ## ambiente DATABRICKS (databricks-connect) - use outro venv
	$(PY) -m pip install -r requirements-databricks.txt

data:
	$(PY) scripts/generate_data.py

data-small:
	$(PY) scripts/generate_data.py --profile small

test:
	$(PY) scripts/run_tests.py unit

test-spark:
	$(PY) scripts/run_tests.py spark

test-delta:
	$(PY) scripts/run_tests.py delta

test-databricks:
	$(PY) scripts/run_tests.py databricks

pipeline-local:
	$(PY) scripts/run_local_pipeline.py

pipeline-databricks:
	$(PY) scripts/run_databricks_pipeline.py

benchmark:
	$(PY) -m loyalty_lakehouse.benchmark --mode local --output docs/results/benchmark_local.json

sql-checks:
	$(PY) scripts/run_sql_checks.py

dashboard:
	$(PY) scripts/build_dashboard.py
