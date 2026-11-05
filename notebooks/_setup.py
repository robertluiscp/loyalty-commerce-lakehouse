# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup compartilhado (executado via `%run ./_setup` pelos demais notebooks)
# MAGIC Coloca `src/` no `sys.path`, carrega a configuração e cria/reaproveita a SparkSession.
# MAGIC * No workspace (Git folder): usa a `spark` do notebook serverless e `config/databricks.example.yaml`.
# MAGIC * Localmente (`python notebooks/01_...py`): usa `LLH_ENV` (`databricks` = Databricks Connect, `local` = Spark local).

# COMMAND ----------

import os
import sys
from pathlib import Path

try:
    _here = Path(__file__).resolve().parent            # execução como script
except NameError:                                        # execução como notebook no Databricks
    _here = Path(os.getcwd())
REPO_ROOT = _here.parent if (_here.parent / "src").exists() else _here
sys.path.insert(0, str(REPO_ROOT / "src"))

from loyalty_lakehouse.config import load_settings  # noqa: E402
from loyalty_lakehouse.utils import Lakehouse, get_spark, running_inside_databricks  # noqa: E402

ENV = "databricks" if running_inside_databricks() else os.environ.get("LLH_ENV", "databricks")
settings = load_settings(ENV)
spark = get_spark(settings)
lh = Lakehouse(settings, spark)


def show(df, n: int = 20):
    """display() no Databricks; impressão tabular fora dele."""
    try:
        display(df.limit(n))  # noqa: F821  (função do notebook Databricks)
    except NameError:
        df.show(n, truncate=False)


print(f"ambiente={ENV} | catálogo={settings.storage.get('catalog')} | spark={spark.version}")
