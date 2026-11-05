# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Parquet x CSV x Delta: benchmark e recursos do Delta Lake
# MAGIC 1. **Benchmark** com os mesmos dados (Silver `purchases` replicada 10x), 3 repetições, mediana:
# MAGIC    tamanho, escrita, leitura completa (`format("noop")`) e leitura de 2 colunas com agregação.
# MAGIC 2. **Delta Lake**: histórico dos MERGEs, time travel, schema enforcement, `mergeSchema` e a
# MAGIC    diferença entre um diretório Parquet e uma tabela Delta com log transacional.
# MAGIC
# MAGIC Os tempos variam com volume, compressão, hardware, cache e configuração; no Databricks Connect
# MAGIC incluem a ida e volta de rede. Metodologia: `docs/benchmark_methodology.md`.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

if "lh" not in globals():  # fora do Databricks o %run é só um comentário
    exec(open(__file__.replace("06_parquet_delta_benchmark.py", "_setup.py"), encoding="utf-8").read())

# COMMAND ----------

import json  # noqa: E402

from loyalty_lakehouse.benchmark import spark_benchmark  # noqa: E402
from loyalty_lakehouse.delta_features import run_all  # noqa: E402

bench = spark_benchmark(lh, replicate=10, repeats=3)
print(json.dumps(bench, indent=2, ensure_ascii=False))

# COMMAND ----------

delta = run_all(lh)
print(json.dumps(delta, indent=2, ensure_ascii=False, default=str))
