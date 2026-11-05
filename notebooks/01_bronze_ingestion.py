# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingestão Bronze
# MAGIC **Entrada:** manifestos e arquivos CSV/JSON no Volume `loyalty_lakehouse.bronze.landing`
# MAGIC (gerados por `scripts/generate_data.py` e enviados com `--upload`).
# MAGIC **Saída:** tabelas Delta `bronze.<entidade>` com valores originais (texto) + auditoria.
# MAGIC
# MAGIC Pontos de atenção:
# MAGIC * schema explícito em texto: nenhuma conversão "silenciosa" na Bronze;
# MAGIC * `_metadata.file_path` registra o arquivo de origem de cada linha;
# MAGIC * `replaceWhere _batch_id = ...` torna o reprocessamento de um lote idempotente;
# MAGIC * o manifesto permite detectar arquivo ausente e divergência de contagem.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

if "lh" not in globals():  # fora do Databricks o %run é só um comentário
    exec(open(__file__.replace("01_bronze_ingestion.py", "_setup.py"), encoding="utf-8").read())

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

from loyalty_lakehouse.bronze import ingest_batch  # noqa: E402
from loyalty_lakehouse.governance import GovernanceRecorder  # noqa: E402
from loyalty_lakehouse.ingestion import SourceFiles  # noqa: E402
from loyalty_lakehouse.utils import new_run_id, utc_now  # noqa: E402

source = SourceFiles(settings)
batches = source.list_batches()
print("Lotes disponíveis:", batches)

# COMMAND ----------

lh.ensure_schemas()
gov = GovernanceRecorder(lh, new_run_id())
gov.ensure_tables()
for batch in batches:
    print(batch, ingest_batch(lh, gov, source, batch, utc_now()))
gov.flush()

# COMMAND ----------

# MAGIC %md Linhas por lote e arquivo de origem (rastreabilidade):

# COMMAND ----------

show(lh.read(lh.table("bronze", "purchases")).groupBy("_batch_id", "_source_file").count())

# COMMAND ----------

# MAGIC %md Linhas malformadas preservadas (JSON truncado) e colunas extras (schema drift):

# COMMAND ----------

show(lh.read(lh.table("bronze", "points_transactions")).where(F.col("_corrupt_record").isNotNull())
     .select("_corrupt_record", "_source_file", "_batch_id"))
show(lh.read(lh.table("bronze", "redemptions")).where(F.col("_extra_columns").isNotNull())
     .select("redemption_id", "_extra_columns").limit(5))
