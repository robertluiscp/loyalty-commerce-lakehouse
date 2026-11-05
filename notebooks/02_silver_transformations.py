# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: tipagem, validação, deduplicação e MERGE
# MAGIC **Entrada:** `bronze.<entidade>` filtrada por lote. **Saída:** `silver.<entidade>` (estado atual,
# MAGIC SCD tipo 1) e `silver.quarantine` (rejeitados com motivo e referência à origem).
# MAGIC
# MAGIC Cada linha recebe uma ação: `INSERT`, `UPDATE`, `UNCHANGED` (reenvio idêntico), `STALE`
# MAGIC (versão mais antiga que a atual) ou `QUARANTINE`. Só INSERT/UPDATE entram no `MERGE INTO`.
# MAGIC
# MAGIC **Shuffle:** a deduplicação usa `Window.partitionBy(chave)` (shuffle pela chave) e a comparação
# MAGIC com o estado atual é um join pela chave (shuffle dos dois lados). As validações de integridade
# MAGIC referencial usam `broadcast` das dimensões pequenas, evitando shuffle da tabela de fatos.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

if "lh" not in globals():  # fora do Databricks o %run é só um comentário
    exec(open(__file__.replace("02_silver_transformations.py", "_setup.py"), encoding="utf-8").read())

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

from loyalty_lakehouse.governance import GovernanceRecorder  # noqa: E402
from loyalty_lakehouse.ingestion import SourceFiles  # noqa: E402
from loyalty_lakehouse.schemas import ENTITIES  # noqa: E402
from loyalty_lakehouse.silver import Context, process_entity, reason_counts  # noqa: E402
from loyalty_lakehouse.utils import new_run_id, utc_now  # noqa: E402

gov = GovernanceRecorder(lh, new_run_id())
gov.ensure_tables()
ctx = Context(lh, settings.rules["timezone"], float(settings.rules["credit_sla_hours"]))
for batch in SourceFiles(settings).list_batches():
    for entity in ENTITIES:
        print(batch, entity.name, process_entity(lh, gov, ctx, entity.name, batch, utc_now()))
gov.flush()

# COMMAND ----------

# MAGIC %md Motivos de rejeição por entidade e lote:

# COMMAND ----------

show(reason_counts(lh).orderBy(F.desc("records")), 50)

# COMMAND ----------

# MAGIC %md Plano físico do estágio de compras (observe `BroadcastHashJoin` nas dimensões e `Exchange` na window):

# COMMAND ----------

from loyalty_lakehouse.silver import RULES, stage  # noqa: E402

raw = lh.read(lh.table("bronze", "purchases")).where("_batch_id = 'B002_20250701'")
stage(raw, RULES["purchases"], ctx, lh.read(lh.table("silver", "purchases"))).explain(mode="formatted")
