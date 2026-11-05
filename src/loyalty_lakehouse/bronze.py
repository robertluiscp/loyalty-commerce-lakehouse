"""Camada Bronze: ingestão rastreável e idempotente por lote.

Entradas: manifesto e arquivos do lote no landing.
Saídas: tabelas Delta ``bronze.<entidade>`` com os valores ORIGINAIS (texto) mais colunas
técnicas: ``_source_file``, ``_batch_id``, ``_run_id``, ``_ingestion_timestamp``,
``_record_hash``, ``_extra_columns`` e ``_corrupt_record``.

Idempotência: cada lote é gravado com ``replaceWhere _batch_id = '<lote>'``. Reprocessar um
lote substitui apenas as linhas daquele lote, sem duplicar e sem tocar nos demais.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F

from .governance import GovernanceRecorder
from .ingestion import SourceFiles, read_raw
from .schemas import ENTITY_BY_NAME
from .utils import Lakehouse


def bronze_ref(lh: Lakehouse, entity_name: str):
    return lh.table("bronze", entity_name)


def ingest_batch(lh: Lakehouse, gov: GovernanceRecorder, source: SourceFiles, batch_id: str,
                 run_ts: datetime) -> dict[str, int]:
    """Ingere todos os arquivos listados no manifesto do lote. Retorna linhas por entidade."""
    spark = lh.spark
    manifest = source.manifest(batch_id)
    written: dict[str, int] = {}
    for item in manifest["files"]:
        entity = ENTITY_BY_NAME.get(item["entity"])
        if entity is None:
            gov.incident("UNKNOWN_ENTITY", "WARNING", f"entidade desconhecida no manifesto: {item['entity']}",
                         batch_id=batch_id, source=item["file"])
            continue
        with gov.step("bronze_ingest", batch_id=batch_id, entity=entity.name) as info:
            info["rows_read"] = int(item["rows"])
            if not source.exists(item["file"]):
                gov.incident("MISSING_SOURCE_FILE", "ERROR", "arquivo listado no manifesto não encontrado",
                             batch_id=batch_id, entity=entity.name, source=item["file"])
                info.update(status="SKIPPED", message="arquivo ausente")
                continue
            df, issues = read_raw(spark, entity, source.path(item["file"]))
            for issue in issues:
                gov.incident(issue["category"], issue["severity"], issue["description"],
                             batch_id=batch_id, entity=entity.name, source=item["file"])
            if df is None:
                info.update(status="SKIPPED", message="schema incompatível")
                continue
            out = (df.withColumn("_batch_id", F.lit(batch_id))
                     .withColumn("_run_id", F.lit(gov.run_id))
                     .withColumn("_ingestion_timestamp", F.lit(run_ts).cast("timestamp")))
            ref = bronze_ref(lh, entity.name)
            lh.write(out, ref, mode="overwrite", replace_where=f"_batch_id = '{batch_id}'")
            stats = (lh.read(ref).where(F.col("_batch_id") == batch_id)
                     .agg(F.count("*").alias("n"),
                          F.sum(F.when(F.col("_corrupt_record").isNotNull(), 1).otherwise(0)).alias("corrupt"))
                     .first())
            n, corrupt = int(stats["n"]), int(stats["corrupt"] or 0)
            written[entity.name] = n
            info["rows_written"] = n
            if corrupt:
                gov.incident("MALFORMED_RECORDS", "WARNING", f"{corrupt} linha(s) malformada(s) preservadas na Bronze",
                             batch_id=batch_id, entity=entity.name, affected=corrupt, source=item["file"])
            expected = int(item["rows"])
            gov.reconciliation_record({
                "check_name": "SOURCE_TO_BRONZE_ROWCOUNT", "entity": entity.name, "business_key": batch_id,
                "customer_id": None, "expected_value": float(expected), "actual_value": float(n),
                "difference": float(n - expected), "status": "OK" if n == expected else "DIVERGENT",
                "details": item["file"],
            })
            if n != expected:
                gov.incident("ROWCOUNT_MISMATCH", "ERROR", f"manifesto={expected}, bronze={n}",
                             batch_id=batch_id, entity=entity.name, affected=abs(n - expected))
    return written
