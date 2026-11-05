"""Verificações comuns de integração (usadas no Databricks e no Delta local)."""
from __future__ import annotations

from pyspark.sql import functions as F

from loyalty_lakehouse.pipeline import run_pipeline
from loyalty_lakehouse.quality import compare_with_ledger
from loyalty_lakehouse.utils import Lakehouse


def run_and_check(settings, spark, ledger_path) -> None:
    # 1) Execução completa (2 lotes) + Gold.
    first = run_pipeline(settings, spark=spark, upload=settings.is_databricks)
    lh = Lakehouse(settings, spark)

    # 2) Toda anomalia do ledger foi detectada na camada esperada, sem falsos positivos.
    rows = compare_with_ledger(lh, ledger_path, first.run_id)
    mismatches = [r for r in rows if not r["match"]]
    assert not mismatches, mismatches

    # 3) Reexecução do lote 2: idempotente (nenhum INSERT/UPDATE, Silver inalterada).
    before = {n: lh.read(lh.table("silver", n)).count() for n in ("purchases", "points_transactions")}
    q_before = lh.read(lh.table("silver", "quarantine")).count()
    again = run_pipeline(settings, spark=spark, batches=[first.batches[-1]], build_gold_layer=False)
    for entity, counts in again.silver[first.batches[-1]].items():
        assert counts.get("INSERT", 0) == 0 and counts.get("UPDATE", 0) == 0, (entity, counts)
    after = {n: lh.read(lh.table("silver", n)).count() for n in ("purchases", "points_transactions")}
    assert before == after
    assert lh.read(lh.table("silver", "quarantine")).count() == q_before

    # 4) Bronze não duplica ao reprocessar o lote (replaceWhere por _batch_id).
    b = lh.read(lh.table("bronze", "purchases")).groupBy("_batch_id").count().collect()
    assert len(b) == 2

    # 5) Delta: histórico com MERGE e time travel para a versão após o lote 1.
    ref = lh.table("silver", "purchases")
    hist = spark.sql(f"DESCRIBE HISTORY {ref.sql}").where(F.col("operation") == "MERGE").select("version").collect()
    assert len(hist) >= 2
    v_first = min(r["version"] for r in hist)
    n_v1 = spark.sql(f"SELECT count(*) AS n FROM {ref.sql} VERSION AS OF {v_first}").first()["n"]
    assert 0 < n_v1 < after["purchases"]

    # 6) Gold produzida a partir da Silver: vendas = compras COMPLETED + RETURNED da Silver.
    sold = lh.read(ref).where(F.col("purchase_status").isin("COMPLETED", "RETURNED")).count()
    gold_sold = lh.read(lh.table("gold", "kpi_monthly_sales")).agg(F.sum("completed_purchases")).first()[0]
    assert gold_sold == sold

    # 7) Governança: log de execução registrou as etapas, sem falhas.
    logs = lh.read(lh.table("governance", "pipeline_execution_log")).where(F.col("run_id") == first.run_id)
    assert logs.where(F.col("status") == "FAILED").count() == 0
    assert {r["step"] for r in logs.select("step").distinct().collect()} >= {
        "bronze_ingest", "silver_transform", "reconciliation", "quality_evaluation", "gold_build"}
