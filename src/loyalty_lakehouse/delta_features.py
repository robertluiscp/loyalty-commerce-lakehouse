"""Demonstrações de recursos do Delta Lake sobre as tabelas reais do projeto.

* histórico de versões (``DESCRIBE HISTORY``) gerado pelos MERGEs da Silver;
* time travel (``VERSION AS OF``) comparando o estado após o lote 1 e o estado atual;
* schema enforcement (append com tipo incompatível é rejeitado);
* evolução de schema controlada (``mergeSchema``) numa tabela de demonstração;
* diferença entre "um diretório de arquivos Parquet" e "uma tabela Delta com log transacional".

Usa uma tabela própria de demonstração (``silver.delta_feature_demo``) para os testes de
schema, sem alterar as tabelas da pipeline.
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from .utils import Lakehouse


def table_history(lh: Lakehouse, layer: str, name: str, limit: int = 20):
    ref = lh.table(layer, name)
    return lh.spark.sql(f"DESCRIBE HISTORY {ref.sql} LIMIT {int(limit)}").select(
        "version", "timestamp", "operation", F.col("operationMetrics").alias("metrics"))


def time_travel_summary(lh: Lakehouse) -> list[dict]:
    """Status das compras em versões da Silver que registraram um MERGE.

    Time travel só alcança o período de retenção (7 dias por padrão): versões mais antigas
    continuam no histórico, mas a leitura é bloqueada e aparece aqui como indisponível.
    """
    ref = lh.table("silver", "purchases")
    versions = sorted({r["version"] for r in table_history(lh, "silver", "purchases", 50)
                       .where(F.col("operation") == "MERGE").select("version").collect()})
    out = []
    for v in sorted(set(versions[:3] + versions[-1:])):
        try:
            rows = lh.spark.sql(f"SELECT purchase_status, count(*) AS n FROM {ref.sql} VERSION AS OF {v} "
                                f"GROUP BY purchase_status ORDER BY purchase_status").collect()
            out.append({"version": v, "status_counts": {r["purchase_status"]: r["n"] for r in rows}})
        except Exception as exc:
            out.append({"version": v, "unavailable": f"{type(exc).__name__}: {str(exc).strip().splitlines()[0][:160]}"})
    return out


def time_travel_demo(lh: Lakehouse) -> dict:
    """Time travel com versões recentes: carga inicial (compras até abril) + MERGE do restante."""
    spark = lh.spark
    ref = lh.table("silver", "delta_time_travel_demo")
    src = lh.read(lh.table("silver", "purchases")).select("purchase_id", "purchase_date", "purchase_status")
    lh.write(src.where("purchase_date < '2025-05-01'"), ref, mode="overwrite", overwrite_schema=True)
    view = lh.temp_view(src.where("purchase_date >= '2025-05-01'"), "src_time_travel_demo")
    spark.sql(f"MERGE INTO {ref.sql} AS t USING {view} AS s ON t.purchase_id = s.purchase_id "
              f"WHEN NOT MATCHED THEN INSERT *")
    current = table_history(lh, "silver", "delta_time_travel_demo").agg(F.max("version")).first()[0]
    count = lambda v: spark.sql(f"SELECT count(*) AS n FROM {ref.sql} VERSION AS OF {v}").first()["n"]
    return {"table": str(ref), "version_before_merge": {"version": current - 1, "rows": count(current - 1)},
            "version_after_merge": {"version": current, "rows": count(current)}}


def schema_demo(lh: Lakehouse) -> dict:
    spark = lh.spark
    ref = lh.table("silver", "delta_feature_demo")
    base = spark.createDataFrame([("C000001", 100), ("C000002", 250)], "customer_id string, points bigint")
    lh.write(base, ref, mode="overwrite", overwrite_schema=True)
    result: dict = {"table": str(ref)}
    # 1) Tipo incompatível: string numa coluna bigint => erro, nada é gravado. (No Databricks o
    #    erro vem da conversão ANSI na escrita - store assignment - antes do commit no Delta.)
    bad = spark.createDataFrame([("C000003", "muitos")], "customer_id string, points string")
    try:
        lh.write(bad, ref, mode="append")
        result["enforcement"] = "ACEITO (inesperado)"
    except Exception as exc:  # AnalysisException (local) / erro equivalente no Databricks
        result["enforcement"] = f"REJEITADO: {type(exc).__name__}: {str(exc).splitlines()[0][:220]}"
    # 2) Coluna nova sem mergeSchema => rejeitada; com mergeSchema => schema evolui.
    extra = spark.createDataFrame([("C000004", 80, "APP")], "customer_id string, points bigint, channel string")
    try:
        lh.write(extra, ref, mode="append")
        result["new_column_without_merge_schema"] = "ACEITO (inesperado)"
    except Exception as exc:
        result["new_column_without_merge_schema"] = f"REJEITADO: {type(exc).__name__}"
    lh.write(extra, ref, mode="append", merge_schema=True)
    final = lh.read(ref)
    result["columns_after_merge_schema"] = final.columns
    result["rows_after"] = final.count()
    result["history"] = [{"version": r["version"], "operation": r["operation"]}
                         for r in table_history(lh, "silver", "delta_feature_demo").collect()]
    return result


def parquet_vs_delta(lh: Lakehouse, base_path: str) -> dict:
    """Mesmos dados, duas gravações: Parquet sobrescrito perde o estado anterior; Delta mantém versões."""
    spark = lh.spark
    v1 = spark.createDataFrame([(1, "A")], "id int, v string")
    v2 = spark.createDataFrame([(1, "B"), (2, "C")], "id int, v string")
    pq, dl = f"{base_path}/parquet_demo", f"{base_path}/delta_demo"
    for df in (v1, v2):
        df.write.mode("overwrite").parquet(pq)
        df.write.format("delta").mode("overwrite").save(dl)
    history = spark.sql(f"DESCRIBE HISTORY delta.`{dl}`")
    latest = history.agg(F.max("version")).first()[0]
    old = spark.read.format("delta").option("versionAsOf", latest - 1).load(dl).orderBy("id").collect()
    return {
        "parquet_rows_now": spark.read.parquet(pq).count(),
        "parquet_previous_version": "indisponível: overwrite substituiu os arquivos",
        "delta_rows_now": spark.read.format("delta").load(dl).count(),
        "delta_previous_version": {"version": latest - 1, "rows": [r.asDict() for r in old]},
        "delta_versions": history.count(),
    }


def run_all(lh: Lakehouse, base_path: str | None = None) -> dict:
    base_path = base_path or (f"/Volumes/{lh.catalog}/bronze/landing/_delta_demo" if lh.settings.is_databricks
                              else lh.settings.resolve("data/benchmark/delta_demo").as_posix())
    hist = table_history(lh, "silver", "purchases").collect()
    return {
        "silver_purchases_history": [{"version": r["version"], "operation": r["operation"],
                                      "metrics": {k: v for k, v in (r["metrics"] or {}).items()
                                                  if k in ("numTargetRowsInserted", "numTargetRowsUpdated",
                                                           "numOutputRows", "numSourceRows")}}
                                     for r in hist],
        "time_travel": time_travel_summary(lh),
        "time_travel_demo": time_travel_demo(lh),
        "schema": schema_demo(lh),
        "parquet_vs_delta": parquet_vs_delta(lh, base_path),
    }


if __name__ == "__main__":
    import argparse
    from .config import load_settings
    from .utils import get_spark
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="local", choices=["local", "databricks"])
    p.add_argument("--output", default=None)
    a = p.parse_args()
    s = load_settings(a.env)
    res = run_all(Lakehouse(s, get_spark(s)))
    text = json.dumps(res, indent=2, ensure_ascii=False, default=str)
    if a.output:
        out = s.resolve(a.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
    print(text)
