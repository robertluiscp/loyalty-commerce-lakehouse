"""Verificações de qualidade com população elegível e limites explícitos.

Cada verificação grava uma linha em ``governance.data_quality_results`` com numerador,
denominador, valor, limite e resultado. Verificações reprovadas abrem um incidente em
``governance.data_quality_incidents``. Definições: docs/data_quality.md.
"""
from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from pyspark.sql import functions as F

from .governance import GovernanceRecorder
from .reconciliation import POINTS_CHECKS
from .schemas import ENTITIES
from .silver import QUARANTINE_TABLE
from .utils import Lakehouse

DUPLICATE_REASONS = ("DUPLICATE_RECORD", "DUPLICATE_KEY_CONFLICT", "DUPLICATE_CREDIT")
REFERENCE_PREFIXES = ("UNKNOWN_", "ORPHAN_")


def _ratio(num: int, den: int) -> float | None:
    return None if den == 0 else num / den


def _check(gov: GovernanceRecorder, *, entity: str, name: str, category: str, population: str, num: int,
           den: int | None, value: float | None, threshold: float | None, comparator: str | None,
           severity: str, batch_id: str | None = None) -> bool | None:
    passed = None
    if comparator and value is not None:
        passed = value <= threshold if comparator == "<=" else value >= threshold
    gov.dq_result({"batch_id": batch_id, "entity": entity, "check_name": name, "check_category": category,
                   "population": population, "numerator": num, "denominator": den, "metric_value": value,
                   "threshold": threshold, "comparator": comparator, "passed": passed, "severity": severity})
    if passed is False:
        gov.incident(f"DQ_{category.upper()}", severity,
                     f"{name} reprovada: valor={value:.6g} {comparator} limite={threshold}",
                     batch_id=batch_id, entity=entity, affected=num)
    return passed


def evaluate_quality(lh: Lakehouse, gov: GovernanceRecorder, batch_ids: list[str],
                     reconciliation_counts: dict[str, int]) -> None:
    th = lh.settings.thresholds
    q = lh.read(lh.table("silver", QUARANTINE_TABLE))
    with gov.step("quality_evaluation") as info:
        # Contagens agregadas no cluster; só o resultado (poucas linhas) vem para o driver.
        q_stats = {(r["entity"], r["_batch_id"]): r for r in q.groupBy("entity", "_batch_id").agg(
            F.count("*").alias("quarantined"),
            F.sum(F.exists("reasons", lambda x: x.isin(*DUPLICATE_REASONS)).cast("int")).alias("dups"),
            F.sum(F.exists("reasons", lambda x: x.startswith("UNKNOWN_") | x.startswith("ORPHAN_")).cast("int")).alias("refs"),
        ).collect()}
        n_checks = 0
        for entity in ENTITIES:
            b_ref = lh.table("bronze", entity.name)
            if not lh.exists(b_ref):
                continue
            totals = {r["_batch_id"]: int(r["n"]) for r in
                      lh.read(b_ref).groupBy("_batch_id").agg(F.count("*").alias("n")).collect()}
            s_ref = lh.table("silver", entity.name)
            corrections = {}
            if lh.exists(s_ref):
                corrections = {r["_batch_id"]: int(r["n"]) for r in lh.read(s_ref)
                               .where(F.size("_corrections") > 0).groupBy("_batch_id")
                               .agg(F.count("*").alias("n")).collect()}
            for batch in batch_ids:
                rows = totals.get(batch, 0)
                if rows == 0:
                    continue
                st = q_stats.get((entity.name, batch))
                quarantined = int(st["quarantined"]) if st else 0
                dups = int(st["dups"] or 0) if st else 0
                refs = int(st["refs"] or 0) if st else 0
                pop = f"linhas do lote na bronze.{entity.name}"
                _check(gov, entity=entity.name, batch_id=batch, name="duplicate_rate", category="uniqueness",
                       population=pop, num=dups, den=rows, value=_ratio(dups, rows),
                       threshold=th["max_duplicate_rate"], comparator="<=", severity="ERROR")
                invalid = quarantined - dups
                _check(gov, entity=entity.name, batch_id=batch, name="invalid_rate", category="validity",
                       population=pop + " (quarentena exceto duplicidades)", num=invalid, den=rows,
                       value=_ratio(invalid, rows), threshold=th["max_invalid_rate"], comparator="<=",
                       severity="ERROR")
                _check(gov, entity=entity.name, batch_id=batch, name="referential_integrity", category="integrity",
                       population=pop, num=rows - refs, den=rows, value=_ratio(rows - refs, rows),
                       threshold=th["min_referential_integrity"], comparator=">=", severity="ERROR")
                _check(gov, entity=entity.name, batch_id=batch, name="quarantined_records", category="validity",
                       population=pop, num=quarantined, den=rows, value=float(quarantined), threshold=None,
                       comparator=None, severity="INFO")
                _check(gov, entity=entity.name, batch_id=batch, name="corrected_records", category="validity",
                       population=f"linhas aceitas na silver.{entity.name} vindas do lote", num=corrections.get(batch, 0),
                       den=None, value=float(corrections.get(batch, 0)), threshold=None, comparator=None,
                       severity="INFO")
                n_checks += 5

        # Verificações globais (estado atual da Silver).
        gold_like = lh.read(lh.table("silver", "points_transactions"))
        purchases = lh.read(lh.table("silver", "purchases"))
        redemptions = lh.read(lh.table("silver", "redemptions"))
        sold = purchases.where(F.col("purchase_status").isin("COMPLETED", "RETURNED")).count()
        red_done = redemptions.where(F.col("redemption_status") == "COMPLETED").count()
        unsold_credits = (gold_like.where((F.col("transaction_type") == "CREDIT") & (F.col("reason_code") == "PURCHASE"))
                          .join(purchases.where(~F.col("purchase_status").isin("COMPLETED", "RETURNED"))
                                .select("purchase_id"), "purchase_id").count())
        divergent = sum(v for k, v in reconciliation_counts.items() if k in POINTS_CHECKS and k != "NEGATIVE_BALANCE")
        den = sold + red_done + unsold_credits
        _check(gov, entity="points_transactions", name="points_inconsistency_rate", category="consistency",
               population="vendas concluídas + resgates concluídos + créditos de compras não vendidas",
               num=divergent, den=den, value=_ratio(divergent, den),
               threshold=th["max_points_inconsistency_rate"], comparator="<=", severity="ERROR")
        neg = reconciliation_counts.get("NEGATIVE_BALANCE", 0)
        _check(gov, entity="customers", name="negative_balance_customers", category="consistency",
               population="clientes com movimentações efetivas", num=neg, den=None, value=float(neg),
               threshold=0.0, comparator="<=", severity="WARNING")
        from .business_metrics import classify_credit_sla
        overdue = (classify_credit_sla(gold_like, lh.settings.reference_timestamp)
                   .where(F.col("sla_category") == "PENDING_OVERDUE").count())
        _check(gov, entity="points_transactions", name="pending_overdue_credits", category="timeliness",
               population="créditos pendentes na data de referência", num=overdue, den=None, value=float(overdue),
               threshold=0.0, comparator="<=", severity="WARNING")
        dup_track = reconciliation_counts.get("TRACKING_DUPLICATE_CONVERSIONS", 0)
        _check(gov, entity="campaign_events", name="campaigns_with_duplicate_tracking", category="uniqueness",
               population="campanhas com eventos CONVERSION", num=dup_track, den=None, value=float(dup_track),
               threshold=None, comparator=None, severity="INFO")
        info["rows_written"] = n_checks + 4


def compare_with_ledger(lh: Lakehouse, ledger_path: str | Path, run_id: str | None = None) -> list[dict]:
    """Compara as anomalias injetadas pelo gerador (ledger) com o que a pipeline detectou.

    Retorna uma linha por expectativa: ``expected`` (do ledger) x ``detected`` (Silver/Gold/
    governança). Só agregados pequenos vêm para o driver.
    """
    with open(ledger_path, encoding="utf-8") as fh:
        ledger = list(csv.DictReader(fh))
    expected = Counter((r["expected_detection"], r["entity"]) for r in ledger)
    q = lh.read(lh.table("silver", QUARANTINE_TABLE))
    # Registros distintos (não linhas): o mesmo registro rejeitado em 2 lotes conta uma vez.
    # Sem chave (ex.: JSON malformado), o hash do texto original identifica o registro.
    q_counts = {(r["entity"], r["primary_reason"]): int(r["n"]) for r in
                q.groupBy("entity", "primary_reason")
                 .agg(F.countDistinct(F.coalesce(F.col("business_key"), F.sha2("raw_record", 256))).alias("n"))
                 .collect()}
    audit = lh.read(lh.table("governance", "reconciliation_audit"))
    if run_id is None:
        run_id = audit.agg(F.max("run_id")).first()[0]
    rec = {r["check_name"]: int(r["n"]) for r in audit.where((F.col("run_id") == run_id) & (F.col("status") == "DIVERGENT"))
           .groupBy("check_name").agg(F.count("*").alias("n")).collect()}
    inc = lh.read(lh.table("governance", "data_quality_incidents"))
    inc_counts = {r["category"]: int(r["n"]) for r in inc.where(F.col("run_id") == run_id)
                  .groupBy("category").agg(F.count("*").alias("n")).collect()}
    gold_overdue = (lh.read(lh.table("gold", "fact_points_transactions"))
                    .where(F.col("sla_category") == "PENDING_OVERDUE").count())
    dup_track = lh.read(lh.table("gold", "kpi_campaign_performance")) \
        .agg(F.sum("tracking_duplicate_conversions")).first()[0] or 0
    rows = []
    for (detection, entity), n in sorted(expected.items()):
        layer, item = detection.split(":", 1)
        if layer == "silver_quarantine":
            detected = q_counts.get((entity, item), 0)
        elif layer == "reconciliation":
            detected = rec.get(item, 0)
        elif layer == "bronze_incident":
            detected = inc_counts.get(item, 0)
        elif item == "PENDING_OVERDUE":
            detected = gold_overdue
        elif item == "DUPLICATE_TRACKING_CONVERSION":
            detected = int(dup_track)
        else:
            detected = None
        rows.append({"expected_detection": detection, "entity": entity, "expected": n, "detected": detected,
                     "match": detected == n})
    # Quarentena sem anomalia correspondente no ledger = falso positivo.
    expected_q = {(e, d.split(":", 1)[1]) for (d, e) in expected if d.startswith("silver_quarantine")}
    for (entity, reason), n in sorted(q_counts.items()):
        if (entity, reason) not in expected_q:
            rows.append({"expected_detection": f"silver_quarantine:{reason}", "entity": entity, "expected": 0,
                         "detected": n, "match": False})
    for check, n in sorted(rec.items()):
        if not any(d == f"reconciliation:{check}" for d, _ in expected):
            rows.append({"expected_detection": f"reconciliation:{check}", "entity": "-", "expected": 0,
                         "detected": n, "match": False})
    return rows
