"""Reconciliação entre compras, movimentações de pontos e resgates (sobre a Silver).

Cada verificação produz linhas DIVERGENT em ``governance.reconciliation_audit`` (ou INFO,
quando é apenas comparativa). As divergências NÃO alteram a Silver: elas viram pendências
operacionais na Gold (``operational_backlog``) e entram no indicador de inconsistência.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from .business_metrics import customer_balances, expected_credit_points
from .governance import GovernanceRecorder
from .utils import Lakehouse

POINTS_CHECKS = ("MISSING_CREDIT", "CREDIT_FOR_NON_COMPLETED_PURCHASE", "POINTS_AMOUNT_MISMATCH",
                 "RETURN_REVERSAL_MISMATCH", "REDEMPTION_DEBIT_MISMATCH", "NEGATIVE_BALANCE")


def _audit(df: DataFrame, check: str, entity: str, key: str, customer: str | None,
           expected, actual, details, status: str = "DIVERGENT") -> DataFrame:
    exp = expected if not isinstance(expected, (int, float)) else F.lit(expected)
    act = actual if not isinstance(actual, (int, float)) else F.lit(actual)
    return df.select(
        F.lit(check).alias("check_name"), F.lit(entity).alias("entity"),
        F.col(key).cast("string").alias("business_key"),
        (F.col(customer) if customer else F.lit(None)).cast("string").alias("customer_id"),
        exp.cast("double").alias("expected_value"), act.cast("double").alias("actual_value"),
        (act.cast("double") - exp.cast("double")).alias("difference"),
        F.lit(status).alias("status"), details.alias("details"))


def build_checks(purchases: DataFrame, points: DataFrame, redemptions: DataFrame, campaigns: DataFrame,
                 events: DataFrame, rules: dict, reference_ts) -> dict[str, DataFrame]:
    """Devolve um DataFrame de divergências por verificação (sem gravar nada)."""
    pc = points.where((F.col("transaction_type") == "CREDIT") & (F.col("reason_code") == "PURCHASE")
                      & ~F.col("status").isin("REJECTED", "CANCELLED"))
    checks: dict[str, DataFrame] = {}

    # 1) Venda concluída sem crédito de pontos.
    sold = purchases.where(F.col("purchase_status").isin("COMPLETED", "RETURNED"))
    any_credit = points.where((F.col("transaction_type") == "CREDIT") & (F.col("reason_code") == "PURCHASE")) \
                       .select("purchase_id").distinct()
    missing = sold.join(any_credit, "purchase_id", "left_anti")
    checks["MISSING_CREDIT"] = _audit(missing, "MISSING_CREDIT", "purchases", "purchase_id", "customer_id",
                                      1, 0, F.concat(F.lit("status="), F.col("purchase_status")))

    # 2) Crédito válido para compra cancelada/pendente.
    p_status = purchases.select("purchase_id", F.col("purchase_status").alias("p_status"))
    bad = pc.join(p_status, "purchase_id").where(F.col("p_status").isin("CANCELLED", "PENDING"))
    checks["CREDIT_FOR_NON_COMPLETED_PURCHASE"] = _audit(
        bad, "CREDIT_FOR_NON_COMPLETED_PURCHASE", "points_transactions", "points_transaction_id", "customer_id",
        0, F.col("points_amount"), F.concat(F.lit("purchase_id="), F.col("purchase_id"),
                                            F.lit(" status="), F.col("p_status")))

    # 3) Pontos creditados diferentes da regra de pontuação.
    exp = expected_credit_points(sold, campaigns, float(rules["points_per_currency_unit"])) \
        .select("purchase_id", "expected_points")
    mism = pc.join(exp, "purchase_id").where(F.col("points_amount") != F.col("expected_points"))
    checks["POINTS_AMOUNT_MISMATCH"] = _audit(
        mism, "POINTS_AMOUNT_MISMATCH", "points_transactions", "points_transaction_id", "customer_id",
        F.col("expected_points"), F.col("points_amount"), F.concat(F.lit("purchase_id="), F.col("purchase_id")))

    # 4) Devolução sem estorno integral (estorno esperado = -crédito concluído).
    returned = purchases.where(F.col("purchase_status") == "RETURNED").select("purchase_id", "customer_id")
    credited = (pc.where(F.col("status") == "COMPLETED").groupBy("purchase_id")
                  .agg(F.min("points_amount").alias("credit_points")))
    reversal = (points.where((F.col("transaction_type") == "ADJUSTMENT") & (F.col("reason_code") == "PURCHASE_RETURN")
                             & (F.col("status") == "COMPLETED"))
                .groupBy("purchase_id").agg(F.sum("points_amount").alias("reversal_points")))
    rr = (returned.join(credited, "purchase_id", "left").join(reversal, "purchase_id", "left")
          .withColumn("expected", -F.coalesce(F.col("credit_points"), F.lit(0)))
          .withColumn("actual", F.coalesce(F.col("reversal_points"), F.lit(0)))
          .where(F.col("expected") != F.col("actual")))
    checks["RETURN_REVERSAL_MISMATCH"] = _audit(rr, "RETURN_REVERSAL_MISMATCH", "purchases", "purchase_id",
                                                "customer_id", F.col("expected"), F.col("actual"),
                                                F.lit("estorno esperado = -crédito concluído"))

    # 5) Resgate concluído deve ter exatamente um DEBIT concluído com os mesmos pontos.
    debits = (points.where((F.col("transaction_type") == "DEBIT") & (F.col("status") == "COMPLETED"))
              .groupBy("redemption_id").agg(F.count("*").alias("n_debits"), F.sum("points_amount").alias("debited")))
    rd = (redemptions.select("redemption_id", "customer_id", "redemption_status", "points_used")
          .join(debits, "redemption_id", "full")
          .withColumn("expected", F.when(F.col("redemption_status") == "COMPLETED", F.col("points_used")).otherwise(0))
          .withColumn("actual", F.coalesce(F.col("debited"), F.lit(0)))
          .where((F.col("expected") != F.col("actual"))
                 | ((F.col("redemption_status") == "COMPLETED") & (F.coalesce(F.col("n_debits"), F.lit(0)) != 1))))
    checks["REDEMPTION_DEBIT_MISMATCH"] = _audit(
        rd, "REDEMPTION_DEBIT_MISMATCH", "redemptions", "redemption_id", "customer_id",
        F.col("expected"), F.col("actual"),
        F.concat(F.lit("status="), F.coalesce(F.col("redemption_status"), F.lit("SEM_RESGATE")),
                 F.lit(" debitos="), F.coalesce(F.col("n_debits"), F.lit(0)).cast("string")))

    # 6) Saldo acumulado negativo em algum momento.
    neg = customer_balances(points, reference_ts).where("has_negative_balance")
    checks["NEGATIVE_BALANCE"] = _audit(neg, "NEGATIVE_BALANCE", "customers", "customer_id", "customer_id",
                                        0, F.col("min_running_balance"),
                                        F.concat(F.lit("saldo final="), F.col("points_balance").cast("string")))

    # 7) Comparativo (INFO): conversões do rastreamento x compras com código da campanha.
    track = (events.where(F.col("event_type") == "CONVERSION").groupBy("campaign_id")
             .agg(F.count("*").alias("tracking_events"),
                  F.countDistinct("purchase_id").alias("tracking_unique_purchases")))
    checks["TRACKING_DUPLICATE_CONVERSIONS"] = _audit(
        track.where(F.col("tracking_events") != F.col("tracking_unique_purchases")),
        "TRACKING_DUPLICATE_CONVERSIONS", "campaign_events", "campaign_id", None,
        F.col("tracking_unique_purchases"), F.col("tracking_events"),
        F.lit("eventos CONVERSION repetidos para a mesma compra"), status="INFO")
    return checks


def run_reconciliation(lh: Lakehouse, gov: GovernanceRecorder) -> dict[str, int]:
    """Executa todas as verificações, grava as divergências e devolve contagens por verificação."""
    s = lambda n: lh.read(lh.table("silver", n))
    settings = lh.settings
    with gov.step("reconciliation") as info:
        checks = build_checks(s("purchases"), s("points_transactions"), s("redemptions"), s("campaigns"),
                              s("campaign_events"), settings.rules, settings.reference_timestamp)
        union = None
        for df in checks.values():
            union = df if union is None else union.unionByName(df)
        gov.reconciliation_rows(union)
        audit = lh.read(lh.table("governance", "reconciliation_audit")).where(F.col("run_id") == gov.run_id)
        counts = {r["check_name"]: int(r["n"]) for r in
                  audit.where(F.col("check_name") != "SOURCE_TO_BRONZE_ROWCOUNT")
                       .groupBy("check_name").agg(F.count("*").alias("n")).collect()}
        counts = {name: counts.get(name, 0) for name in checks}
        info["rows_written"] = sum(counts.values())
        info["message"] = ", ".join(f"{k}={v}" for k, v in counts.items())
    return counts
