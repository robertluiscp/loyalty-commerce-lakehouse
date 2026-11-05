"""Regras de negócio de pontos, compras e campanhas (cenários determinísticos)."""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import functions as F

from helpers import REF
from loyalty_lakehouse.business_metrics import (attribute_purchases, classify_credit_sla, customer_balances,
                                                effective_flag, expected_credit_points, purchase_measures,
                                                safe_rate, signed_points)

pytestmark = pytest.mark.spark

POINTS_DDL = ("points_transaction_id string, customer_id string, transaction_type string, points_amount bigint, "
              "status string, transaction_timestamp timestamp, expected_completion_timestamp timestamp, "
              "actual_completion_timestamp timestamp")


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def points(spark, rows):
    return spark.createDataFrame([(r[0], r[1], r[2], r[3], r[4], ts(r[5]), ts(r[6]) if r[6] else None,
                                   ts(r[7]) if r[7] else None) for r in rows], POINTS_DDL)


def test_signed_points_by_type(spark):
    df = points(spark, [("T1", "C1", "CREDIT", 100, "COMPLETED", "2025-01-01T10:00:00+00:00", None, "2025-01-01T10:00:00+00:00"),
                        ("T2", "C1", "DEBIT", 30, "COMPLETED", "2025-01-02T10:00:00+00:00", None, "2025-01-02T10:00:00+00:00"),
                        ("T3", "C1", "EXPIRATION", 20, "COMPLETED", "2025-01-03T10:00:00+00:00", None, "2025-01-03T10:00:00+00:00"),
                        ("T4", "C1", "ADJUSTMENT", -10, "COMPLETED", "2025-01-04T10:00:00+00:00", None, "2025-01-04T10:00:00+00:00"),
                        ("T5", "C1", "ADJUSTMENT", 15, "COMPLETED", "2025-01-05T10:00:00+00:00", None, "2025-01-05T10:00:00+00:00")])
    got = {r["points_transaction_id"]: r["signed_points"] for r in signed_points(df).collect()}
    assert got == {"T1": 100, "T2": -30, "T3": -20, "T4": -10, "T5": 15}


def test_balance_uses_all_history_and_only_effective_movements(spark):
    df = points(spark, [
        ("T1", "C1", "CREDIT", 100, "COMPLETED", "2025-01-05T10:00:00+00:00", None, "2025-01-05T12:00:00+00:00"),  # mês anterior
        ("T2", "C1", "DEBIT", 30, "COMPLETED", "2025-03-01T10:00:00+00:00", None, "2025-03-01T10:00:00+00:00"),
        ("T3", "C1", "EXPIRATION", 20, "COMPLETED", "2025-04-01T10:00:00+00:00", None, "2025-04-01T10:00:00+00:00"),
        ("T4", "C1", "ADJUSTMENT", -10, "COMPLETED", "2025-05-01T10:00:00+00:00", None, "2025-05-01T10:00:00+00:00"),
        ("T5", "C1", "CREDIT", 50, "PENDING", "2025-06-29T10:00:00+00:00", None, None),            # pendente: fora
        ("T6", "C1", "CREDIT", 70, "REJECTED", "2025-06-01T10:00:00+00:00", None, None),           # rejeitado: fora
        ("T7", "C1", "CREDIT", 40, "COMPLETED", "2025-06-30T10:00:00+00:00", None, "2025-07-02T10:00:00+00:00"),  # após ref
    ])
    row = customer_balances(df, REF).first()
    assert row["points_balance"] == 100 - 30 - 20 - 10
    assert row["points_credited"] == 100 and row["points_debited"] == 30 and row["points_expired"] == 20
    assert row["points_adjusted"] == -10
    assert row["pending_credit_points"] == 50 + 40
    assert row["has_negative_balance"] is False


def test_negative_running_balance_is_detected_even_if_final_balance_is_positive(spark):
    df = points(spark, [
        ("T1", "C2", "CREDIT", 100, "COMPLETED", "2025-01-01T10:00:00+00:00", None, "2025-01-01T10:00:00+00:00"),
        ("T2", "C2", "DEBIT", 150, "COMPLETED", "2025-02-01T10:00:00+00:00", None, "2025-02-01T10:00:00+00:00"),
        ("T3", "C2", "CREDIT", 100, "COMPLETED", "2025-03-01T10:00:00+00:00", None, "2025-03-01T10:00:00+00:00"),
    ])
    row = customer_balances(df, REF).first()
    assert row["points_balance"] == 50 and row["min_running_balance"] == -50 and row["has_negative_balance"] is True


def test_credit_sla_categories(spark):
    exp = "2025-06-20T00:00:00+00:00"
    df = points(spark, [
        ("ON", "C1", "CREDIT", 1, "COMPLETED", "2025-06-17T00:00:00+00:00", exp, "2025-06-19T00:00:00+00:00"),
        ("LATE", "C1", "CREDIT", 1, "COMPLETED", "2025-06-17T00:00:00+00:00", exp, "2025-06-21T00:00:00+00:00"),
        ("PEND_OK", "C1", "CREDIT", 1, "PENDING", "2025-06-29T00:00:00+00:00", "2025-07-02T00:00:00+00:00", None),
        ("PEND_LATE", "C1", "CREDIT", 1, "PENDING", "2025-06-17T00:00:00+00:00", exp, None),
        ("AFTER_REF", "C1", "CREDIT", 1, "COMPLETED", "2025-06-17T00:00:00+00:00", exp, "2025-07-05T00:00:00+00:00"),
        ("CANC", "C1", "CREDIT", 1, "CANCELLED", "2025-06-17T00:00:00+00:00", exp, None),
        ("DEB", "C1", "DEBIT", 1, "COMPLETED", "2025-06-17T00:00:00+00:00", None, "2025-06-17T00:00:00+00:00"),
    ])
    got = {r["points_transaction_id"]: r["sla_category"] for r in classify_credit_sla(df, REF).collect()}
    assert got == {"ON": "COMPLETED_ON_TIME", "LATE": "COMPLETED_LATE", "PEND_OK": "PENDING_WITHIN_SLA",
                   "PEND_LATE": "PENDING_OVERDUE", "AFTER_REF": "PENDING_OVERDUE",  # pendente na data de referência
                   "CANC": "NOT_APPLICABLE", "DEB": None}


def test_effective_flag(spark):
    df = points(spark, [("A", "C1", "CREDIT", 1, "COMPLETED", "2025-06-01T00:00:00+00:00", None, "2025-06-01T00:00:00+00:00"),
                        ("B", "C1", "CREDIT", 1, "PENDING", "2025-06-01T00:00:00+00:00", None, None)])
    got = {r["points_transaction_id"]: r["affects_balance"] for r in effective_flag(df, REF).collect()}
    assert got == {"A": True, "B": False}


def test_purchase_measures_exclude_cancelled_and_pending(spark):
    df = spark.createDataFrame([
        ("P1", "COMPLETED", 100.0, 10.0, 90.0), ("P2", "RETURNED", 50.0, 0.0, 50.0),
        ("P3", "CANCELLED", 70.0, 0.0, 70.0), ("P4", "PENDING", 30.0, 0.0, 30.0),
    ], "purchase_id string, purchase_status string, gross_amount double, discount_amount double, net_amount double")
    df = df.select("purchase_id", "purchase_status", *[F.col(c).cast("decimal(12,2)").alias(c)
                                                       for c in ("gross_amount", "discount_amount", "net_amount")])
    agg = purchase_measures(df).agg(F.sum("gross_sales").alias("g"), F.sum("discounts").alias("d"),
                                    F.sum("net_sales").alias("n"), F.sum("returns_amount").alias("r"),
                                    F.sum("realized_revenue").alias("real"),
                                    F.sum(F.col("is_sold").cast("int")).alias("sold")).first()
    assert (float(agg["g"]), float(agg["d"]), float(agg["n"]), float(agg["r"]), float(agg["real"]), agg["sold"]) == \
        (150.0, 10.0, 140.0, 50.0, 90.0, 2)


def test_safe_rate_returns_null_on_zero_denominator(spark):
    rows = spark.createDataFrame([(1, 4), (1, 0)], "n int, d int").select(safe_rate(F.col("n"), F.col("d")).alias("r")).collect()
    assert [r["r"] for r in rows] == [0.25, None]


def _campaign_frames(spark):
    campaigns = spark.createDataFrame([("CMP001", "MULTIPLIER", "2025-03-01", "2025-03-31", "2.0")],
                                      "campaign_id string, campaign_type string, s string, e string, m string") \
        .selectExpr("campaign_id", "campaign_type", "to_date(s) start_date", "to_date(e) end_date",
                    "cast(m as decimal(4,2)) points_multiplier")
    purchases = spark.createDataFrame([
        ("P1", "C1", "CMP001", "2025-03-10T12:00:00+00:00", "COMPLETED", "90.55"),   # exposição 2 dias antes
        ("P2", "C2", "CMP001", "2025-03-10T12:00:00+00:00", "COMPLETED", "10.00"),   # exposição 8 dias antes
        ("P3", "C1", "CMP001", "2025-04-10T12:00:00+00:00", "COMPLETED", "10.00"),   # fora da vigência
        ("P4", "C1", "CMP001", "2025-03-11T12:00:00+00:00", "CANCELLED", "10.00"),
        ("P5", "C3", None, "2025-03-11T12:00:00+00:00", "COMPLETED", "10.00"),
    ], "purchase_id string, customer_id string, campaign_id string, t string, purchase_status string, net string")
    purchases = purchases.selectExpr("purchase_id", "customer_id", "campaign_id",
                                     "to_timestamp(t) purchase_timestamp", "to_date(to_timestamp(t)) purchase_date",
                                     "purchase_status", "cast(net as decimal(12,2)) net_amount")
    events = spark.createDataFrame([
        ("CMP001", "C1", "EXPOSURE", "2025-03-08T12:00:00+00:00"),
        ("CMP001", "C1", "EXPOSURE", "2025-03-09T12:00:00+00:00"),   # exposição repetida não duplica
        ("CMP001", "C1", "CLICK", "2025-03-09T13:00:00+00:00"),
        ("CMP001", "C2", "EXPOSURE", "2025-03-02T11:00:00+00:00"),
    ], "campaign_id string, customer_id string, event_type string, t string").selectExpr(
        "campaign_id", "customer_id", "event_type", "to_timestamp(t) event_timestamp")
    return campaigns, purchases, events


def test_attribution_rule(spark):
    campaigns, purchases, events = _campaign_frames(spark)
    got = {r["purchase_id"]: (r["attribution_status"], r["attributed_campaign_id"])
           for r in attribute_purchases(purchases, events, campaigns, 7).collect()}
    assert got == {"P1": ("ATTRIBUTED", "CMP001"), "P2": ("TAGGED_WITHOUT_EXPOSURE", None),
                   "P3": ("TAGGED_OUTSIDE_PERIOD", None), "P4": ("NOT_SOLD", None)}


def test_expected_credit_points_applies_multiplier_only_in_period(spark):
    campaigns, purchases, _ = _campaign_frames(spark)
    got = {r["purchase_id"]: r["expected_points"] for r in expected_credit_points(purchases, campaigns, 1.0).collect()}
    assert got["P1"] == 181      # floor(90.55 x 1 x 2)
    assert got["P3"] == 10       # fora da vigência: multiplicador 1
    assert got["P5"] == 10       # sem campanha
