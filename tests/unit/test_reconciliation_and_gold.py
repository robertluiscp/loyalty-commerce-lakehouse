"""Reconciliação e indicadores Gold sobre uma Silver construída em memória com regras reais."""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from helpers import REF, build_silver, event, points_txn, purchase, redemption
from loyalty_lakehouse.config import load_settings
from loyalty_lakehouse.governance import DQ_RESULTS_SCHEMA
from loyalty_lakehouse.gold import compute_gold
from loyalty_lakehouse.reconciliation import build_checks

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def scenario(spark):
    """Cenário pequeno com resultados conhecidos (valores em R$ e pontos)."""
    raw = {
        "purchases": [
            # março: 2 vendas concluídas (100 e 50 líquidos), 1 cancelada, 1 devolvida
            purchase("PU00000001", net_amount="100.00", gross_amount="110.00", discount_amount="10.00",
                     campaign_id="CMP001"),
            purchase("PU00000002", customer_id="C000002", net_amount="50.00", gross_amount="50.00",
                     discount_amount="0.00", purchase_timestamp="2025-03-12T10:00:00-03:00",
                     status_updated_at="2025-03-12T10:30:00-03:00"),
            purchase("PU00000003", purchase_status="CANCELLED"),
            purchase("PU00000004", purchase_status="RETURNED", net_amount="40.00", gross_amount="40.00",
                     discount_amount="0.00", status_updated_at="2025-03-20T10:00:00-03:00"),
            # abril: 1 venda (200)
            purchase("PU00000005", net_amount="200.00", gross_amount="200.00", discount_amount="0.00",
                     purchase_timestamp="2025-04-05T10:00:00-03:00", status_updated_at="2025-04-05T11:00:00-03:00"),
        ],
        "points_transactions": [
            points_txn("PT000000001", purchase_id="PU00000001", points_amount="200"),        # x2 campanha: ok
            points_txn("PT000000002", purchase_id="PU00000002", customer_id="C000002", points_amount="75",
                       actual_completion_timestamp="2025-03-16T10:00:00-03:00"),             # divergente e atrasado
            points_txn("PT000000003", purchase_id="PU00000003", points_amount="90"),         # compra cancelada
            points_txn("PT000000004", purchase_id="PU00000004", points_amount="40"),         # devolvida sem estorno
            points_txn("PT000000005", purchase_id="PU00000005", points_amount="200", status="PENDING",
                       transaction_timestamp="2025-04-05T11:00:00-03:00", actual_completion_timestamp=None,
                       expected_completion_timestamp="2025-04-08T11:00:00-03:00", expiration_date=None,
                       status_updated_at="2025-04-05T11:00:00-03:00"),                       # pendente vencido
            points_txn("PT000000006", transaction_type="DEBIT", reason_code="REDEMPTION", points_amount="500",
                       redemption_id="RD0000001", expected_completion_timestamp=None,
                       transaction_timestamp="2025-04-01T11:00:00-03:00",
                       actual_completion_timestamp="2025-04-01T11:00:00-03:00"),             # saldo negativo
        ],
        "redemptions": [
            redemption("RD0000001"),
            redemption("RD0000002", redemption_status="CANCELLED", completion_timestamp=None),
            redemption("RD0000003", redemption_status="REQUESTED", completion_timestamp=None,
                       status_updated_at="2025-04-01T10:00:00-03:00"),
            redemption("RD0000004", redemption_status="COMPLETED", customer_id="C000002"),  # sem débito
        ],
        "campaign_events": [
            event("EV000000001"), event("EV000000002", event_timestamp="2025-03-09T09:00:00-03:00"),
            event("EV000000003", customer_id="C000002"),
            event("EV000000004", event_type="CONVERSION", purchase_id="PU00000001",
                  event_timestamp="2025-03-10T10:00:00-03:00"),
            event("EV000000005", event_type="CONVERSION", purchase_id="PU00000001",
                  event_timestamp="2025-03-10T10:00:05-03:00"),                              # duplicado no rastreamento
        ],
    }
    silver = build_silver(spark, raw)
    settings = load_settings("local")
    checks = build_checks(silver["purchases"], silver["points_transactions"], silver["redemptions"],
                          silver["campaigns"], silver["campaign_events"], settings.rules, REF)
    audit = None
    for df in checks.values():
        audit = df if audit is None else audit.unionByName(df)
    audit = (audit.withColumn("run_id", F.lit("test"))
             .withColumn("audited_at", F.lit(REF).cast("timestamp")).localCheckpoint())
    checks = {k: v.localCheckpoint() for k, v in checks.items()}
    inputs = dict(silver, reconciliation_audit=audit,
                  data_quality_results=spark.createDataFrame([], DQ_RESULTS_SCHEMA))
    return {"checks": checks, "gold": compute_gold(settings, inputs, "test")}


def keys(df):
    return sorted(r["business_key"] for r in df.collect())


def test_reconciliation_checks(scenario):
    c = scenario["checks"]
    assert keys(c["MISSING_CREDIT"]) == []
    assert keys(c["CREDIT_FOR_NON_COMPLETED_PURCHASE"]) == ["PT000000003"]
    assert keys(c["POINTS_AMOUNT_MISMATCH"]) == ["PT000000002"]          # esperado 50, creditado 75
    assert keys(c["RETURN_REVERSAL_MISMATCH"]) == ["PU00000004"]
    assert keys(c["REDEMPTION_DEBIT_MISMATCH"]) == ["RD0000004"]
    assert keys(c["NEGATIVE_BALANCE"]) == ["C000001"]
    assert keys(c["TRACKING_DUPLICATE_CONVERSIONS"]) == ["CMP001"]


def test_monthly_sales_exclude_cancelled_and_compute_ticket(scenario):
    rows = {str(r["month"]): r for r in scenario["gold"]["kpi_monthly_sales"].collect()}
    mar, apr = rows["2025-03-01"], rows["2025-04-01"]
    assert mar["completed_purchases"] == 3                      # 2 concluídas + 1 devolvida
    assert mar["cancelled_purchases"] == 1
    assert float(mar["net_sales"]) == 190.0                     # 100 + 50 + 40
    assert float(mar["realized_revenue"]) == 150.0              # sem a devolvida
    assert float(mar["avg_ticket"]) == pytest.approx(63.33, abs=0.01)
    assert apr["prev_month_net_sales"] == mar["net_sales"]
    assert float(apr["mom_net_sales_change_pct"]) == pytest.approx(5.26, abs=0.01)  # (200-190)/190


def test_points_summary_denominators(scenario):
    s = scenario["gold"]["kpi_points_summary"].first()
    # créditos concluídos: PT1 (no prazo), PT2 (atrasado), PT3 e PT4 (no prazo) => 3/4
    assert s["completed_on_time"] == 3 and s["completed_late"] == 1
    assert s["on_time_credit_rate"] == 0.75
    assert s["pending_overdue"] == 1                             # pendente não entra no denominador acima
    assert s["customers_negative_balance"] == 1
    # divergências de pontos: PT3, PT2, PU4, RD4 = 4; população auditada = 4 vendas + 2 resgates
    # concluídos + 1 crédito de compra não vendida = 7
    assert s["points_divergences"] == 4
    assert s["points_inconsistency_rate"] == pytest.approx(4 / 7, abs=1e-5)


def test_redemption_completion_rate_ignores_pending(scenario):
    r = scenario["gold"]["kpi_redemption_summary"].first()
    assert (r["requested"], r["completed"], r["cancelled"], r["pending"]) == (4, 2, 1, 1)
    assert r["completion_rate"] == pytest.approx(2 / 3, abs=1e-4)
    assert r["points_used_completed"] == 1000
    assert r["debit_divergences"] == 1


def test_campaign_metrics_count_unique_customers(scenario):
    c = scenario["gold"]["kpi_campaign_performance"].where("campaign_id = 'CMP001'").first()
    assert c["exposures"] == 3 and c["exposed_customers"] == 2
    assert c["converted_customers"] == 1 and c["attributed_purchases"] == 1
    assert c["conversion_rate"] == 0.5
    assert float(c["attributed_net_sales"]) == 100.0
    assert c["tracking_conversion_events"] == 2 and c["tracking_duplicate_conversions"] == 1
    assert c["roi"] is None


def test_customer_snapshot_and_activity(scenario):
    snap = {r["customer_id"]: r for r in scenario["gold"]["customer_snapshot"].collect()}
    assert snap["C000001"]["has_negative_balance"] is True
    assert snap["C000001"]["days_since_last_purchase"] == 87          # última venda 05/04, referência 01/07
    assert snap["C000001"]["inactivity_status"] == "ACTIVE_WINDOW"
    assert snap["C000002"]["inactivity_status"] == "INACTIVE_90D"     # última venda 12/03 (111 dias)
    act = {(r["customer_id"], str(r["month"])): r for r in scenario["gold"]["customer_activity_monthly"].collect()}
    assert act[("C000001", "2025-03-01")]["is_recurring_buyer"] is False  # 2 vendas no MESMO dia
    assert act[("C000001", "2025-03-01")]["sold_purchases"] == 2
