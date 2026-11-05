"""Regras da Silver com cenários determinísticos (Spark local em memória)."""
from __future__ import annotations

import pytest

from helpers import (CAMPAIGNS, CHANNELS, CUSTOMERS, PARTNERS, DictContext, accepted, actions, event, points_txn,
                     purchase, reasons_of, run_stage)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def ctx(spark):
    c = DictContext()
    for name, rows in (("channels", CHANNELS), ("partners", PARTNERS), ("customers", CUSTOMERS),
                       ("campaigns", CAMPAIGNS)):
        c.tables[name] = accepted(run_stage(spark, name, rows, c), name)
    return c


def test_valid_purchase_is_inserted_with_local_business_date(spark, ctx):
    staged = run_stage(spark, "purchases", [purchase("PU00000001", purchase_timestamp="2025-03-10T22:30:00-03:00",
                                                     status_updated_at="2025-03-10T23:00:00-03:00")], ctx)
    row = staged.first()
    assert row["_action"] == "INSERT" and list(row["_reasons"]) == []
    # 22:30 em São Paulo = 01:30 UTC do dia seguinte; a data de negócio continua 10/03.
    assert str(row["purchase_date"]) == "2025-03-10"


@pytest.mark.parametrize("changes, expected", [
    ({"purchase_timestamp": "2025-02-30T10:00:00-03:00"}, "INVALID_TIMESTAMP"),
    ({"purchase_timestamp": "31/13/2025 25:61"}, "INVALID_TIMESTAMP"),
    ({"gross_amount": "-150.00", "discount_amount": "0.00", "net_amount": "-150.00"}, "NEGATIVE_AMOUNT"),
    ({"gross_amount": "80.00", "discount_amount": "95.00", "net_amount": "-15.00"}, "DISCOUNT_EXCEEDS_GROSS"),
    ({"net_amount": "50.00"}, "NET_AMOUNT_MISMATCH"),
    ({"gross_amount": "abc"}, "INVALID_AMOUNT_FORMAT"),
    ({"customer_id": "C999999"}, "UNKNOWN_CUSTOMER"),
    ({"partner_id": "P999"}, "UNKNOWN_PARTNER"),
    ({"campaign_id": "CMP999"}, "UNKNOWN_CAMPAIGN"),
    ({"purchase_status": "DONE"}, "INVALID_STATUS"),
    ({"purchase_id": "X1"}, "INVALID_ID_FORMAT"),
    ({"currency": "USD"}, "INVALID_CURRENCY"),
    ({"status_updated_at": "2025-03-01T10:00:00-03:00"}, "INCONSISTENT_STATUS_TIMESTAMP"),
])
def test_invalid_purchases_are_quarantined_with_reason(spark, ctx, changes, expected):
    staged = run_stage(spark, "purchases", [purchase("PU00000002", **changes)], ctx)
    row = staged.first()
    assert row["_action"] == "QUARANTINE"
    assert list(row["_reasons"]) == [expected]


def test_status_formatting_is_corrected_and_recorded(spark, ctx):
    row = run_stage(spark, "purchases", [purchase("PU00000003", purchase_status=" completed ")], ctx).first()
    assert row["_action"] == "INSERT" and row["purchase_status"] == "COMPLETED"
    assert list(row["_corrections"]) == ["STATUS_NORMALIZED"]


def test_duplicates_within_batch(spark, ctx):
    rows = [purchase("PU00000004"), purchase("PU00000004"),             # cópia exata
            purchase("PU00000005"), purchase("PU00000005", net_amount="90.00", discount_amount="10.00",
                                                gross_amount="100.00", channel_id="CH01",
                                                status_updated_at="2025-03-10T10:40:00-03:00",
                                                purchase_status="CANCELLED")]  # mesma chave, conteúdo diferente
    staged = run_stage(spark, "purchases", rows, ctx)
    got = sorted((r["purchase_id"], r["_action"], tuple(r["_reasons"])) for r in staged.collect())
    assert got == [
        ("PU00000004", "INSERT", ()),
        ("PU00000004", "QUARANTINE", ("DUPLICATE_RECORD",)),
        ("PU00000005", "INSERT", ()),   # a versão mais recente (CANCELLED 10:40) vence
        ("PU00000005", "QUARANTINE", ("DUPLICATE_KEY_CONFLICT",)),
    ]
    kept = staged.where("purchase_id = 'PU00000005' and _action = 'INSERT'").first()
    assert kept["purchase_status"] == "CANCELLED"


def test_incremental_actions_against_current_state(spark, ctx):
    b1 = run_stage(spark, "purchases", [
        purchase("PU00000010", purchase_status="PENDING", status_updated_at="2025-03-10T10:00:00-03:00"),
        purchase("PU00000011", purchase_status="CANCELLED", status_updated_at="2025-03-10T11:00:00-03:00"),
        purchase("PU00000012"),
        purchase("PU00000013", status_updated_at="2025-03-12T10:00:00-03:00"),
    ], ctx)
    target = accepted(b1, "purchases")
    b2 = run_stage(spark, "purchases", [
        purchase("PU00000010", purchase_status="COMPLETED", status_updated_at="2025-03-10T12:00:00-03:00"),
        purchase("PU00000011", purchase_status="COMPLETED", status_updated_at="2025-03-11T10:00:00-03:00"),
        purchase("PU00000012"),                                                  # reenvio idêntico
        purchase("PU00000013", status_updated_at="2025-03-11T10:00:00-03:00"),   # versão mais antiga
        purchase("PU00000014"),
    ], ctx, target=target, batch="B002")
    assert actions(b2) == {"PU00000010": "UPDATE", "PU00000011": "QUARANTINE", "PU00000012": "UNCHANGED",
                           "PU00000013": "STALE", "PU00000014": "INSERT"}
    assert reasons_of(b2)["PU00000011"] == ["INVALID_STATUS_TRANSITION"]


def test_reprocessing_same_batch_is_idempotent(spark, ctx):
    rows = [purchase("PU00000020"), purchase("PU00000021", purchase_status="PENDING")]
    first = run_stage(spark, "purchases", rows, ctx)
    again = run_stage(spark, "purchases", rows, ctx, target=accepted(first, "purchases"))
    assert set(actions(again).values()) == {"UNCHANGED"}


def test_points_rules(spark, ctx):
    pctx = DictContext(dict(ctx.tables))
    pctx.tables["purchases"] = accepted(run_stage(spark, "purchases", [purchase("PU00000030")], pctx), "purchases")
    pctx.tables["redemptions"] = None
    existing = accepted(run_stage(spark, "points_transactions", [
        points_txn("PT000000001", purchase_id="PU00000030")], pctx), "points_transactions")
    pctx.tables["points_transactions"] = existing
    staged = run_stage(spark, "points_transactions", [
        points_txn("PT000000002", purchase_id="PU00000030", transaction_timestamp="2025-03-10T10:32:00-03:00"),
        points_txn("PT000000003", purchase_id="PU99999999"),
        points_txn("PT000000004", purchase_id=None),
        points_txn("PT000000005", reason_code="WELCOME_BONUS", purchase_id=None),
        points_txn("PT000000006", transaction_type="ADJUSTMENT", reason_code="MANUAL_GOODWILL", points_amount="50",
                   expected_completion_timestamp=None),
        points_txn("PT000000007", transaction_type="DEBIT", reason_code="REDEMPTION", redemption_id=None,
                   expected_completion_timestamp=None),
        points_txn("PT000000008", status="COMPLETED", actual_completion_timestamp=None, purchase_id="PU00000030"),
        points_txn("PT000000009", transaction_type="CREDIT", points_amount="0", reason_code="WELCOME_BONUS"),
        points_txn("PT000000010", reason_code="WELCOME_BONUS", expected_completion_timestamp=None),
    ], pctx, target=existing, batch="B002")
    r = reasons_of(staged)
    assert r["PT000000002"] == ["DUPLICATE_CREDIT"]
    assert r["PT000000003"] == ["ORPHAN_CREDIT"]
    assert r["PT000000004"] == ["MISSING_PURCHASE_REF"]
    assert r["PT000000005"] == []                       # exceção documentada
    assert r["PT000000006"] == ["MISSING_OPERATOR"]     # ajuste manual precisa de operador
    assert r["PT000000007"] == ["MISSING_REDEMPTION_REF"]
    assert "COMPLETED_WITHOUT_COMPLETION_TS" in r["PT000000008"]
    assert r["PT000000009"] == ["INVALID_POINTS_AMOUNT"]
    derived = staged.where("points_transaction_id = 'PT000000010'").selectExpr(
        "_corrections", "date_format(expected_completion_timestamp, 'yyyy-MM-dd HH:mm:ss') AS expected_utc").first()
    assert list(derived["_corrections"]) == ["EXPECTED_COMPLETION_DERIVED"]
    assert derived["expected_utc"] == "2025-03-13 13:30:00"  # 10/03 10:30 -03:00 + 72h, em UTC


def test_malformed_json_is_quarantined(spark, ctx):
    staged = run_stage(spark, "points_transactions",
                       [{"_corrupt_record": '{"points_transaction_id": "PT-BROKEN-1", "customer_id": '}],
                       DictContext(dict(ctx.tables)))
    row = staged.first()
    assert row["_action"] == "QUARANTINE" and row["_reasons"][0] == "MALFORMED_RECORD"
    assert "PT-BROKEN-1" in row["_raw_record"]  # texto original preservado para rastreabilidade


def test_campaign_events_are_immutable(spark, ctx):
    first = run_stage(spark, "campaign_events", [event("EV000000001")], ctx)
    target = accepted(first, "campaign_events")
    again = run_stage(spark, "campaign_events", [event("EV000000001"), event("EV000000001", channel_id="CH01",
                                                                                 event_type="CLICK"),
                                                 event("EV000000002", customer_id="C999999")],
                      ctx, target=target, batch="B002")
    got = sorted((r["event_id"], r["_action"], tuple(r["_reasons"])) for r in again.collect())
    assert ("EV000000001", "UNCHANGED", ()) in got
    assert ("EV000000002", "QUARANTINE", ("UNKNOWN_CUSTOMER",)) in got
    assert any(a == "QUARANTINE" and "DUPLICATE_KEY_CONFLICT" in rs for e, a, rs in got if e == "EV000000001")
