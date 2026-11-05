"""Testes do gerador (Python puro): reprodutibilidade, contabilidade das anomalias e coerência."""
from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path

from loyalty_lakehouse.data_generator import DEFAULT_ANOMALIES, generate_to_disk


def _digest(folder: Path) -> dict[str, str]:
    return {p.relative_to(folder).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(folder.rglob("*")) if p.is_file()}


def _rows(path: Path, entity: str) -> dict[str, list[dict]]:
    """Linhas por lote de uma entidade (CSV ou JSONL), ignorando linhas JSON malformadas."""
    out: dict[str, list[dict]] = {}
    for f in sorted((path / "landing" / entity).glob("batch_id=*/*")):
        batch = f.parent.name.split("=", 1)[1]
        if f.suffix == ".csv":
            with f.open(encoding="utf-8") as fh:
                out[batch] = list(csv.DictReader(fh))
        else:
            rows = []
            for line in f.read_text(encoding="utf-8").splitlines():
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            out[batch] = rows
    return out


def _ledger(path: Path) -> list[dict]:
    with (path / "anomaly_ledger.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_same_seed_generates_identical_files(small_dataset, tmp_path):
    again = tmp_path / "again"
    generate_to_disk(small_dataset["cfg"], small_dataset["rules"], again)
    assert _digest(again) == _digest(small_dataset["path"])


def test_different_seed_changes_data(small_dataset, tmp_path):
    other = tmp_path / "other"
    generate_to_disk({**small_dataset["cfg"], "seed": 7}, small_dataset["rules"], other)
    a = _digest(small_dataset["path"])
    b = _digest(other)
    assert a["landing/purchases/batch_id=B001_20250501/purchases.csv"] != b["landing/purchases/batch_id=B001_20250501/purchases.csv"]


def test_every_anomaly_is_accounted_in_ledger_and_report(small_dataset):
    ledger = _ledger(small_dataset["path"])
    counts = Counter(r["anomaly_type"] for r in ledger)
    assert counts == Counter(small_dataset["report"]["anomalies_injected"])
    configured = small_dataset["cfg"]["anomalies"]
    for anomaly in DEFAULT_ANOMALIES:
        # Saldo insuficiente depende de haver cliente/recompensa elegível; nunca excede o pedido.
        if anomaly == "insufficient_balance_redemption":
            assert 1 <= counts[anomaly] <= configured[anomaly]
        else:
            assert counts[anomaly] == configured[anomaly], anomaly
    assert all(r["batch_id"] for r in ledger), "toda anomalia deve indicar o lote"


def test_manifest_row_counts_match_files(small_dataset):
    landing = small_dataset["path"] / "landing"
    for manifest in (landing / "_manifests").glob("*.json"):
        data = json.loads(manifest.read_text(encoding="utf-8"))
        for item in data["files"]:
            lines = (landing / item["file"]).read_text(encoding="utf-8").splitlines()
            header = 1 if item["file"].endswith(".csv") else 0
            assert len(lines) - header == item["rows"], item["file"]


def test_purchase_ids_unique_except_injected_duplicates(small_dataset):
    path = small_dataset["path"]
    ledger = _ledger(path)
    injected = {r["record_id"] for r in ledger if r["anomaly_type"] == "duplicate_purchase"}
    for batch, rows in _rows(path, "purchases").items():
        counts = Counter(r["purchase_id"] for r in rows)
        dups = {k for k, v in counts.items() if v > 1}
        expected = {r["record_id"] for r in ledger if r["anomaly_type"] == "duplicate_purchase" and r["batch_id"] == batch}
        assert dups == expected
    assert injected


def test_referential_integrity_of_normal_records(small_dataset):
    path = small_dataset["path"]
    ledger = _ledger(path)
    bad_ids = {r["record_id"] for r in ledger}
    customers = {r["customer_id"] for rows in _rows(path, "customers").values() for r in rows}
    partners = {r["partner_id"] for rows in _rows(path, "partners").values() for r in rows}
    for rows in _rows(path, "purchases").values():
        for r in rows:
            if r["purchase_id"] in bad_ids:
                continue
            assert r["customer_id"] in customers and r["partner_id"] in partners


def test_facts_only_reference_customers_already_in_the_batch_snapshot(small_dataset):
    """Nenhum fato normal pode chegar antes do cadastro do cliente (inconsistência não contabilizada)."""
    path = small_dataset["path"]
    bad_ids = {r["record_id"] for r in _ledger(path)}
    snapshots = _rows(path, "customers")
    known: set[str] = set()
    for batch in sorted(snapshots):
        known |= {r["customer_id"] for r in snapshots[batch]}
        for entity, key in (("purchases", "purchase_id"), ("points_transactions", "points_transaction_id"),
                            ("redemptions", "redemption_id"), ("campaign_events", "event_id")):
            for r in _rows(path, entity).get(batch, []):
                if r[key] not in bad_ids:
                    assert r["customer_id"] in known, (entity, batch, r[key], r["customer_id"])


def _final_points(path: Path) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for batch in sorted(_rows(path, "points_transactions")):
        for r in _rows(path, "points_transactions")[batch]:
            latest[r["points_transaction_id"]] = r
    return latest


def test_balance_never_negative_except_injected_insufficient_balance(small_dataset):
    """Simula o razão de pontos final: só os clientes da anomalia ficam com saldo negativo."""
    path = small_dataset["path"]
    ledger = _ledger(path)
    excluded = {r["record_id"] for r in ledger if r["expected_detection"].startswith("silver_quarantine")}
    redemptions = {r["redemption_id"]: r for rows in _rows(path, "redemptions").values() for r in rows}
    anomaly_customers = {redemptions[r["record_id"]]["customer_id"] for r in ledger
                         if r["anomaly_type"] == "insufficient_balance_redemption"}
    sign = {"CREDIT": 1, "DEBIT": -1, "EXPIRATION": -1, "ADJUSTMENT": 1}
    events = defaultdict(list)
    for r in _final_points(path).values():
        if r["status"] == "COMPLETED" and r["points_transaction_id"] not in excluded:
            events[r["customer_id"]].append((r["actual_completion_timestamp"], r["points_transaction_id"],
                                             sign[r["transaction_type"]] * int(r["points_amount"])))
    negative = set()
    for cid, evs in events.items():
        bal = 0
        for _, _, amount in sorted(evs):
            bal += amount
            if bal < 0:
                negative.add(cid)
    assert negative == anomaly_customers


def test_one_purchase_credit_per_sold_purchase(small_dataset):
    path = small_dataset["path"]
    ledger = _ledger(path)
    dup_credits = {r["record_id"] for r in ledger if r["anomaly_type"] == "duplicate_credit"}
    per_purchase = Counter(r["purchase_id"] for r in _final_points(path).values()
                           if r["transaction_type"] == "CREDIT" and r["reason_code"] == "PURCHASE"
                           and r["purchase_id"] and r["points_transaction_id"] not in dup_credits)
    assert per_purchase and max(per_purchase.values()) == 1
