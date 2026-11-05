"""Gerador de dados sintéticos reproduzível (Python puro, sem Spark).

Como funciona
-------------
1. Simula a "verdade" do negócio entre ``start_date`` e ``end_date``: clientes, parceiros,
   campanhas, compras com ciclo de vida de status, créditos de pontos, resgates e eventos.
2. Cada registro é *versionado* (lista de versões com o instante em que passaram a valer).
3. Gera dois lotes de extração, como um sistema de origem faria:
   - lote 1: fotografia no corte (``batch_cutoff``) de tudo o que já existia;
   - lote 2: registros novos depois do corte + registros antigos cujo status mudou.
4. Injeta anomalias **controladas e contabilizadas** (``anomaly_ledger.csv``) para testar a
   pipeline. Nenhuma inconsistência é criada sem registro no ledger.

Todas as entidades são fictícias. Não há nomes, e-mails, telefones nem dados reais.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Iterable

from .schemas import ENTITY_BY_NAME, SourceEntity

TZ = timezone(timedelta(hours=-3))  # America/Sao_Paulo (sem horário de verão desde 2019)
CENT = Decimal("0.01")

CHANNELS = [
    ("CH01", "APP", "DIGITAL"),
    ("CH02", "WEB", "DIGITAL"),
    ("CH03", "LOJA_FISICA", "FISICO"),
    ("CH04", "SITE_PARCEIRO", "DIGITAL"),
    ("CH05", "CENTRAL_ATENDIMENTO", "ASSISTIDO"),
]
CHANNEL_WEIGHTS = [0.38, 0.27, 0.22, 0.10, 0.03]

PARTNER_CATEGORIES = {
    # categoria: (prefixo fictício, ticket médio aproximado em R$)
    "SUPERMERCADO": ("Mercado", 180),
    "FARMACIA": ("Drogaria", 90),
    "MODA": ("Ateliê", 220),
    "ELETRONICOS": ("Eletro", 900),
    "VIAGEM": ("Viagens", 1400),
    "COMBUSTIVEL": ("Posto", 200),
    "RESTAURANTE": ("Cantina", 110),
    "PET": ("Pet", 150),
}
NAME_SUFFIXES = [
    "Aurora", "Horizonte", "Ipê", "Maré", "Cerrado", "Jacarandá", "Brisa", "Serra",
    "Lume", "Vértice", "Alvorada", "Coral", "Bússola", "Ventura", "Pitanga", "Quintal",
]
SEGMENTS = ["ENTRY", "REGULAR", "PREMIUM", "VIP"]
SEGMENT_WEIGHTS = [0.40, 0.35, 0.18, 0.07]
SEGMENT_ACTIVITY = {"ENTRY": 0.6, "REGULAR": 1.0, "PREMIUM": 1.7, "VIP": 2.8}
SEGMENT_REDEMPTION = {"ENTRY": 0.6, "REGULAR": 1.0, "PREMIUM": 1.5, "VIP": 2.2}
REGIONS = ["SUDESTE", "NORDESTE", "SUL", "CENTRO_OESTE", "NORTE"]
REGION_WEIGHTS = [0.42, 0.27, 0.14, 0.08, 0.09]
REWARD_COSTS = [500, 800, 1000, 1500, 2000, 3000, 5000]
REWARD_CATEGORIES = ["VOUCHER", "PRODUTO", "CASHBACK", "EXPERIENCIA", "MILHAS"]
MONTHS_PT = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho",
             "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

DEFAULT_ANOMALIES = {
    "duplicate_purchase": 25, "unknown_customer_purchase": 15, "unknown_partner_purchase": 10,
    "invalid_purchase_date": 10, "invalid_amount": 12, "unknown_purchase_status": 5,
    "invalid_status_transition": 8, "orphan_credit": 10, "credit_missing_purchase_ref": 8,
    "duplicate_credit": 15, "credit_for_cancelled_purchase": 6, "points_amount_mismatch": 10,
    "overdue_pending_credit": 20, "insufficient_balance_redemption": 8,
    "unknown_customer_campaign_event": 20, "malformed_json_line": 2,
    "schema_drift_extra_column": 1, "duplicate_conversion_event": 15,
}
DEFAULT_SCENARIOS = {"overlap_resend_purchases": 30, "status_case_noise": 20}


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def fmt_ts(dt: datetime | None) -> str | None:
    return dt.astimezone(TZ).isoformat(timespec="seconds") if dt else None


def fmt_money(value: Decimal) -> str:
    return str(value.quantize(CENT, rounding=ROUND_HALF_UP))


def day_start(d: date) -> datetime:
    return datetime.combine(d, time(0, 0), TZ)


def day_end(d: date) -> datetime:
    return datetime.combine(d, time(23, 59, 59), TZ)


def expected_credit_points(net: Decimal, rate: float, multiplier: float) -> int:
    """Regra de pontuação: floor(valor líquido x pontos por real x multiplicador)."""
    return int(math.floor(net * Decimal(str(rate)) * Decimal(str(multiplier))))


@dataclass
class Versioned:
    """Registro com histórico: cada versão vale a partir de ``ts``."""
    key: str
    versions: list[tuple[datetime, dict]] = field(default_factory=list)

    @property
    def created(self) -> datetime:
        return self.versions[0][0]

    def at(self, t: datetime) -> dict | None:
        current = None
        for ts, row in self.versions:
            if ts <= t:
                current = row
            else:
                break
        return current


@dataclass
class Purchase:
    purchase_id: str
    customer_id: str
    partner_id: str
    channel_id: str
    ts: datetime
    gross: Decimal
    discount: Decimal
    net: Decimal
    final_status: str
    done_ts: datetime | None      # instante em que virou COMPLETED/CANCELLED
    returned_ts: datetime | None
    campaign_id: str | None = None


@dataclass
class Credit:
    txn_id: str
    purchase: Purchase | None
    customer_id: str
    reason: str
    amount: int
    ts: datetime
    actual: datetime | None       # None => nunca concluído
    cancelled_at: datetime | None = None
    spendable: bool = True        # entra na simulação de saldo disponível?
    operator_id: str | None = None
    ttype: str = "CREDIT"
    redemption_id: str | None = None


# ---------------------------------------------------------------------------
# Gerador
# ---------------------------------------------------------------------------
class LoyaltyDataGenerator:
    def __init__(self, generator_cfg: dict[str, Any], rules: dict[str, Any]):
        self.cfg = generator_cfg
        self.rules = rules
        self.rng = random.Random(int(generator_cfg.get("seed", 42)))
        self.start = date.fromisoformat(str(generator_cfg["start_date"]))
        self.end = date.fromisoformat(str(generator_cfg["end_date"]))
        self.cutoff = day_start(date.fromisoformat(str(generator_cfg["batch_cutoff"])))
        self.extract_2 = datetime.fromisoformat(rules["reference_timestamp"])
        self.period_start = day_start(self.start)
        self.period_end = day_end(self.end)
        self.sla = timedelta(hours=float(rules["credit_sla_hours"]))
        self.rate = float(rules["points_per_currency_unit"])
        self.attr_window = timedelta(days=int(rules["attribution_window_days"]))
        self.validity = timedelta(days=int(rules.get("points_validity_days", 365)))
        self.anomaly_cfg = {**DEFAULT_ANOMALIES, **(generator_cfg.get("anomalies") or {})}
        self.scenario_cfg = {**DEFAULT_SCENARIOS, **(generator_cfg.get("scenarios") or {})}
        self.batches = [
            (f"B001_{self.cutoff:%Y%m%d}", self.cutoff),
            (f"B002_{self.extract_2:%Y%m%d}", self.extract_2),
        ]
        self.ledger: list[dict] = []
        self.scenarios: Counter = Counter()
        self._seq: Counter = Counter()

    # ---- ids ---------------------------------------------------------------
    def _next(self, prefix: str, width: int) -> str:
        self._seq[prefix] += 1
        return f"{prefix}{self._seq[prefix]:0{width}d}"

    def _rand_ts(self, lo: datetime, hi: datetime) -> datetime:
        span = (hi - lo).total_seconds()
        return lo + timedelta(seconds=int(self.rng.random() * max(span, 1)))

    def _log_anomaly(self, anomaly: str, entity: str, record_id: str, expected: str,
                     description: str, batch_id: str | None = None) -> None:
        self.ledger.append({
            "anomaly_type": anomaly, "entity": entity, "record_id": record_id,
            "batch_id": batch_id or "", "expected_detection": expected,
            "description": description,
        })

    # ---- dimensões -----------------------------------------------------------
    def _gen_partners(self) -> list[dict]:
        n = int(self.cfg["partners"])
        cats = list(PARTNER_CATEGORIES)
        used: set[str] = set()
        partners = []
        for i in range(n):
            cat = cats[i % len(cats)]
            prefix = PARTNER_CATEGORIES[cat][0]
            name = f"{prefix} {self.rng.choice(NAME_SUFFIXES)}"
            while name in used:
                name = f"{prefix} {self.rng.choice(NAME_SUFFIXES)} {self.rng.randint(2, 99)}"
            used.add(name)
            onboarding = date(2022, 1, 1) + timedelta(days=self.rng.randint(0, 900))
            partners.append({
                "partner_id": f"P{i + 1:03d}", "partner_name": name, "partner_category": cat,
                "region": self.rng.choices(REGIONS, REGION_WEIGHTS)[0],
                "partner_status": "INACTIVE" if i in (n - 1, n - 2) else "ACTIVE",
                "onboarding_date": onboarding.isoformat(),
                "_weight": self.rng.lognormvariate(0, 0.8) * (0.1 if i >= n - 2 else 1),
            })
        return partners

    def _gen_customers(self) -> list[dict]:
        n = int(self.cfg["customers"])
        customers = []
        late_limit = self.end - timedelta(days=10)
        for i in range(n):
            if self.rng.random() < 0.65:
                reg = date(2023, 1, 1) + timedelta(days=self.rng.randint(0, 730))
            else:
                reg = self.start + timedelta(days=self.rng.randint(0, (late_limit - self.start).days))
            seg = self.rng.choices(SEGMENTS, SEGMENT_WEIGHTS)[0]
            created = datetime.combine(reg, time(self.rng.randint(7, 22), self.rng.randint(0, 59)), TZ)
            customers.append({
                "customer_id": f"C{i + 1:06d}", "registration_date": reg.isoformat(),
                "customer_segment": seg,
                "region": self.rng.choices(REGIONS, REGION_WEIGHTS)[0],
                "preferred_channel": self.rng.choices([c[1] for c in CHANNELS], CHANNEL_WEIGHTS)[0],
                "is_active": "false" if self.rng.random() < 0.02 else "true",
                "created_at": fmt_ts(created),
                "_reg_ts": created,
                "_weight": SEGMENT_ACTIVITY[seg] * self.rng.lognormvariate(0, 0.7),
            })
        return customers

    def _gen_rewards(self, partners: list[dict]) -> list[dict]:
        rewards = []
        for i in range(int(self.cfg["rewards"])):
            cost = REWARD_COSTS[i % len(REWARD_COSTS)]
            cat = REWARD_CATEGORIES[i % len(REWARD_CATEGORIES)]
            partner = self.rng.choice(partners)["partner_id"] if self.rng.random() < 0.6 else ""
            rewards.append({
                "reward_id": f"R{i + 1:03d}", "reward_name": f"{cat.title()} {cost} pontos",
                "reward_category": cat, "points_cost": str(cost), "partner_id": partner,
            })
        return rewards

    def _gen_campaigns(self) -> list[dict]:
        types = ["MULTIPLIER", "MULTIPLIER", "BONUS", "AWARENESS", "MULTIPLIER"]
        campaigns = []
        latest_start = self.end - timedelta(days=20)
        for i in range(int(self.cfg["campaigns"])):
            ctype = types[i % len(types)]
            start = self.start + timedelta(days=4 + self.rng.randint(0, (latest_start - self.start).days - 4))
            end = min(start + timedelta(days=self.rng.randint(14, 40)), self.end)
            multiplier = self.rng.choice([2.0, 3.0]) if ctype == "MULTIPLIER" else 1.0
            theme = {"MULTIPLIER": f"Pontos x{int(multiplier)}", "BONUS": "Bônus Extra",
                     "AWARENESS": "Semana do Cliente"}[ctype]
            target = "ALL" if self.rng.random() < 0.5 else self.rng.choice(SEGMENTS)
            campaigns.append({
                "campaign_id": f"CMP{i + 1:03d}",
                "campaign_name": f"{theme} - {MONTHS_PT[start.month - 1]} ({i + 1:02d})",
                "campaign_type": ctype, "start_date": start.isoformat(), "end_date": end.isoformat(),
                "target_segment": target, "points_multiplier": f"{multiplier:.1f}",
                "_start": day_start(start), "_end": day_end(end),
            })
        return campaigns

    def _campaign_versions(self, c: dict) -> Versioned:
        base = {k: v for k, v in c.items() if not k.startswith("_")}
        created = c["_start"] - timedelta(days=10)
        v = Versioned(c["campaign_id"])
        v.versions.append((created, {**base, "campaign_status": "PLANNED"}))
        v.versions.append((c["_start"], {**base, "campaign_status": "ACTIVE"}))
        v.versions.append((c["_end"] + timedelta(seconds=1), {**base, "campaign_status": "ENDED"}))
        return v

    # ---- compras -------------------------------------------------------------
    def _gen_purchases(self, customers: list[dict], partners: list[dict]) -> list[Purchase]:
        n = int(self.cfg["purchases"])
        cust_w = [c["_weight"] for c in customers]
        part_w = [p["_weight"] for p in partners]
        chosen_c = self.rng.choices(customers, cust_w, k=n)
        chosen_p = self.rng.choices(partners, part_w, k=n)
        chosen_ch = self.rng.choices(CHANNELS, CHANNEL_WEIGHTS, k=n)
        purchases = []
        for cust, part, ch in zip(chosen_c, chosen_p, chosen_ch):
            lo = max(self.period_start, cust["_reg_ts"] + timedelta(hours=1))
            ts = self._rand_ts(lo, self.period_end - timedelta(hours=1))
            mean = PARTNER_CATEGORIES[part["partner_category"]][1]
            gross = Decimal(str(round(self.rng.lognormvariate(math.log(mean), 0.6), 2))).quantize(CENT)
            gross = max(gross, Decimal("5.00"))
            discount = Decimal("0.00")
            if self.rng.random() < 0.30:
                discount = (gross * Decimal(str(self.rng.uniform(0.05, 0.20)))).quantize(CENT)
            net = gross - discount
            # Ciclo de vida
            returned_ts = None
            if ts > self.period_end - timedelta(days=3) and self.rng.random() < 0.5:
                final, done = "PENDING", None
            else:
                u = self.rng.random()
                if u < 0.07:
                    final, done = "CANCELLED", ts + timedelta(minutes=self.rng.randint(10, 600))
                else:
                    final, done = "COMPLETED", ts + timedelta(minutes=self.rng.randint(1, 180))
                    if u > 0.96:
                        candidate = done + timedelta(days=self.rng.randint(2, 20))
                        if candidate < self.period_end:
                            final, returned_ts = "RETURNED", candidate
                if done and done > self.period_end:
                    final, done, returned_ts = "PENDING", None, None
            purchases.append(Purchase(
                self._next("PU", 8), cust["customer_id"], part["partner_id"], ch[0], ts,
                gross, discount, net, final, done, returned_ts,
            ))
        purchases.sort(key=lambda p: (p.ts, p.purchase_id))
        return purchases

    def _purchase_versions(self, p: Purchase) -> Versioned:
        base = {
            "purchase_id": p.purchase_id, "customer_id": p.customer_id, "partner_id": p.partner_id,
            "channel_id": p.channel_id, "purchase_timestamp": fmt_ts(p.ts),
            "gross_amount": fmt_money(p.gross), "discount_amount": fmt_money(p.discount),
            "net_amount": fmt_money(p.net), "currency": "BRL",
            "campaign_id": p.campaign_id or "",
            "source_system": "POS" if p.channel_id == "CH03" else "ECOMMERCE",
        }
        v = Versioned(p.purchase_id)
        v.versions.append((p.ts, {**base, "purchase_status": "PENDING", "status_updated_at": fmt_ts(p.ts)}))
        if p.done_ts:
            status = "CANCELLED" if p.final_status == "CANCELLED" else "COMPLETED"
            v.versions.append((p.done_ts, {**base, "purchase_status": status,
                                           "status_updated_at": fmt_ts(p.done_ts)}))
        if p.returned_ts:
            v.versions.append((p.returned_ts, {**base, "purchase_status": "RETURNED",
                                               "status_updated_at": fmt_ts(p.returned_ts)}))
        return v

    # ---- campanhas: exposição, clique e atribuição --------------------------------
    def _gen_campaign_events(self, campaigns: list[dict], customers: list[dict],
                             purchases: list[Purchase]) -> list[dict]:
        by_customer: dict[str, list[Purchase]] = defaultdict(list)
        for p in purchases:
            by_customer[p.customer_id].append(p)
        times = {cid: [p.ts for p in ps] for cid, ps in by_customer.items()}
        events: list[dict] = []
        digital = [("CH01", 0.55), ("CH02", 0.30), ("CH04", 0.15)]
        for c in campaigns:
            eligible = [cu for cu in customers if cu["_reg_ts"] < c["_start"]
                        and (c["target_segment"] == "ALL" or cu["customer_segment"] == c["target_segment"])]
            if not eligible:
                continue
            exposed_ids: set[str] = set()
            for _ in range(int(self.cfg["exposures_per_campaign"])):
                cust = self.rng.choice(eligible)
                cid = cust["customer_id"]
                exposed_ids.add(cid)
                ch = self.rng.choices([d[0] for d in digital], [d[1] for d in digital])[0]
                exp_ts = self._rand_ts(c["_start"], c["_end"])
                events.append(self._event(c, cid, "EXPOSURE", exp_ts, ch))
                if self.rng.random() >= 0.22:
                    continue
                click_ts = min(exp_ts + timedelta(minutes=self.rng.randint(1, 120)), c["_end"])
                events.append(self._event(c, cid, "CLICK", click_ts, ch))
                if self.rng.random() >= 0.55:
                    continue
                # Primeira compra do cliente após o clique, dentro da janela de atribuição.
                ts_list = times.get(cid, [])
                idx = bisect_right(ts_list, click_ts)
                limit = min(click_ts + self.attr_window, c["_end"])
                while idx < len(ts_list) and ts_list[idx] <= limit:
                    p = by_customer[cid][idx]
                    if p.campaign_id is None:
                        p.campaign_id = c["campaign_id"]
                        events.append(self._event(c, cid, "CONVERSION", p.ts, ch, p.purchase_id))
                        events.append(self._event(c, cid, "PURCHASE_ATTRIBUTED",
                                                  p.ts + timedelta(minutes=1), ch, p.purchase_id))
                        break
                    idx += 1
            # Compras com código da campanha, mas sem exposição registrada (atribuição não verificável).
            for p in purchases:
                if (p.campaign_id is None and c["_start"] <= p.ts <= c["_end"]
                        and p.customer_id not in exposed_ids and self.rng.random() < 0.004):
                    p.campaign_id = c["campaign_id"]
                    self.scenarios["campaign_tag_without_exposure"] += 1
        events.sort(key=lambda e: (e["_ts"], e["event_id"]))
        return events

    def _event(self, c: dict, cid: str, etype: str, ts: datetime, ch: str,
               purchase_id: str | None = None) -> dict:
        return {
            "event_id": self._next("EV", 9), "campaign_id": c["campaign_id"], "customer_id": cid,
            "event_type": etype, "event_timestamp": fmt_ts(ts), "channel_id": ch,
            "purchase_id": purchase_id, "source_system": "MARTECH", "_ts": ts,
        }

    # ---- pontos ----------------------------------------------------------------
    def _multiplier(self, p: Purchase, campaigns: dict[str, dict]) -> float:
        c = campaigns.get(p.campaign_id or "")
        if c and c["campaign_type"] == "MULTIPLIER" and c["_start"] <= p.ts <= c["_end"]:
            return float(c["points_multiplier"])
        return 1.0

    def _gen_credits(self, purchases: list[Purchase], customers: list[dict],
                     campaigns: dict[str, dict], reserved: set[str]) -> list[Credit]:
        credits: list[Credit] = []
        for p in purchases:
            if p.purchase_id in reserved or p.done_ts is None or p.final_status == "CANCELLED":
                continue
            amount = expected_credit_points(p.net, self.rate, self._multiplier(p, campaigns))
            if amount <= 0:
                continue
            expected = p.done_ts + self.sla
            if self.rng.random() < 0.07 and p.done_ts + timedelta(hours=200) < self.extract_2:
                delay = timedelta(hours=self.rng.uniform(self.sla.total_seconds() / 3600 + 1, 200))
            else:
                delay = timedelta(hours=self.rng.uniform(0.2, 60))
            actual = p.done_ts + delay
            cr = Credit(self._next("PT", 9), p, p.customer_id, "PURCHASE", amount, p.done_ts, actual)
            if p.returned_ts and actual > p.returned_ts:
                cr.actual, cr.cancelled_at = None, p.returned_ts  # devolvida antes de concluir
            if p.final_status == "RETURNED":
                cr.spendable = False  # o estorno integral anula o crédito (ver docs/business_rules.md)
            credits.append(cr)
            # Bônus fixo de campanhas BONUS (exceção documentada: não é crédito "PURCHASE").
            c = campaigns.get(p.campaign_id or "")
            if c and c["campaign_type"] == "BONUS" and p.final_status == "COMPLETED":
                ts = p.done_ts + timedelta(days=1)
                if ts < self.period_end:
                    credits.append(Credit(self._next("PT", 9), p, p.customer_id, "CAMPAIGN_BONUS",
                                          150, ts, ts + timedelta(minutes=5)))
        # Bônus de boas-vindas para quem se cadastrou no período.
        for cu in customers:
            if cu["_reg_ts"] >= self.period_start:
                ts = cu["_reg_ts"] + timedelta(minutes=1)
                credits.append(Credit(self._next("PT", 9), None, cu["customer_id"], "WELCOME_BONUS",
                                      200, ts, ts + timedelta(seconds=30)))
        # Ajustes manuais positivos, rastreáveis por operador.
        with_credit = sorted({c.customer_id for c in credits})
        reg = {cu["customer_id"]: cu["_reg_ts"] for cu in customers}
        for cid in self.rng.sample(with_credit, max(1, len(with_credit) // 200)):
            # O ajuste só pode existir depois do cadastro do cliente.
            lo = max(self.period_start, reg[cid] + timedelta(days=1))
            ts = self._rand_ts(lo, self.period_end - timedelta(hours=1))
            credits.append(Credit(self._next("PT", 9), None, cid, "MANUAL_GOODWILL",
                                  self.rng.randint(100, 400), ts, ts + timedelta(seconds=30),
                                  operator_id=f"OP-{self.rng.randint(1, 4):02d}", ttype="ADJUSTMENT"))
        credits.sort(key=lambda c: (c.ts, c.txn_id))
        return credits

    def _simulate_redemptions(self, customers: list[dict], credits: list[Credit],
                              rewards: list[dict], neg_customers: list[str],
                              exp_customers: list[str]) -> tuple[list[dict], list[Credit]]:
        """Simula resgates e expirações cliente a cliente, respeitando o saldo disponível."""
        inflows: dict[str, list[tuple[datetime, int]]] = defaultdict(list)
        for c in credits:
            if c.spendable and c.actual and c.actual <= self.period_end:
                inflows[c.customer_id].append((c.actual, c.amount))
        reward_costs = sorted((int(r["points_cost"]), r["reward_id"], r["partner_id"]) for r in rewards)
        min_cost = reward_costs[0][0]
        neg_set, exp_set = set(neg_customers), set(exp_customers)
        redemptions: list[dict] = []
        outflows: list[Credit] = []
        months = []
        m = date(self.start.year, self.start.month, 1)
        while m <= self.end:
            nxt = date(m.year + (m.month // 12), m.month % 12 + 1, 1)
            months.append((day_start(m), min(day_start(nxt) - timedelta(seconds=1), self.period_end)))
            m = nxt
        rate = float(self.cfg["redemption_rate"])
        for cu in customers:
            cid = cu["customer_id"]
            ins = sorted(inflows.get(cid, []))
            requests = []
            for lo, hi in months:
                lo = max(lo, cu["_reg_ts"] + timedelta(days=1))
                if lo < hi and self.rng.random() < rate * SEGMENT_REDEMPTION[cu["customer_segment"]]:
                    requests.append(self._rand_ts(lo, hi))
            if cid in neg_set and not requests:
                requests.append(self._rand_ts(self.cutoff - timedelta(days=60), self.cutoff - timedelta(days=5)))
            timeline = [(r, "REQ") for r in requests]
            if cid in exp_set:
                timeline.append((datetime.combine(date(self.end.year, self.end.month, 15), time(10), TZ), "EXP"))
            timeline.sort()
            committed = 0
            forced_negative = cid not in neg_set  # True => já aplicou (ou não precisa) a anomalia
            for t, kind in timeline:
                available = sum(a for ts, a in ins if ts <= t) - committed
                if kind == "EXP":
                    if available >= 50:
                        amount = min(available, self.rng.randint(50, 300))
                        committed += amount
                        outflows.append(Credit(self._next("PT", 9), None, cid, "POINTS_EXPIRED", amount,
                                               t, t, ttype="EXPIRATION"))
                    continue
                over = [rc for rc in reward_costs if rc[0] > max(available, 0)]
                fin_forced = t + timedelta(hours=self.rng.uniform(0.1, 6))
                is_anomaly = not forced_negative and bool(over) and fin_forced <= self.period_end
                if is_anomaly:
                    # Anomalia controlada: resgate concluído sem saldo suficiente.
                    cost, rid, pid = over[0]
                    final, fin_ts = "COMPLETED", fin_forced
                    forced_negative = True
                else:
                    affordable = [rc for rc in reward_costs if rc[0] <= available]
                    if affordable and self.rng.random() < 0.92:
                        cost, rid, pid = self.rng.choice(affordable)
                    elif available < min_cost and self.rng.random() > 0.25:
                        continue
                    else:
                        cost, rid, pid = self.rng.choice(reward_costs)
                    if cost <= available:
                        u = self.rng.random()
                        if u < 0.88:
                            final, fin_ts = "COMPLETED", t + timedelta(hours=self.rng.uniform(0.1, 48))
                        elif u < 0.95:
                            final, fin_ts = "CANCELLED", t + timedelta(hours=self.rng.uniform(1, 24))
                        else:
                            final, fin_ts = "REJECTED", t + timedelta(minutes=self.rng.uniform(1, 120))
                    else:
                        final, fin_ts = "REJECTED", t + timedelta(minutes=self.rng.uniform(1, 10))
                rd_id = self._next("RD", 7)
                if is_anomaly:
                    batch = self.batches[0][0] if t < self.cutoff else self.batches[1][0]
                    self._log_anomaly("insufficient_balance_redemption", "redemptions", rd_id,
                                      "reconciliation:NEGATIVE_BALANCE",
                                      f"cliente {cid}: resgate de {cost} pts com saldo {available}", batch)
                if final == "COMPLETED":
                    committed += cost
                    if fin_ts <= self.period_end:
                        outflows.append(Credit(self._next("PT", 9), None, cid, "REDEMPTION", cost,
                                               fin_ts, fin_ts, ttype="DEBIT", redemption_id=rd_id))
                redemptions.append({
                    "redemption_id": rd_id, "customer_id": cid, "reward_id": rid,
                    "partner_id": pid, "_ts": t, "_final": final, "_fin_ts": fin_ts,
                    "points_used": cost,
                })
        redemptions.sort(key=lambda r: (r["_ts"], r["redemption_id"]))
        return redemptions, outflows

    def _redemption_versions(self, r: dict) -> Versioned:
        base = {k: v for k, v in r.items() if not k.startswith("_")}
        base.update({"redemption_timestamp": fmt_ts(r["_ts"]), "points_used": str(r["points_used"]),
                     "source_system": "REWARDS_PLATFORM"})
        v = Versioned(r["redemption_id"])
        v.versions.append((r["_ts"], {**base, "redemption_status": "REQUESTED",
                                      "status_updated_at": fmt_ts(r["_ts"]), "completion_timestamp": None}))
        if r["_fin_ts"] <= self.period_end:
            v.versions.append((r["_fin_ts"], {
                **base, "redemption_status": r["_final"], "status_updated_at": fmt_ts(r["_fin_ts"]),
                "completion_timestamp": fmt_ts(r["_fin_ts"]) if r["_final"] == "COMPLETED" else None,
            }))
        return v

    def _points_versions(self, c: Credit) -> Versioned:
        is_credit = c.ttype == "CREDIT"
        sign = -1 if c.ttype == "ADJUSTMENT" and c.reason == "PURCHASE_RETURN" else 1
        base = {
            "points_transaction_id": c.txn_id,
            "purchase_id": c.purchase.purchase_id if c.purchase else None,
            "redemption_id": c.redemption_id,
            "customer_id": c.customer_id, "transaction_type": c.ttype, "reason_code": c.reason,
            "points_amount": sign * c.amount, "transaction_timestamp": fmt_ts(c.ts),
            "expected_completion_timestamp": fmt_ts(c.ts + self.sla) if is_credit else None,
            "operator_id": c.operator_id if c.ttype == "ADJUSTMENT" else None,
            "source_system": "POINTS_ENGINE",
        }
        v = Versioned(c.txn_id)
        pending = {**base, "status": "PENDING", "status_updated_at": fmt_ts(c.ts),
                   "actual_completion_timestamp": None, "expiration_date": None}
        if c.actual and c.actual <= c.ts:
            v.versions.append((c.ts, {**pending, "status": "COMPLETED",
                                      "actual_completion_timestamp": fmt_ts(c.actual),
                                      "expiration_date": (c.actual + self.validity).date().isoformat()
                                      if is_credit else None}))
            return v
        v.versions.append((c.ts, pending))
        if c.actual:
            v.versions.append((c.actual, {**pending, "status": "COMPLETED",
                                          "status_updated_at": fmt_ts(c.actual),
                                          "actual_completion_timestamp": fmt_ts(c.actual),
                                          "expiration_date": (c.actual + self.validity).date().isoformat()
                                          if is_credit else None}))
        elif c.cancelled_at:
            v.versions.append((c.cancelled_at, {**pending, "status": "CANCELLED",
                                                "status_updated_at": fmt_ts(c.cancelled_at)}))
        return v

    # ---- lotes -------------------------------------------------------------------
    def _batch_rows(self, records: Iterable[Versioned]) -> dict[str, list[dict]]:
        """Fotografia por lote: lote 1 no corte; lote 2 = novos + alterados após o corte."""
        (b1, t1), (b2, t2) = self.batches
        out: dict[str, list[dict]] = {b1: [], b2: []}
        for rec in records:
            s1, s2 = rec.at(t1), rec.at(t2)
            if s1 is not None:
                out[b1].append(dict(s1))
            if s2 is not None and s2 != s1:
                out[b2].append(dict(s2))
        return out

    def _dim_snapshots(self, rows: list[dict], created_key, static: bool = False) -> dict[str, list[dict]]:
        (b1, t1), (b2, t2) = self.batches
        clean = lambda r: {k: v for k, v in r.items() if not k.startswith("_")}
        out = {b1: [clean(r) for r in rows if created_key(r) <= t1]}
        out[b2] = [] if static else [clean(r) for r in rows if created_key(r) <= t2]
        return out

    # ---- principal -------------------------------------------------------------------
    def generate(self) -> dict[str, Any]:
        rng = self.rng
        a = self.anomaly_cfg
        partners = self._gen_partners()
        customers = self._gen_customers()
        rewards = self._gen_rewards(partners)
        campaigns = self._gen_campaigns()
        camp_by_id = {c["campaign_id"]: c for c in campaigns}
        purchases = self._gen_purchases(customers, partners)
        events = self._gen_campaign_events(campaigns, customers, purchases)
        b1, b2 = self.batches[0][0], self.batches[1][0]

        # --- reservas de compras para anomalias/cenários (conjuntos disjuntos) ---
        def single_batch(p: Purchase) -> bool:
            last_change = p.returned_ts or p.done_ts or p.ts
            return p.ts < self.cutoff and last_change < self.cutoff

        stable = [p for p in purchases if single_batch(p) and p.final_status == "COMPLETED"]
        rng.shuffle(stable)
        dup_purchases = stable[: a["duplicate_purchase"]]
        overlap = stable[len(dup_purchases): len(dup_purchases) + self.scenario_cfg["overlap_resend_purchases"]]
        noise_start = len(dup_purchases) + len(overlap)
        noise = stable[noise_start: noise_start + self.scenario_cfg["status_case_noise"]]
        cancelled_early = [p for p in purchases if p.final_status == "CANCELLED" and p.done_ts < self.cutoff]
        rng.shuffle(cancelled_early)
        transition = cancelled_early[: a["invalid_status_transition"]]
        cancelled_credit = cancelled_early[len(transition): len(transition) + a["credit_for_cancelled_purchase"]]
        reserved = {p.purchase_id for p in transition + cancelled_credit}

        credits = self._gen_credits(purchases, customers, camp_by_id, reserved)

        # --- anomalias em créditos (antes da simulação de saldo) ---
        candidates = [c for c in credits if c.reason == "PURCHASE" and c.actual
                      and c.purchase.final_status == "COMPLETED"]
        rng.shuffle(candidates)
        overdue = [c for c in candidates if c.ts + self.sla < self.extract_2 - timedelta(days=1)][: a["overdue_pending_credit"]]
        used = {c.txn_id for c in overdue}
        for c in overdue:
            c.actual = None
            self._log_anomaly("overdue_pending_credit", "points_transactions", c.txn_id,
                              "gold:PENDING_OVERDUE", "crédito nunca concluído, prazo vencido")
        rest = [c for c in candidates if c.txn_id not in used]
        mismatch = rest[: a["points_amount_mismatch"]]
        for c in mismatch:
            c.amount = int(c.amount * 1.5) + 7
            used.add(c.txn_id)
            self._log_anomaly("points_amount_mismatch", "points_transactions", c.txn_id,
                              "reconciliation:POINTS_AMOUNT_MISMATCH", "pontos diferentes da regra")
        rest = [c for c in rest if c.txn_id not in used]
        dup_sources = rest[: a["duplicate_credit"]]
        extra_credits: list[Credit] = []
        for c in dup_sources:
            dup = Credit(self._next("PT", 9), c.purchase, c.customer_id, "PURCHASE", c.amount,
                         c.ts + timedelta(minutes=2), c.actual + timedelta(minutes=2), spendable=False)
            extra_credits.append(dup)
            self._log_anomaly("duplicate_credit", "points_transactions", dup.txn_id,
                              "silver_quarantine:DUPLICATE_CREDIT",
                              f"segundo crédito para a compra {c.purchase.purchase_id}")
        for p in cancelled_credit:
            amount = expected_credit_points(p.net, self.rate, 1.0)
            cr = Credit(self._next("PT", 9), p, p.customer_id, "PURCHASE", max(amount, 1),
                        p.done_ts + timedelta(minutes=5), p.done_ts + timedelta(hours=2), spendable=False)
            extra_credits.append(cr)
            self._log_anomaly("credit_for_cancelled_purchase", "points_transactions", cr.txn_id,
                              "reconciliation:CREDIT_FOR_NON_COMPLETED_PURCHASE",
                              f"crédito para compra cancelada {p.purchase_id}")
        valid_customers = [c["customer_id"] for c in customers if c["_reg_ts"] < self.cutoff - timedelta(days=30)]
        for i in range(a["orphan_credit"] + a["credit_missing_purchase_ref"]):
            ts = self._rand_ts(self.period_start + timedelta(days=30), self.period_end - timedelta(days=5))
            cid = rng.choice(valid_customers)
            orphan = i < a["orphan_credit"]
            fake = Purchase(f"PU9{rng.randint(1000000, 9999999)}", cid, "", "", ts, Decimal(0),
                            Decimal(0), Decimal(0), "", None, None) if orphan else None
            cr = Credit(self._next("PT", 9), fake, cid, "PURCHASE", rng.randint(50, 500), ts,
                        ts + timedelta(hours=2), spendable=False)
            extra_credits.append(cr)
            if orphan:
                self._log_anomaly("orphan_credit", "points_transactions", cr.txn_id,
                                  "silver_quarantine:ORPHAN_CREDIT", "crédito para compra inexistente")
            else:
                self._log_anomaly("credit_missing_purchase_ref", "points_transactions", cr.txn_id,
                                  "silver_quarantine:MISSING_PURCHASE_REF", "crédito PURCHASE sem purchase_id")

        # --- simulação de resgates e expirações ---
        returned_customers = {p.customer_id for p in purchases if p.final_status == "RETURNED"}
        pool = sorted({c.customer_id for c in credits if c.spendable and c.actual} - returned_customers)
        rng.shuffle(pool)
        neg_customers = pool[: a["insufficient_balance_redemption"]]
        others = pool[len(neg_customers):]
        exp_customers = others[: max(1, len(customers) // 25)]
        redemptions, outflows = self._simulate_redemptions(customers, credits, rewards,
                                                           neg_customers, exp_customers)
        # Estornos integrais de devoluções (crédito concluído antes da devolução).
        reversals = []
        for c in credits:
            p = c.purchase
            if c.reason == "PURCHASE" and p and p.returned_ts and c.actual and c.actual <= p.returned_ts:
                reversals.append(Credit(self._next("PT", 9), p, c.customer_id, "PURCHASE_RETURN",
                                        c.amount, p.returned_ts, p.returned_ts,
                                        ttype="ADJUSTMENT", operator_id="SYSTEM"))
        all_points = credits + extra_credits + outflows + reversals
        all_points.sort(key=lambda c: (c.ts, c.txn_id))

        # --- versões e lotes ---
        purchase_rows = self._batch_rows(self._purchase_versions(p) for p in purchases)
        points_rows = self._batch_rows(self._points_versions(c) for c in all_points)
        redemption_rows = self._batch_rows(self._redemption_versions(r) for r in redemptions)
        event_rows = {b1: [], b2: []}
        for e in events:
            row = {k: v for k, v in e.items() if not k.startswith("_")}
            event_rows[b1 if e["_ts"] < self.cutoff else b2].append(row)

        # --- anomalias e cenários em compras ---
        by_id_b1 = {r["purchase_id"]: r for r in purchase_rows[b1]}
        for p in dup_purchases:
            purchase_rows[b1].append(dict(by_id_b1[p.purchase_id]))
            self._log_anomaly("duplicate_purchase", "purchases", p.purchase_id,
                              "silver_quarantine:DUPLICATE_RECORD", "linha idêntica repetida no lote", b1)
        for p in overlap:
            purchase_rows[b2].append(dict(by_id_b1[p.purchase_id]))
            self.scenarios["overlap_resend_purchases"] += 1
        for p in noise:
            row = by_id_b1[p.purchase_id]
            row["purchase_status"] = f" {row['purchase_status'].lower()} "
            self.scenarios["status_case_noise"] += 1
        for p in transition:
            row = dict(by_id_b1[p.purchase_id])
            when = self._rand_ts(self.cutoff + timedelta(days=1), self.period_end - timedelta(days=2))
            row.update({"purchase_status": "COMPLETED", "status_updated_at": fmt_ts(when)})
            purchase_rows[b2].append(row)
            self._log_anomaly("invalid_status_transition", "purchases", p.purchase_id,
                              "silver_quarantine:INVALID_STATUS_TRANSITION", "CANCELLED -> COMPLETED", b2)
        template = purchases[0]

        def new_purchase_row(batch: str, **changes) -> dict:
            ts = self._rand_ts(self.period_start, self.cutoff - timedelta(days=1)) if batch == b1 \
                else self._rand_ts(self.cutoff, self.period_end - timedelta(days=1))
            p = Purchase(self._next("PU", 8), rng.choice(valid_customers), template.partner_id, "CH01", ts,
                         Decimal("100.00"), Decimal("0.00"), Decimal("100.00"), "COMPLETED", ts, None)
            row = self._purchase_versions(p).versions[0][1]
            row.update({"purchase_status": "COMPLETED", "status_updated_at": fmt_ts(ts + timedelta(minutes=30))})
            row.update(changes)
            purchase_rows[batch].append(row)
            return row

        specs = [
            ("unknown_customer_purchase", "silver_quarantine:UNKNOWN_CUSTOMER",
             lambda i: {"customer_id": f"C9{rng.randint(10000, 99999)}"}),
            ("unknown_partner_purchase", "silver_quarantine:UNKNOWN_PARTNER",
             lambda i: {"partner_id": f"P9{rng.randint(10, 99)}"}),
            ("invalid_purchase_date", "silver_quarantine:INVALID_TIMESTAMP",
             lambda i: {"purchase_timestamp": ["2025-02-30T10:00:00-03:00", "31/13/2025 25:61",
                                               "data-invalida"][i % 3]}),
            ("unknown_purchase_status", "silver_quarantine:INVALID_STATUS",
             lambda i: {"purchase_status": ["DONE", "CONCLUIDO", "UNKNOWN"][i % 3]}),
        ]
        for anomaly, expected, change in specs:
            for i in range(a[anomaly]):
                batch = b1 if i % 2 == 0 else b2
                row = new_purchase_row(batch, **change(i))
                self._log_anomaly(anomaly, "purchases", row["purchase_id"], expected, str(change(i)), batch)
        amount_variants = [
            ({"gross_amount": "-150.00", "discount_amount": "0.00", "net_amount": "-150.00"}, "NEGATIVE_AMOUNT"),
            ({"gross_amount": "80.00", "discount_amount": "95.00", "net_amount": "-15.00"}, "DISCOUNT_EXCEEDS_GROSS"),
            ({"gross_amount": "200.00", "discount_amount": "20.00", "net_amount": "150.00"}, "NET_AMOUNT_MISMATCH"),
        ]
        for i in range(a["invalid_amount"]):
            batch = b1 if i % 2 == 0 else b2
            change, reason = amount_variants[i % 3]
            row = new_purchase_row(batch, **change)
            self._log_anomaly("invalid_amount", "purchases", row["purchase_id"],
                              f"silver_quarantine:{reason}", reason, batch)

        # --- anomalias em eventos de campanha ---
        conversions = [e for e in events if e["event_type"] == "CONVERSION"]
        rng.shuffle(conversions)
        for e in conversions[: a["duplicate_conversion_event"]]:
            dup = {k: v for k, v in e.items() if not k.startswith("_")}
            dup["event_id"] = self._next("EV", 9)
            dup["event_timestamp"] = fmt_ts(e["_ts"] + timedelta(seconds=5))
            batch = b1 if e["_ts"] < self.cutoff else b2
            event_rows[batch].append(dup)
            self._log_anomaly("duplicate_conversion_event", "campaign_events", dup["event_id"],
                              "quality:DUPLICATE_TRACKING_CONVERSION", f"duplica {e['event_id']}", batch)
        for i in range(a["unknown_customer_campaign_event"]):
            c = campaigns[i % len(campaigns)]
            ts = self._rand_ts(c["_start"], c["_end"])
            ev = self._event(c, f"C9{rng.randint(10000, 99999)}", "EXPOSURE", ts, "CH01")
            batch = b1 if ts < self.cutoff else b2
            event_rows[batch].append({k: v for k, v in ev.items() if not k.startswith("_")})
            self._log_anomaly("unknown_customer_campaign_event", "campaign_events", ev["event_id"],
                              "silver_quarantine:UNKNOWN_CUSTOMER", "evento com cliente inexistente", batch)
        for e in self.ledger:
            if not e["batch_id"]:
                e["batch_id"] = self._find_batch(e, points_rows, redemption_rows)

        dims = {
            "channels": self._dim_snapshots(
                [dict(zip(("channel_id", "channel_name", "channel_type"), c)) for c in CHANNELS],
                lambda r: self.period_start, static=True),
            "partners": self._dim_snapshots(partners, lambda r: self.period_start),
            "customers": self._dim_snapshots(customers, lambda r: r["_reg_ts"]),
            "rewards": self._dim_snapshots(rewards, lambda r: self.period_start, static=True),
        }
        (_, t1), (_, t2) = self.batches
        # Campanhas: fotografia completa em cada lote (dimensão), com status do momento.
        camp_versions = [self._campaign_versions(c) for c in campaigns]
        camp_rows = {b1: [v.at(t1) for v in camp_versions if v.at(t1)],
                     b2: [v.at(t2) for v in camp_versions if v.at(t2)]}
        dims["campaigns"] = camp_rows
        return {
            "batches": self.batches,
            "tables": {**dims, "purchases": purchase_rows, "points_transactions": points_rows,
                       "redemptions": redemption_rows, "campaign_events": event_rows},
        }

    def _find_batch(self, entry: dict, points_rows: dict, redemption_rows: dict) -> str:
        rid = entry["record_id"]
        for batch, rows in points_rows.items():
            if any(r["points_transaction_id"] == rid for r in rows):
                return batch
        return ""


# ---------------------------------------------------------------------------
# Escrita dos arquivos
# ---------------------------------------------------------------------------
def _write_csv(path: Path, entity: SourceEntity, rows: list[dict], extra_cols: list[str] | None = None) -> int:
    cols = list(entity.columns) + (extra_cols or [])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in cols})
    return len(rows)


def _write_jsonl(path: Path, entity: SourceEntity, rows: list[dict], broken_lines: list[str] | None = None) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for r in rows:
            fh.write(json.dumps({k: r.get(k) for k in entity.columns}, ensure_ascii=False) + "\n")
        for line in broken_lines or []:
            fh.write(line + "\n")
    return len(rows) + len(broken_lines or [])


def write_dataset(result: dict, generator: LoyaltyDataGenerator, output: Path) -> dict:
    """Grava landing/<entidade>/batch_id=<lote>/arquivo, manifestos, ledger e relatório."""
    landing = output / "landing"
    counts: dict[str, dict[str, int]] = defaultdict(dict)
    rng = random.Random(int(generator.cfg.get("seed", 42)) + 1)
    b1, b2 = (b for b, _ in result["batches"])
    for batch_id, extracted_at in result["batches"]:
        manifest = {"batch_id": batch_id, "extracted_at": fmt_ts(extracted_at), "files": []}
        for name, by_batch in result["tables"].items():
            rows = by_batch.get(batch_id, [])
            if not rows:
                continue
            entity = ENTITY_BY_NAME[name]
            path = landing / name / f"batch_id={batch_id}" / entity.file_name
            if entity.file_format == "csv":
                extra = None
                if name == "redemptions" and batch_id == b2 and generator.anomaly_cfg["schema_drift_extra_column"]:
                    extra = ["device_type"]
                    for r in rows:
                        r["device_type"] = rng.choice(["APP", "WEB"])
                    generator._log_anomaly("schema_drift_extra_column", "redemptions", entity.file_name,
                                           "bronze_incident:UNEXPECTED_COLUMNS",
                                           "coluna extra device_type no arquivo", batch_id)
                n = _write_csv(path, entity, rows, extra)
            else:
                broken = []
                if name == "points_transactions" and batch_id == b1:
                    for i in range(generator.anomaly_cfg["malformed_json_line"]):
                        broken.append('{"points_transaction_id": "PT-BROKEN-%d", "customer_id": ' % (i + 1))
                        generator._log_anomaly("malformed_json_line", "points_transactions",
                                               f"PT-BROKEN-{i + 1}", "silver_quarantine:MALFORMED_RECORD",
                                               "linha JSON truncada", batch_id)
                n = _write_jsonl(path, entity, rows, broken)
            counts[name][batch_id] = n
            manifest["files"].append({"entity": name, "file": f"{name}/batch_id={batch_id}/{entity.file_name}",
                                      "rows": n})
        mpath = landing / "_manifests" / f"{batch_id}.json"
        mpath.parent.mkdir(parents=True, exist_ok=True)
        mpath.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

    ledger_path = output / "anomaly_ledger.csv"
    with ledger_path.open("w", newline="", encoding="utf-8") as fh:
        cols = ["anomaly_type", "entity", "record_id", "batch_id", "expected_detection", "description"]
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        writer.writerows(generator.ledger)
    report = {
        "seed": generator.cfg.get("seed", 42),
        "parameters": {k: v for k, v in generator.cfg.items() if k != "anomalies"},
        "rules": generator.rules,
        "batches": [{"batch_id": b, "extracted_at": fmt_ts(t)} for b, t in result["batches"]],
        "rows_written": counts,
        "anomalies_injected": dict(sorted(Counter(e["anomaly_type"] for e in generator.ledger).items())),
        "scenarios": dict(sorted(generator.scenarios.items())),
        "note": "Dados 100% sintéticos. As anomalias são artificiais e servem apenas para testar a pipeline.",
    }
    (output / "generation_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False),
                                                   encoding="utf-8")
    return report


def generate_to_disk(generator_cfg: dict, rules: dict, output: Path, clean: bool = True) -> dict:
    """Gera o dataset completo em ``output`` (apaga somente as saídas anteriores do gerador)."""
    output = Path(output)
    if clean:
        # Remove somente ARQUIVOS antigos (pastas ficam): pastas sincronizadas (ex.: OneDrive)
        # podem bloquear a remoção de diretórios no Windows.
        target = output / "landing"
        if target.exists():
            for f in sorted(target.rglob("*")):
                if f.is_file():
                    f.unlink()
        for f in ("anomaly_ledger.csv", "generation_report.json"):
            (output / f).unlink(missing_ok=True)
    gen = LoyaltyDataGenerator(generator_cfg, rules)
    result = gen.generate()
    return write_dataset(result, gen, output)


SMALL_PROFILE = {
    "customers": 120, "partners": 8, "campaigns": 3, "rewards": 7, "purchases": 1200,
    "exposures_per_campaign": 150,
    "anomalies": {k: 2 for k in DEFAULT_ANOMALIES} | {"schema_drift_extra_column": 1,
                                                     "malformed_json_line": 1},
    "scenarios": {"overlap_resend_purchases": 5, "status_case_noise": 3},
}


def main(argv: list[str] | None = None) -> None:
    from .config import load_settings

    parser = argparse.ArgumentParser(description="Gera os dados sintéticos do projeto.")
    parser.add_argument("--env", default="local", choices=["local", "databricks"])
    parser.add_argument("--profile", default="default", choices=["default", "small"])
    parser.add_argument("--seed", type=int)
    parser.add_argument("--purchases", type=int)
    parser.add_argument("--customers", type=int)
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    settings = load_settings(args.env)
    cfg = dict(settings.generator)
    if args.profile == "small":
        cfg.update(SMALL_PROFILE)
    for key in ("seed", "purchases", "customers"):
        if getattr(args, key) is not None:
            cfg[key] = getattr(args, key)
    output = settings.resolve(args.output or cfg["output_path"])
    report = generate_to_disk(cfg, settings.rules, output)
    print(json.dumps({"output": str(output), "rows_written": report["rows_written"],
                      "anomalies_injected": report["anomalies_injected"],
                      "scenarios": report["scenarios"]}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
