"""Construtores de dados pequenos e determinísticos para testes com Spark em memória."""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

from loyalty_lakehouse import schemas as S
from loyalty_lakehouse.silver import RULES, Context, stage

TZ = "America/Sao_Paulo"
REF = datetime(2025, 7, 1, 3, 0, tzinfo=timezone.utc)  # 2025-07-01T00:00-03:00


class DictContext(Context):
    """Contexto da Silver baseado em dicionário (sem Lakehouse/IO)."""

    def __init__(self, tables: dict | None = None, sla_hours: float = 72):
        super().__init__(lh=None, tz=TZ, sla_hours=sla_hours)
        self.tables = tables or {}

    def silver(self, name):
        return self.tables.get(name)


def raw_df(spark, entity_name: str, rows: list[dict], batch: str = "B001"):
    """DataFrame no formato da Bronze (todas as colunas texto + colunas técnicas)."""
    entity = S.ENTITY_BY_NAME[entity_name]
    cols = list(entity.columns)
    data = [tuple(None if r.get(c) is None else str(r.get(c)) for c in cols) + (r.get("_corrupt_record"),)
            for r in rows]
    df = spark.createDataFrame(data, ", ".join(f"`{c}` string" for c in cols + ["_corrupt_record"]))
    hash_input = F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("∅")) for c in cols])
    return (df.withColumn("_extra_columns", F.lit(None).cast("string"))
              .withColumn("_source_file", F.lit(f"memory://{entity_name}/{batch}"))
              .withColumn("_batch_id", F.lit(batch))
              .withColumn("_ingestion_timestamp", F.lit(REF).cast("timestamp"))
              .withColumn("_record_hash", F.sha2(hash_input, 256)))


def run_stage(spark, entity_name: str, rows: list[dict], ctx: DictContext, target=None, batch: str = "B001"):
    """Executa a etapa da Silver em memória e devolve o DataFrame estagiado (com _action)."""
    return stage(raw_df(spark, entity_name, rows, batch), RULES[entity_name], ctx, target)


def accepted(staged, entity_name: str):
    """Linhas aceitas (INSERT/UPDATE) no formato da tabela Silver."""
    rules = RULES[entity_name]
    return staged.where(F.col("_action").isin("INSERT", "UPDATE")).select(
        *rules.columns, "_batch_id", "_source_file", "_ingestion_timestamp", "_record_hash",
        "_content_hash", "_corrections")


def actions(staged) -> dict[str, str]:
    key = staged.columns[0]
    return {r[key]: r["_action"] for r in staged.select(key, "_action").collect()}


def reasons_of(staged) -> dict[str, list[str]]:
    key = staged.columns[0]
    return {r[key]: list(r["_reasons"]) for r in staged.select(key, "_reasons").collect()}


# ---- Dimensões mínimas --------------------------------------------------------------
CUSTOMERS = [
    {"customer_id": "C000001", "registration_date": "2024-01-10", "customer_segment": "REGULAR", "region": "SUDESTE",
     "preferred_channel": "APP", "is_active": "true", "created_at": "2024-01-10T10:00:00-03:00"},
    {"customer_id": "C000002", "registration_date": "2024-02-10", "customer_segment": "VIP", "region": "SUL",
     "preferred_channel": "WEB", "is_active": "true", "created_at": "2024-02-10T10:00:00-03:00"},
]
PARTNERS = [{"partner_id": "P001", "partner_name": "Mercado Teste", "partner_category": "SUPERMERCADO",
             "region": "SUDESTE", "partner_status": "ACTIVE", "onboarding_date": "2022-01-01"}]
CHANNELS = [{"channel_id": "CH01", "channel_name": "APP", "channel_type": "DIGITAL"}]
CAMPAIGNS = [{"campaign_id": "CMP001", "campaign_name": "Pontos x2", "campaign_type": "MULTIPLIER",
              "start_date": "2025-03-01", "end_date": "2025-03-31", "campaign_status": "ENDED",
              "target_segment": "ALL", "points_multiplier": "2.0"}]
REWARDS = [{"reward_id": "R001", "reward_name": "Voucher", "reward_category": "VOUCHER", "points_cost": "500",
            "partner_id": ""}]


def purchase(pid: str, **kw) -> dict:
    base = {"purchase_id": pid, "customer_id": "C000001", "partner_id": "P001", "channel_id": "CH01",
            "purchase_timestamp": "2025-03-10T10:00:00-03:00", "gross_amount": "100.00",
            "discount_amount": "10.00", "net_amount": "90.00", "currency": "BRL", "purchase_status": "COMPLETED",
            "status_updated_at": "2025-03-10T10:30:00-03:00", "campaign_id": None, "source_system": "ECOMMERCE"}
    base.update(kw)
    return base


def points_txn(tid: str, **kw) -> dict:
    base = {"points_transaction_id": tid, "purchase_id": None, "redemption_id": None, "customer_id": "C000001",
            "transaction_type": "CREDIT", "reason_code": "PURCHASE", "points_amount": "90",
            "transaction_timestamp": "2025-03-10T10:30:00-03:00", "status": "COMPLETED",
            "status_updated_at": "2025-03-10T12:00:00-03:00",
            "expected_completion_timestamp": "2025-03-13T10:30:00-03:00",
            "actual_completion_timestamp": "2025-03-10T12:00:00-03:00", "expiration_date": "2026-03-10",
            "operator_id": None, "source_system": "POINTS_ENGINE"}
    base.update(kw)
    return base


def redemption(rid: str, **kw) -> dict:
    base = {"redemption_id": rid, "customer_id": "C000001", "reward_id": "R001", "partner_id": None,
            "redemption_timestamp": "2025-04-01T10:00:00-03:00", "points_used": "500",
            "redemption_status": "COMPLETED", "status_updated_at": "2025-04-01T11:00:00-03:00",
            "completion_timestamp": "2025-04-01T11:00:00-03:00", "source_system": "REWARDS_PLATFORM"}
    base.update(kw)
    return base


def event(eid: str, **kw) -> dict:
    base = {"event_id": eid, "campaign_id": "CMP001", "customer_id": "C000001", "event_type": "EXPOSURE",
            "event_timestamp": "2025-03-08T09:00:00-03:00", "channel_id": "CH01", "purchase_id": None,
            "source_system": "MARTECH"}
    base.update(kw)
    return base


def build_silver(spark, raw: dict[str, list[dict]]) -> dict:
    """Roda a Silver inteira em memória (dimensões primeiro), como a pipeline faria num lote."""
    ctx = DictContext()
    order = ["channels", "partners", "customers", "campaigns", "rewards", "purchases", "redemptions",
             "points_transactions", "campaign_events"]
    defaults = {"channels": CHANNELS, "partners": PARTNERS, "customers": CUSTOMERS, "campaigns": CAMPAIGNS,
                "rewards": REWARDS}
    quarantine = []
    for name in order:
        rows = raw.get(name, defaults.get(name, []))
        # localCheckpoint corta a linhagem (como a leitura de uma tabela Delta faria na pipeline);
        # sem isso o plano da Gold acumula todos os estágios e estoura a memória do driver.
        staged = run_stage(spark, name, rows, ctx).localCheckpoint()  # sem linhas => tabela vazia tipada
        ctx.tables[name] = accepted(staged, name).localCheckpoint()
        quarantine.append(staged.where(F.col("_action") == "QUARANTINE").select(
            F.lit(name).alias("entity"), F.col(RULES[name].key).alias("business_key"),
            F.col("_reasons").alias("reasons"), F.element_at("_reasons", 1).alias("primary_reason"),
            "_batch_id", F.col("_ingestion_timestamp")))
    q = quarantine[0]
    for part in quarantine[1:]:
        q = q.unionByName(part)
    ctx.tables["quarantine"] = q.localCheckpoint()
    return ctx.tables
