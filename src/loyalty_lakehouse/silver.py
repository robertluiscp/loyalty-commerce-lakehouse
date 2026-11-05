"""Camada Silver: tipagem, validação, deduplicação, quarentena e MERGE idempotente.

Fluxo por entidade e lote
-------------------------
1. ``parse``: converte texto em tipos (``try_cast``/``try_to_timestamp``), normaliza e
   gera ``_reasons`` (motivos de rejeição) e ``_corrections`` (correções aplicadas).
2. Deduplicação dentro do lote pela chave de negócio (window function).
3. Comparação com o estado atual da Silver -> ação por linha:
   ``INSERT`` | ``UPDATE`` | ``UNCHANGED`` (reenvio idêntico) | ``STALE`` (versão antiga)
   | ``QUARANTINE``.
4. Linhas ``QUARANTINE`` vão para ``silver.quarantine`` com motivo e referência à origem.
5. ``INSERT``/``UPDATE`` entram por ``MERGE INTO`` na tabela Silver.

Reexecutar o mesmo lote produz o mesmo estado (idempotência): reenvios idênticos são
``UNCHANGED``, a quarentena do lote é substituída via ``replaceWhere``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from pyspark.sql import Column, DataFrame, Window, functions as F

from . import schemas as S
from .bronze import bronze_ref
from .governance import GovernanceRecorder
from .utils import Lakehouse

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"
QUARANTINE_TABLE = "quarantine"
ACTIONS = ("INSERT", "UPDATE", "UNCHANGED", "STALE", "QUARANTINE")


# ---------------------------------------------------------------------------
# Funções auxiliares de expressão (todas distribuídas, sem collect)
# ---------------------------------------------------------------------------
def txt(c: str) -> Column:
    """Texto aparado; string vazia vira NULL."""
    t = F.trim(F.col(c))
    return F.when(t == "", F.lit(None).cast("string")).otherwise(t)


def code(c: str) -> Column:
    """Normalização de códigos: trim + maiúsculas (correção documentada)."""
    return F.upper(txt(c))


def ts(c: str) -> Column:
    return F.try_to_timestamp(txt(c), F.lit(TS_FORMAT))


def dt(c: str) -> Column:
    return F.to_date(F.try_to_timestamp(txt(c), F.lit("yyyy-MM-dd")))


def dec(c: str, precision: str = "12,2") -> Column:
    return F.expr(f"try_cast(nullif(trim(`{c}`), '') as decimal({precision}))")


def bigint(c: str) -> Column:
    return F.expr(f"try_cast(nullif(trim(`{c}`), '') as bigint)")


def boolean(c: str) -> Column:
    return F.expr(f"try_cast(nullif(lower(trim(`{c}`)), '') as boolean)")


def reason(code_: str, condition: Column) -> Column:
    return F.when(condition, F.lit(code_))


def reasons(*items: Column) -> Column:
    return F.filter(F.array(*items), lambda x: x.isNotNull())


def add_reasons(df: DataFrame, *items: Column) -> DataFrame:
    """Acrescenta motivos ao array ``_reasons`` já existente."""
    return df.withColumn("_reasons", F.concat(F.col("_reasons"), reasons(*items)))


def invalid_ts(raw: str, parsed: str) -> Column:
    return F.col(parsed).isNull() & txt(raw).isNotNull()


def flag_exists(df: DataFrame, col: str, lookup: DataFrame | None, key: str, flag: str) -> DataFrame:
    """Integridade referencial via LEFT JOIN com dimensão pequena (broadcast => sem shuffle)."""
    if lookup is None:
        return df.withColumn(flag, F.lit(False))
    lk = lookup.select(F.col(key).alias(f"__k_{flag}")).distinct().withColumn(flag, F.lit(True))
    out = df.join(F.broadcast(lk), df[col] == F.col(f"__k_{flag}"), "left").drop(f"__k_{flag}")
    return out.withColumn(flag, F.coalesce(F.col(flag), F.lit(False)))


def content_hash(columns: list[str]) -> Column:
    return F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                                         for c in columns]), 256)


def tz_date(col: Column, tz: str) -> Column:
    return F.to_date(F.from_utc_timestamp(col, tz))


# ---------------------------------------------------------------------------
# Contexto: dimensões/tabelas já processadas usadas nas validações cruzadas
# ---------------------------------------------------------------------------
@dataclass
class Context:
    lh: Lakehouse
    tz: str
    sla_hours: float
    _cache: dict = field(default_factory=dict)

    def silver(self, name: str) -> DataFrame | None:
        ref = self.lh.table("silver", name)
        return self.lh.read(ref) if self.lh.exists(ref) else None


@dataclass(frozen=True)
class EntityRules:
    entity: S.SourceEntity
    parse: Callable[[DataFrame, Context], DataFrame]
    columns: tuple[str, ...]                 # colunas de negócio tipadas da Silver
    version_col: str | None = None           # None => versão = _batch_id (dimensões)
    status_col: str | None = None
    transitions: dict | None = None
    insert_only: bool = False

    @property
    def key(self) -> str:
        return self.entity.business_key


def _base(df: DataFrame) -> DataFrame:
    return df.withColumn("_reasons", F.array().cast("array<string>")) \
             .withColumn("_corrections", F.array().cast("array<string>"))


def _status(df: DataFrame, raw_col: str, out_col: str, allowed: tuple[str, ...]) -> DataFrame:
    df = df.withColumn(out_col, code(raw_col))
    df = df.withColumn("_corrections", F.concat(F.col("_corrections"), reasons(
        reason("STATUS_NORMALIZED", F.col(out_col).isNotNull() & (F.col(out_col) != F.col(raw_col))))))
    return add_reasons(df, reason("INVALID_STATUS", F.col(out_col).isNull() | ~F.col(out_col).isin(*allowed)))


def _id_check(col: str, pattern: str) -> list[Column]:
    return [reason("MISSING_ID", txt(col).isNull()),
            reason("INVALID_ID_FORMAT", txt(col).isNotNull() & ~txt(col).rlike(pattern))]


# ---------------------------------------------------------------------------
# Parsers por entidade (raw => tipado + motivos)
# ---------------------------------------------------------------------------
def parse_customers(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select(
        "*", txt("customer_id").alias("s_customer_id"), dt("registration_date").alias("s_registration_date"),
        code("customer_segment").alias("s_customer_segment"), code("region").alias("s_region"),
        code("preferred_channel").alias("s_preferred_channel"), boolean("is_active").alias("s_is_active"),
        ts("created_at").alias("s_created_at"))
    return add_reasons(
        df, *_id_check("customer_id", r"^C\d{6}$"),
        reason("INVALID_DATE", F.col("s_registration_date").isNull()),
        reason("INVALID_SEGMENT", ~F.coalesce(F.col("s_customer_segment").isin(*S.CUSTOMER_SEGMENTS), F.lit(False))),
        reason("INVALID_REGION", ~F.coalesce(F.col("s_region").isin(*S.REGIONS), F.lit(False))),
        reason("INVALID_BOOLEAN", F.col("s_is_active").isNull()),
        reason("INVALID_TIMESTAMP", invalid_ts("created_at", "s_created_at")))


def parse_partners(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select(
        "*", txt("partner_id").alias("s_partner_id"), txt("partner_name").alias("s_partner_name"),
        code("partner_category").alias("s_partner_category"), code("region").alias("s_region"),
        code("partner_status").alias("s_partner_status"), dt("onboarding_date").alias("s_onboarding_date"))
    return add_reasons(
        df, *_id_check("partner_id", r"^P\d{3}$"),
        reason("MISSING_NAME", F.col("s_partner_name").isNull()),
        reason("INVALID_STATUS", ~F.coalesce(F.col("s_partner_status").isin("ACTIVE", "INACTIVE"), F.lit(False))),
        reason("INVALID_DATE", F.col("s_onboarding_date").isNull()))


def parse_campaigns(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select(
        "*", txt("campaign_id").alias("s_campaign_id"), txt("campaign_name").alias("s_campaign_name"),
        code("campaign_type").alias("s_campaign_type"), dt("start_date").alias("s_start_date"),
        dt("end_date").alias("s_end_date"), code("campaign_status").alias("s_campaign_status"),
        code("target_segment").alias("s_target_segment"),
        dec("points_multiplier", "4,2").alias("s_points_multiplier"))
    return add_reasons(
        df, *_id_check("campaign_id", r"^CMP\d{3}$"),
        reason("INVALID_TYPE", ~F.coalesce(F.col("s_campaign_type").isin("MULTIPLIER", "BONUS", "AWARENESS"), F.lit(False))),
        reason("INVALID_DATE", F.col("s_start_date").isNull() | F.col("s_end_date").isNull()),
        reason("END_BEFORE_START", F.col("s_end_date") < F.col("s_start_date")),
        reason("INVALID_STATUS", ~F.coalesce(F.col("s_campaign_status").isin("PLANNED", "ACTIVE", "ENDED"), F.lit(False))),
        reason("INVALID_MULTIPLIER", F.coalesce(F.col("s_points_multiplier") < 1, F.lit(True))))


def parse_channels(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select("*", txt("channel_id").alias("s_channel_id"), code("channel_name").alias("s_channel_name"),
                           code("channel_type").alias("s_channel_type"))
    return add_reasons(df, *_id_check("channel_id", r"^CH\d{2}$"))


def parse_rewards(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select("*", txt("reward_id").alias("s_reward_id"), txt("reward_name").alias("s_reward_name"),
                           code("reward_category").alias("s_reward_category"),
                           bigint("points_cost").alias("s_points_cost"), txt("partner_id").alias("s_partner_id"))
    return add_reasons(df, *_id_check("reward_id", r"^R\d{3}$"),
                       reason("INVALID_POINTS", F.coalesce(F.col("s_points_cost") <= 0, F.lit(True))))


def parse_purchases(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select(
        "*", txt("purchase_id").alias("s_purchase_id"), txt("customer_id").alias("s_customer_id"),
        txt("partner_id").alias("s_partner_id"), txt("channel_id").alias("s_channel_id"),
        ts("purchase_timestamp").alias("s_purchase_timestamp"),
        dec("gross_amount").alias("s_gross_amount"), dec("discount_amount").alias("s_discount_amount"),
        dec("net_amount").alias("s_net_amount"), code("currency").alias("s_currency"),
        ts("status_updated_at").alias("s_status_updated_at"), txt("campaign_id").alias("s_campaign_id"),
        code("source_system").alias("s_source_system"))
    df = df.withColumn("s_purchase_date", tz_date(F.col("s_purchase_timestamp"), ctx.tz))
    df = _status(df, "purchase_status", "s_purchase_status", S.PURCHASE_STATUSES)
    amounts_ok = (F.col("s_gross_amount").isNotNull() & F.col("s_discount_amount").isNotNull()
                  & F.col("s_net_amount").isNotNull())
    df = add_reasons(
        df, *_id_check("purchase_id", r"^PU\d{8}$"),
        reason("MISSING_TIMESTAMP", txt("purchase_timestamp").isNull()),
        reason("INVALID_TIMESTAMP", invalid_ts("purchase_timestamp", "s_purchase_timestamp")
               | invalid_ts("status_updated_at", "s_status_updated_at")),
        reason("INCONSISTENT_STATUS_TIMESTAMP", F.col("s_status_updated_at") < F.col("s_purchase_timestamp")),
        reason("INVALID_AMOUNT_FORMAT", ~amounts_ok),
        reason("NEGATIVE_AMOUNT", amounts_ok & ((F.col("s_gross_amount") < 0) | (F.col("s_discount_amount") < 0))),
        reason("ZERO_GROSS_AMOUNT", amounts_ok & (F.col("s_gross_amount") == 0)),
        reason("DISCOUNT_EXCEEDS_GROSS", amounts_ok & (F.col("s_gross_amount") >= 0)
               & (F.col("s_discount_amount") > F.col("s_gross_amount"))),
        reason("NET_AMOUNT_MISMATCH", amounts_ok
               & (F.col("s_net_amount") != F.col("s_gross_amount") - F.col("s_discount_amount"))),
        reason("INVALID_CURRENCY", F.coalesce(F.col("s_currency") != "BRL", F.lit(True))))
    df = flag_exists(df, "s_customer_id", ctx.silver("customers"), "customer_id", "_ok_customer")
    df = flag_exists(df, "s_partner_id", ctx.silver("partners"), "partner_id", "_ok_partner")
    df = flag_exists(df, "s_channel_id", ctx.silver("channels"), "channel_id", "_ok_channel")
    df = flag_exists(df, "s_campaign_id", ctx.silver("campaigns"), "campaign_id", "_ok_campaign")
    return add_reasons(
        df, reason("UNKNOWN_CUSTOMER", ~F.col("_ok_customer")),
        reason("UNKNOWN_PARTNER", ~F.col("_ok_partner")),
        reason("UNKNOWN_CHANNEL", ~F.col("_ok_channel")),
        reason("UNKNOWN_CAMPAIGN", F.col("s_campaign_id").isNotNull() & ~F.col("_ok_campaign")))


def parse_redemptions(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select(
        "*", txt("redemption_id").alias("s_redemption_id"), txt("customer_id").alias("s_customer_id"),
        txt("reward_id").alias("s_reward_id"), txt("partner_id").alias("s_partner_id"),
        ts("redemption_timestamp").alias("s_redemption_timestamp"), bigint("points_used").alias("s_points_used"),
        ts("status_updated_at").alias("s_status_updated_at"), ts("completion_timestamp").alias("s_completion_timestamp"),
        code("source_system").alias("s_source_system"))
    df = _status(df, "redemption_status", "s_redemption_status", S.REDEMPTION_STATUSES)
    df = add_reasons(
        df, *_id_check("redemption_id", r"^RD\d{7}$"),
        reason("INVALID_TIMESTAMP", F.col("s_redemption_timestamp").isNull()
               | invalid_ts("status_updated_at", "s_status_updated_at")
               | invalid_ts("completion_timestamp", "s_completion_timestamp")),
        reason("INVALID_POINTS", F.coalesce(F.col("s_points_used") <= 0, F.lit(True))),
        reason("COMPLETED_WITHOUT_COMPLETION_TS",
               (F.col("s_redemption_status") == "COMPLETED") & F.col("s_completion_timestamp").isNull()))
    df = flag_exists(df, "s_customer_id", ctx.silver("customers"), "customer_id", "_ok_customer")
    df = flag_exists(df, "s_reward_id", ctx.silver("rewards"), "reward_id", "_ok_reward")
    df = flag_exists(df, "s_partner_id", ctx.silver("partners"), "partner_id", "_ok_partner")
    return add_reasons(
        df, reason("UNKNOWN_CUSTOMER", ~F.col("_ok_customer")),
        reason("UNKNOWN_REWARD", ~F.col("_ok_reward")),
        reason("UNKNOWN_PARTNER", F.col("s_partner_id").isNotNull() & ~F.col("_ok_partner")))


def parse_points(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select(
        "*", txt("points_transaction_id").alias("s_points_transaction_id"), txt("purchase_id").alias("s_purchase_id"),
        txt("redemption_id").alias("s_redemption_id"), txt("customer_id").alias("s_customer_id"),
        code("transaction_type").alias("s_transaction_type"), code("reason_code").alias("s_reason_code"),
        bigint("points_amount").alias("s_points_amount"),
        ts("transaction_timestamp").alias("s_transaction_timestamp"),
        ts("status_updated_at").alias("s_status_updated_at"),
        ts("expected_completion_timestamp").alias("s_expected_raw"),
        ts("actual_completion_timestamp").alias("s_actual_completion_timestamp"),
        dt("expiration_date").alias("s_expiration_date"), txt("operator_id").alias("s_operator_id"),
        code("source_system").alias("s_source_system"))
    # Linha malformada primeiro: vira o motivo principal (os demais motivos são consequência).
    df = add_reasons(df, reason("MALFORMED_RECORD", F.col("_corrupt_record").isNotNull()))
    df = _status(df, "status", "s_status", S.POINTS_STATUSES)
    is_credit = F.col("s_transaction_type") == "CREDIT"
    # Prazo previsto: o informado pela origem; se ausente num crédito, deriva pela regra de SLA.
    derived = F.col("s_transaction_timestamp") + F.expr(f"INTERVAL {int(ctx.sla_hours * 3600)} SECONDS")
    df = df.withColumn("s_expected_completion_timestamp",
                       F.when(is_credit, F.coalesce(F.col("s_expected_raw"), derived)))
    df = df.withColumn("_corrections", F.concat(F.col("_corrections"), reasons(
        reason("EXPECTED_COMPLETION_DERIVED", is_credit & F.col("s_expected_raw").isNull()
               & F.col("s_transaction_timestamp").isNotNull()))))
    df = add_reasons(
        df,
        *_id_check("points_transaction_id", r"^PT\d{9}$"),
        reason("INVALID_TYPE", ~F.coalesce(F.col("s_transaction_type").isin(*S.POINTS_TYPES), F.lit(False))),
        reason("INVALID_TIMESTAMP", F.col("s_transaction_timestamp").isNull()
               | invalid_ts("status_updated_at", "s_status_updated_at")
               | invalid_ts("actual_completion_timestamp", "s_actual_completion_timestamp")),
        reason("INVALID_POINTS_AMOUNT", F.col("s_points_amount").isNull()
               | ((F.col("s_transaction_type") != "ADJUSTMENT") & (F.col("s_points_amount") <= 0))
               | ((F.col("s_transaction_type") == "ADJUSTMENT") & (F.col("s_points_amount") == 0))),
        reason("COMPLETED_WITHOUT_COMPLETION_TS",
               (F.col("s_status") == "COMPLETED") & F.col("s_actual_completion_timestamp").isNull()),
        reason("MISSING_ADJUSTMENT_REASON", (F.col("s_transaction_type") == "ADJUSTMENT") & F.col("s_reason_code").isNull()),
        reason("MISSING_OPERATOR", (F.col("s_transaction_type") == "ADJUSTMENT")
               & F.coalesce(F.col("s_reason_code").startswith("MANUAL_"), F.lit(False)) & F.col("s_operator_id").isNull()),
        reason("MISSING_PURCHASE_REF", is_credit & F.col("s_purchase_id").isNull()
               & ~F.coalesce(F.col("s_reason_code").isin(*S.CREDIT_REASONS_WITHOUT_PURCHASE), F.lit(False))),
        reason("MISSING_REDEMPTION_REF", (F.col("s_transaction_type") == "DEBIT") & F.col("s_redemption_id").isNull()))
    df = flag_exists(df, "s_customer_id", ctx.silver("customers"), "customer_id", "_ok_customer")
    df = flag_exists(df, "s_purchase_id", ctx.silver("purchases"), "purchase_id", "_ok_purchase")
    df = flag_exists(df, "s_redemption_id", ctx.silver("redemptions"), "redemption_id", "_ok_redemption")
    df = add_reasons(
        df, reason("UNKNOWN_CUSTOMER", F.col("s_customer_id").isNotNull() & ~F.col("_ok_customer")),
        reason("ORPHAN_CREDIT", is_credit & F.col("s_purchase_id").isNotNull() & ~F.col("_ok_purchase")),
        reason("ORPHAN_DEBIT", (F.col("s_transaction_type") == "DEBIT") & F.col("s_redemption_id").isNotNull()
               & ~F.col("_ok_redemption")))
    return _flag_duplicate_credits(df, ctx)


def _flag_duplicate_credits(df: DataFrame, ctx: Context) -> DataFrame:
    """Um crédito PURCHASE por compra: o mais antigo (timestamp, id) é o canônico.

    Considera os créditos já existentes na Silver + os do lote atual (window por compra:
    gera shuffle por ``purchase_id``, aceitável pois só envolve créditos de compra).
    """
    eligible = ((F.col("s_transaction_type") == "CREDIT") & (F.col("s_reason_code") == "PURCHASE")
                & F.col("s_purchase_id").isNotNull() & ~F.col("s_status").isin("REJECTED", "CANCELLED")
                & (F.size("_reasons") == 0))
    incoming = df.where(eligible).select(
        F.col("s_points_transaction_id").alias("txn"), F.col("s_purchase_id").alias("pid"),
        F.col("s_transaction_timestamp").alias("t"))
    existing = ctx.silver("points_transactions")
    if existing is not None:
        incoming = incoming.unionByName(existing.where(
            (F.col("transaction_type") == "CREDIT") & (F.col("reason_code") == "PURCHASE")
            & ~F.col("status").isin("REJECTED", "CANCELLED")).select(
            F.col("points_transaction_id").alias("txn"), F.col("purchase_id").alias("pid"),
            F.col("transaction_timestamp").alias("t")))
    w = Window.partitionBy("pid").orderBy("t", "txn")
    canonical = (incoming.dropDuplicates(["txn"]).withColumn("rn", F.row_number().over(w))
                 .where("rn = 1").select(F.col("pid").alias("__pid"), F.col("txn").alias("__canonical")))
    out = df.join(canonical, df["s_purchase_id"] == F.col("__pid"), "left")
    out = add_reasons(out, reason("DUPLICATE_CREDIT", eligible & F.col("__canonical").isNotNull()
                                  & (F.col("__canonical") != F.col("s_points_transaction_id"))))
    return out.drop("__pid", "__canonical")


def parse_campaign_events(raw: DataFrame, ctx: Context) -> DataFrame:
    df = _base(raw).select(
        "*", txt("event_id").alias("s_event_id"), txt("campaign_id").alias("s_campaign_id"),
        txt("customer_id").alias("s_customer_id"), code("event_type").alias("s_event_type"),
        ts("event_timestamp").alias("s_event_timestamp"), txt("channel_id").alias("s_channel_id"),
        txt("purchase_id").alias("s_purchase_id"), code("source_system").alias("s_source_system"))
    df = df.withColumn("s_event_date", tz_date(F.col("s_event_timestamp"), ctx.tz))
    df = add_reasons(
        df, reason("MALFORMED_RECORD", F.col("_corrupt_record").isNotNull()),
        *_id_check("event_id", r"^EV\d{9}$"),
        reason("INVALID_EVENT_TYPE", ~F.coalesce(F.col("s_event_type").isin(*S.CAMPAIGN_EVENT_TYPES), F.lit(False))),
        reason("INVALID_TIMESTAMP", F.col("s_event_timestamp").isNull()),
        reason("MISSING_PURCHASE_REF", F.col("s_event_type").isin("CONVERSION", "PURCHASE_ATTRIBUTED")
               & F.col("s_purchase_id").isNull()))
    df = flag_exists(df, "s_campaign_id", ctx.silver("campaigns"), "campaign_id", "_ok_campaign")
    df = flag_exists(df, "s_customer_id", ctx.silver("customers"), "customer_id", "_ok_customer")
    df = flag_exists(df, "s_purchase_id", ctx.silver("purchases"), "purchase_id", "_ok_purchase")
    return add_reasons(
        df, reason("UNKNOWN_CAMPAIGN", ~F.col("_ok_campaign")),
        reason("UNKNOWN_CUSTOMER", ~F.col("_ok_customer")),
        reason("UNKNOWN_PURCHASE", F.col("s_purchase_id").isNotNull() & ~F.col("_ok_purchase")))


RULES: dict[str, EntityRules] = {
    "customers": EntityRules(S.CUSTOMERS, parse_customers, (
        "customer_id", "registration_date", "customer_segment", "region", "preferred_channel",
        "is_active", "created_at")),
    "partners": EntityRules(S.PARTNERS, parse_partners, (
        "partner_id", "partner_name", "partner_category", "region", "partner_status", "onboarding_date")),
    "campaigns": EntityRules(S.CAMPAIGNS, parse_campaigns, (
        "campaign_id", "campaign_name", "campaign_type", "start_date", "end_date", "campaign_status",
        "target_segment", "points_multiplier")),
    "channels": EntityRules(S.CHANNELS, parse_channels, ("channel_id", "channel_name", "channel_type")),
    "rewards": EntityRules(S.REWARDS, parse_rewards, (
        "reward_id", "reward_name", "reward_category", "points_cost", "partner_id")),
    "purchases": EntityRules(S.PURCHASES, parse_purchases, (
        "purchase_id", "customer_id", "partner_id", "channel_id", "purchase_timestamp", "purchase_date",
        "gross_amount", "discount_amount", "net_amount", "currency", "purchase_status", "status_updated_at",
        "campaign_id", "source_system"),
        version_col="status_updated_at", status_col="purchase_status", transitions=S.PURCHASE_TRANSITIONS),
    "redemptions": EntityRules(S.REDEMPTIONS, parse_redemptions, (
        "redemption_id", "customer_id", "reward_id", "partner_id", "redemption_timestamp", "points_used",
        "redemption_status", "status_updated_at", "completion_timestamp", "source_system"),
        version_col="status_updated_at", status_col="redemption_status", transitions=S.REDEMPTION_TRANSITIONS),
    "points_transactions": EntityRules(S.POINTS_TRANSACTIONS, parse_points, (
        "points_transaction_id", "purchase_id", "redemption_id", "customer_id", "transaction_type",
        "reason_code", "points_amount", "transaction_timestamp", "status", "status_updated_at",
        "expected_completion_timestamp", "actual_completion_timestamp", "expiration_date", "operator_id",
        "source_system"),
        version_col="status_updated_at", status_col="status", transitions=S.POINTS_TRANSITIONS),
    "campaign_events": EntityRules(S.CAMPAIGN_EVENTS, parse_campaign_events, (
        "event_id", "campaign_id", "customer_id", "event_type", "event_timestamp", "event_date",
        "channel_id", "purchase_id", "source_system"), insert_only=True),
}

TECH_COLUMNS = ("_batch_id", "_source_file", "_ingestion_timestamp", "_record_hash")


def _allowed_transitions(transitions: dict) -> list[str]:
    return sorted(f"{a}>{b}" for a, targets in transitions.items() for b in targets)


# ---------------------------------------------------------------------------
# Estágio: valida, deduplica e decide a ação de cada linha
# ---------------------------------------------------------------------------
def stage(raw: DataFrame, rules: EntityRules, ctx: Context, target: DataFrame | None) -> DataFrame:
    key = rules.key
    # Payload original para a quarentena: o texto bruto da linha malformada, quando houver.
    raw = raw.withColumn("_raw_record", F.coalesce(
        F.col("_corrupt_record"), F.to_json(F.struct(*[F.col(c) for c in rules.entity.columns]))))
    parsed = rules.parse(raw, ctx)
    typed = parsed.select(
        *[F.col(f"s_{c}").alias(c) for c in rules.columns],
        *[F.col(c) for c in TECH_COLUMNS], "_raw_record", "_reasons", "_corrections")
    typed = typed.withColumn("_content_hash", content_hash(list(rules.columns)))
    version = F.col(rules.version_col) if rules.version_col else F.col("_batch_id")

    # 1) Duplicidade dentro do lote (somente entre linhas válidas).
    valid = F.size("_reasons") == 0
    w = Window.partitionBy(F.col(key), valid).orderBy(version.desc_nulls_last(), F.col("_content_hash"))
    typed = (typed.withColumn("_rn", F.row_number().over(w))
                  .withColumn("_first_hash", F.first("_content_hash").over(w)))
    typed = add_reasons(typed,
                        reason("DUPLICATE_RECORD", valid & (F.col("_rn") > 1) & (F.col("_content_hash") == F.col("_first_hash"))),
                        reason("DUPLICATE_KEY_CONFLICT", valid & (F.col("_rn") > 1) & (F.col("_content_hash") != F.col("_first_hash"))))
    typed = typed.drop("_rn", "_first_hash")

    # 2) Comparação com o estado atual (Silver).
    if target is None:
        action = F.when(F.size("_reasons") > 0, "QUARANTINE").otherwise("INSERT")
        return typed.withColumn("_action", action)
    t_cols = [F.col(key).alias("__t_key"), F.col("_content_hash").alias("__t_hash"),
              (F.col(rules.version_col) if rules.version_col else F.col("_batch_id")).alias("__t_version")]
    if rules.status_col:
        t_cols.append(F.col(rules.status_col).alias("__t_status"))
    cur = target.select(*t_cols)
    joined = typed.join(cur, typed[key] == F.col("__t_key"), "left")
    matched = F.col("__t_key").isNotNull()
    same = matched & (F.col("_content_hash") == F.col("__t_hash"))
    stale = matched & (version < F.col("__t_version"))
    if rules.insert_only:
        joined = add_reasons(joined, reason("IMMUTABLE_RECORD_CHANGED", matched & ~same & (F.size("_reasons") == 0)))
    elif rules.status_col:
        changed_status = matched & ~same & ~stale & (F.col(rules.status_col) != F.col("__t_status"))
        transition = F.concat_ws(">", F.col("__t_status"), F.col(rules.status_col))
        joined = add_reasons(joined, reason(
            "INVALID_STATUS_TRANSITION", (F.size("_reasons") == 0) & changed_status
            & ~transition.isin(*_allowed_transitions(rules.transitions))))
        joined = add_reasons(joined, reason(
            "CONFLICTING_VERSION", (F.size("_reasons") == 0) & matched & ~same & (version == F.col("__t_version"))))
    action = (F.when(F.size("_reasons") > 0, "QUARANTINE")
               .when(~matched, "INSERT")
               .when(same, "UNCHANGED")
               .when(stale, "STALE")
               .otherwise("UPDATE"))
    return joined.withColumn("_action", action).drop(*[c for c in joined.columns if c.startswith("__t_")])


def process_entity(lh: Lakehouse, gov: GovernanceRecorder, ctx: Context, entity_name: str, batch_id: str,
                   run_ts: datetime) -> dict[str, int]:
    """Processa uma entidade de um lote da Bronze para a Silver. Retorna contagens por ação."""
    rules = RULES[entity_name]
    spark = lh.spark
    with gov.step("silver_transform", batch_id=batch_id, entity=entity_name) as info:
        b_ref = bronze_ref(lh, entity_name)
        if not lh.exists(b_ref):
            info.update(status="SKIPPED", message="tabela bronze inexistente")
            return {}
        raw = lh.read(b_ref).where(F.col("_batch_id") == batch_id)
        target_ref = lh.table("silver", entity_name)
        target = lh.read(target_ref) if lh.exists(target_ref) else None
        staged = stage(raw, rules, ctx, target)

        # Materializa o estágio (evita recomputar o plano em cada ação; não há cache no serverless).
        stg_ref = lh.table("silver", f"stg_{entity_name}")
        lh.write(staged.withColumn("_run_id", F.lit(gov.run_id)), stg_ref, mode="overwrite", overwrite_schema=True)
        staged = lh.read(stg_ref)

        counts = {r["_action"]: int(r["n"]) for r in staged.groupBy("_action").agg(F.count("*").alias("n")).collect()}
        info["rows_read"] = sum(counts.values())
        if info["rows_read"] == 0:
            info.update(status="SKIPPED", message="lote sem linhas para a entidade")
            return counts

        # Quarentena (idempotente por entidade + lote).
        q = staged.where(F.col("_action") == "QUARANTINE").select(
            F.lit(entity_name).alias("entity"), F.col(rules.key).cast("string").alias("business_key"),
            F.col("_reasons").alias("reasons"), F.element_at("_reasons", 1).alias("primary_reason"),
            F.col("_raw_record").alias("raw_record"), *[F.col(c) for c in TECH_COLUMNS],
            F.col("_run_id"), F.lit(run_ts).cast("timestamp").alias("quarantined_at"))
        q_ref = lh.table("silver", QUARANTINE_TABLE)
        lh.write(q, q_ref, mode="overwrite",
                 replace_where=f"entity = '{entity_name}' AND _batch_id = '{batch_id}'")

        # MERGE das linhas aceitas.
        accepted = staged.where(F.col("_action").isin("INSERT", "UPDATE")).select(
            *rules.columns, *TECH_COLUMNS, "_content_hash", "_corrections",
            F.lit(run_ts).cast("timestamp").alias("_silver_updated_at"))
        lh.create_if_missing(accepted, target_ref)
        view = lh.temp_view(accepted, f"src_{entity_name}")
        key = rules.key
        if rules.insert_only:
            sql = (f"MERGE INTO {target_ref.sql} AS t USING {view} AS s ON t.{key} = s.{key} "
                   f"WHEN NOT MATCHED THEN INSERT *")
        else:
            sql = (f"MERGE INTO {target_ref.sql} AS t USING {view} AS s ON t.{key} = s.{key} "
                   f"WHEN MATCHED AND s._content_hash <> t._content_hash THEN UPDATE SET * "
                   f"WHEN NOT MATCHED THEN INSERT *")
        spark.sql(sql)

        info["rows_written"] = counts.get("INSERT", 0) + counts.get("UPDATE", 0)
        info["rows_quarantined"] = counts.get("QUARANTINE", 0)
        info["message"] = ", ".join(f"{k}={counts.get(k, 0)}" for k in ACTIONS)
        return counts


def reason_counts(lh: Lakehouse, batch_id: str | None = None) -> DataFrame:
    """Quantidade de linhas em quarentena por entidade e motivo (explode do array de motivos)."""
    q = lh.read(lh.table("silver", QUARANTINE_TABLE))
    if batch_id:
        q = q.where(F.col("_batch_id") == batch_id)
    return (q.select("entity", "_batch_id", F.explode("reasons").alias("reason"))
             .groupBy("entity", "_batch_id", "reason").agg(F.count("*").alias("records")))
